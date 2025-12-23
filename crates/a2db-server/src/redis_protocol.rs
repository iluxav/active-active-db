use crate::metrics::Metrics;
use a2db_core::{CounterStore, Delta};
use std::io;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, BufWriter as TokioBufWriter,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Redis-compatible protocol server
/// Supports: INCR, INCRBY, DECR, DECRBY, GET, SET, MGET, PING, QUIT, TTL, EXPIRE, and more
pub struct RedisServer {
    store: Arc<CounterStore>,
    delta_tx: mpsc::Sender<Delta>,
    metrics: Arc<Metrics>,
}

impl RedisServer {
    pub fn new(
        store: Arc<CounterStore>,
        delta_tx: mpsc::Sender<Delta>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            store,
            delta_tx,
            metrics,
        }
    }

    pub async fn serve(self, addr: &str) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Redis-compatible server listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            debug!("Redis client connected: {}", peer_addr);

            let store = Arc::clone(&self.store);
            let delta_tx = self.delta_tx.clone();
            let metrics = Arc::clone(&self.metrics);

            metrics.connection_opened();

            tokio::spawn(async move {
                if let Err(e) =
                    handle_connection(socket, store, delta_tx, Arc::clone(&metrics)).await
                {
                    if !is_connection_closed(&e) {
                        error!("Connection error from {}: {}", peer_addr, e);
                    }
                }
                metrics.connection_closed();
                debug!("Redis client disconnected: {}", peer_addr);
            });
        }
    }
}

fn is_connection_closed(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof
    )
}

async fn handle_connection(
    socket: TcpStream,
    store: Arc<CounterStore>,
    delta_tx: mpsc::Sender<Delta>,
    metrics: Arc<Metrics>,
) -> io::Result<()> {
    // Disable Nagle's algorithm for lower latency
    socket.set_nodelay(true)?;

    let (reader, writer) = socket.into_split();
    let mut reader = TokioBufReader::with_capacity(65536, reader);
    let mut writer = TokioBufWriter::with_capacity(65536, writer);
    let mut line = String::with_capacity(256);
    let mut response_buf = String::with_capacity(4096);
    // Reuse args buffer across commands to reduce allocations
    let mut args: Vec<String> = Vec::with_capacity(8);

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(()); // Connection closed
        }

        response_buf.clear();

        if line.starts_with('*') {
            // RESP Array format (standard redis-cli)
            handle_resp_command(
                &mut reader,
                &line,
                &store,
                &delta_tx,
                &mut response_buf,
                &metrics,
                &mut args,
            )
            .await?;
        } else {
            // Inline command format (telnet-style)
            handle_inline_command(&line, &store, &delta_tx, &mut response_buf, &metrics, &mut args);
        }

        writer.write_all(response_buf.as_bytes()).await?;

        // Only flush when no more data is immediately available (pipeline batch complete)
        // This is the key optimization for pipelining!
        if reader.buffer().is_empty() {
            writer.flush().await?;
        }
    }
}

/// Handle RESP protocol array command
/// Reuses the args buffer to reduce allocations
async fn handle_resp_command(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    first_line: &str,
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
    metrics: &Arc<Metrics>,
    args: &mut Vec<String>,
) -> io::Result<()> {
    // Parse array length: *N
    let count: usize = first_line
        .trim()
        .trim_start_matches('*')
        .parse()
        .unwrap_or(0);

    if count == 0 {
        response.push_str("-ERR invalid command\r\n");
        return Ok(());
    }

    // Reuse args buffer - ensure capacity and work with existing Strings
    let prev_len = args.len();
    let mut line = String::with_capacity(64);

    for i in 0..count {
        line.clear();
        reader.read_line(&mut line).await?;

        // Should be $N (bulk string length)
        if !line.starts_with('$') {
            response.push_str("-ERR protocol error\r\n");
            return Ok(());
        }

        // Read the actual string
        line.clear();
        reader.read_line(&mut line).await?;

        // Reuse existing String capacity if available
        if i < prev_len {
            args[i].clear();
            args[i].push_str(line.trim());
        } else {
            args.push(line.trim().to_string());
        }
    }
    // Truncate extra elements from previous larger commands
    args.truncate(count);

    let start = Instant::now();
    execute_command(args, store, delta_tx, response, metrics);
    if !args.is_empty() {
        metrics.record_command(&args[0], start);
    }
    Ok(())
}

/// Handle inline command (telnet-style)
/// Reuses the args buffer to reduce allocations
fn handle_inline_command(
    line: &str,
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
    metrics: &Arc<Metrics>,
    args: &mut Vec<String>,
) {
    // Reuse args buffer
    let prev_len = args.len();
    let mut i = 0;
    for part in line.split_whitespace() {
        if i < prev_len {
            args[i].clear();
            args[i].push_str(part);
        } else {
            args.push(part.to_string());
        }
        i += 1;
    }
    args.truncate(i);

    if args.is_empty() {
        response.push_str("-ERR empty command\r\n");
        return;
    }
    let start = Instant::now();
    execute_command(args, store, delta_tx, response, metrics);
    metrics.record_command(&args[0], start);
}

/// Execute a parsed command - writes response directly to buffer
/// Uses match statement for efficient command dispatch (compiler optimizes to jump table)
fn execute_command(
    args: &[String],
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
    metrics: &Arc<Metrics>,
) {
    use std::fmt::Write;

    if args.is_empty() {
        response.push_str("-ERR empty command\r\n");
        return;
    }

    // to_uppercase + match is faster than if-else chain with eq_ignore_ascii_case
    // because the compiler optimizes match into a jump table
    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "INCR" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'incr' command\r\n");
                return;
            }
            let key = &args[1];
            match store.increment_str(key, 1) {
                Some((value, delta)) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue INCR delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    let _ = write!(response, ":{}\r\n", value);
                }
                None => {
                    response.push_str(
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            }
        }

        "GET" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'get' command\r\n");
                return;
            }
            let key = &args[1];
            if let Some(value) = store.get_string(key) {
                let _ = write!(response, "${}\r\n{}\r\n", value.len(), value);
            } else {
                let value = store.get(key);
                if value != 0 || store.exists(key) {
                    let value_str = value.to_string();
                    let _ = write!(response, "${}\r\n{}\r\n", value_str.len(), value_str);
                } else {
                    response.push_str("$-1\r\n");
                }
            }
        }

        "INCRBY" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'incrby' command\r\n");
                return;
            }
            let key = &args[1];
            let amount: i64 = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    response.push_str("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            if amount < 0 {
                response.push_str(
                    "-ERR INCRBY only supports positive values, use DECRBY for decrement\r\n",
                );
                return;
            }
            match store.increment_str(key, amount as u64) {
                Some((value, delta)) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue INCRBY delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    let _ = write!(response, ":{}\r\n", value);
                }
                None => {
                    response.push_str(
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            }
        }

        "DECR" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'decr' command\r\n");
                return;
            }
            let key = &args[1];
            match store.decrement_str(key, 1) {
                Some((value, delta)) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue DECR delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    let _ = write!(response, ":{}\r\n", value);
                }
                None => {
                    response.push_str(
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            }
        }

        "DECRBY" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'decrby' command\r\n");
                return;
            }
            let key = &args[1];
            let amount: i64 = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    response.push_str("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            if amount < 0 {
                response.push_str(
                    "-ERR DECRBY only supports positive values, use INCRBY for increment\r\n",
                );
                return;
            }
            match store.decrement_str(key, amount as u64) {
                Some((value, delta)) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue DECRBY delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    let _ = write!(response, ":{}\r\n", value);
                }
                None => {
                    response.push_str(
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            }
        }

        "PING" => response.push_str("+PONG\r\n"),

        "QUIT" => response.push_str("+OK\r\n"),

        "SET" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'set' command\r\n");
                return;
            }
            let key = &args[1];
            let value = &args[2];
            match store.set_string_str(key, value.clone()) {
                Some(delta) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue SET delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    response.push_str("+OK\r\n");
                }
                None => {
                    response.push_str(
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            }
        }

        "MGET" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'mget' command\r\n");
                return;
            }
            let keys: Vec<String> = args[1..].to_vec();
            let values = store.mget(&keys);
            let _ = write!(response, "*{}\r\n", values.len());
            for value in values {
                let _ = write!(response, ":{}\r\n", value);
            }
        }

        "SETNX" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'setnx' command\r\n");
                return;
            }
            let key = &args[1];
            let value = &args[2];
            if !store.exists(key) {
                match store.set_string_str(key, value.clone()) {
                    Some(delta) => {
                        if delta_tx.try_send(delta).is_ok() {
                            metrics.delta_sent();
                        } else {
                            metrics.delta_send_error();
                        }
                        response.push_str(":1\r\n");
                    }
                    None => response.push_str(":0\r\n"),
                }
            } else {
                response.push_str(":0\r\n");
            }
        }

        "APPEND" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'append' command\r\n");
                return;
            }
            let key = &args[1];
            let append_value = &args[2];
            let existing = store.get_string(key).unwrap_or_default();
            let new_value = format!("{}{}", existing, append_value);
            let new_len = new_value.len();
            match store.set_string_str(key, new_value) {
                Some(delta) => {
                    if delta_tx.try_send(delta).is_ok() {
                        metrics.delta_sent();
                    } else {
                        metrics.delta_send_error();
                    }
                    let _ = write!(response, ":{}\r\n", new_len);
                }
                None => {
                    response.push_str(
                        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
            }
        }

        "DEL" | "DELETE" => {
            response.push_str("-ERR DELETE not supported (CRDT is grow-only)\r\n");
        }

        "INFO" => {
            let info = format!(
                "# Server\r\nredis_version:a2db-1.0\r\n# Keyspace\r\nkeys:{}\r\nreplica_id:{}\r\n",
                store.key_count(),
                store.local_replica_id()
            );
            let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
        }

        "DBSIZE" => {
            let _ = write!(response, ":{}\r\n", store.key_count());
        }

        "KEYS" => {
            if args.len() < 2 || args[1] != "*" {
                response.push_str("-ERR only KEYS * is supported\r\n");
                return;
            }
            let keys = store.keys();
            let _ = write!(response, "*{}\r\n", keys.len());
            for key in keys {
                let _ = write!(response, "${}\r\n{}\r\n", key.len(), key);
            }
        }

        "COMMAND" => response.push_str("*0\r\n"),

        "SCAN" => {
            let cursor: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let count: usize = 100;
            let all_keys = store.keys();
            let total = all_keys.len();
            let start = cursor;
            let end = std::cmp::min(start + count, total);
            let next_cursor = if end >= total { 0 } else { end };
            let keys_slice: Vec<_> = if start < total {
                all_keys.iter().skip(start).take(count).collect()
            } else {
                vec![]
            };
            let cursor_str = next_cursor.to_string();
            let _ = write!(
                response,
                "*2\r\n${}\r\n{}\r\n",
                cursor_str.len(),
                next_cursor
            );
            let _ = write!(response, "*{}\r\n", keys_slice.len());
            for key in keys_slice {
                let _ = write!(response, "${}\r\n{}\r\n", key.len(), key);
            }
        }

        "TYPE" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'type' command\r\n");
                return;
            }
            let key = &args[1];
            match store.get_type(key) {
                Some("counter") | Some("string") | Some(_) => response.push_str("+string\r\n"),
                None => response.push_str("+none\r\n"),
            }
        }

        "TTL" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'ttl' command\r\n");
                return;
            }
            let key = &args[1];
            let ttl = store.ttl(key);
            let _ = write!(response, ":{}\r\n", ttl);
        }

        "PTTL" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'pttl' command\r\n");
                return;
            }
            let key = &args[1];
            let pttl = store.pttl(key);
            let _ = write!(response, ":{}\r\n", pttl);
        }

        "EXPIRE" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'expire' command\r\n");
                return;
            }
            let key = &args[1];
            let seconds: u64 = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    response.push_str("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            match store.expire(key, seconds * 1000) {
                Some(delta) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue EXPIRE delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    response.push_str(":1\r\n");
                }
                None => response.push_str(":0\r\n"),
            }
        }

        "PEXPIRE" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'pexpire' command\r\n");
                return;
            }
            let key = &args[1];
            let ms: u64 = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    response.push_str("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            match store.expire(key, ms) {
                Some(delta) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue PEXPIRE delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    response.push_str(":1\r\n");
                }
                None => response.push_str(":0\r\n"),
            }
        }

        "EXPIREAT" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'expireat' command\r\n");
                return;
            }
            let key = &args[1];
            let unix_seconds: u64 = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    response.push_str("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            match store.expire_at(key, unix_seconds * 1000) {
                Some(delta) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue EXPIREAT delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    response.push_str(":1\r\n");
                }
                None => response.push_str(":0\r\n"),
            }
        }

        "PEXPIREAT" => {
            if args.len() < 3 {
                response.push_str("-ERR wrong number of arguments for 'pexpireat' command\r\n");
                return;
            }
            let key = &args[1];
            let unix_ms: u64 = match args[2].parse() {
                Ok(n) => n,
                Err(_) => {
                    response.push_str("-ERR value is not an integer or out of range\r\n");
                    return;
                }
            };
            match store.expire_at(key, unix_ms) {
                Some(delta) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue PEXPIREAT delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    response.push_str(":1\r\n");
                }
                None => response.push_str(":0\r\n"),
            }
        }

        "PERSIST" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'persist' command\r\n");
                return;
            }
            let key = &args[1];
            match store.persist(key) {
                Some(delta) => {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue PERSIST delta: {}", e);
                        metrics.delta_send_error();
                    } else {
                        metrics.delta_sent();
                    }
                    response.push_str(":1\r\n");
                }
                None => response.push_str(":0\r\n"),
            }
        }

        "EXISTS" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'exists' command\r\n");
                return;
            }
            let keys_to_check = &args[1..];
            let existing_keys = store.keys();
            let count = keys_to_check
                .iter()
                .filter(|k| existing_keys.iter().any(|ek| ek.as_ref() == k.as_str()))
                .count();
            let _ = write!(response, ":{}\r\n", count);
        }

        "STRLEN" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'strlen' command\r\n");
                return;
            }
            let key = &args[1];
            if let Some(value) = store.get_string(key) {
                let _ = write!(response, ":{}\r\n", value.len());
            } else {
                let value = store.get(key);
                if value != 0 || store.exists(key) {
                    let len = value.to_string().len();
                    let _ = write!(response, ":{}\r\n", len);
                } else {
                    response.push_str(":0\r\n");
                }
            }
        }

        "ECHO" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'echo' command\r\n");
                return;
            }
            let msg = &args[1];
            let _ = write!(response, "${}\r\n{}\r\n", msg.len(), msg);
        }

        "SELECT" => response.push_str("+OK\r\n"),

        "FLUSHDB" | "FLUSHALL" => {
            response.push_str("-ERR FLUSH not supported (G-Counter is grow-only)\r\n");
        }

        "CLIENT" => {
            if args.len() > 1 {
                match args[1].to_uppercase().as_str() {
                    "SETNAME" => response.push_str("+OK\r\n"),
                    "GETNAME" => response.push_str("$-1\r\n"),
                    "LIST" => response.push_str("$0\r\n\r\n"),
                    "ID" => response.push_str(":1\r\n"),
                    _ => response.push_str("+OK\r\n"),
                }
            } else {
                response.push_str("+OK\r\n");
            }
        }

        "CONFIG" => {
            if args.len() > 1 && args[1].to_uppercase() == "GET" {
                response.push_str("*0\r\n");
            } else {
                response.push_str("+OK\r\n");
            }
        }

        "DEBUG" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'debug' command\r\n");
                return;
            }
            match args[1].to_uppercase().as_str() {
                "CRDT" => {
                    if args.len() < 3 {
                        response.push_str(
                            "-ERR usage: DEBUG CRDT <key> - show CRDT components for a key\r\n",
                        );
                        return;
                    }
                    let key = &args[2];
                    match store.debug_counter_state(key) {
                        Some((p_components, n_components, value, expires_at_ms)) => {
                            let mut info = format!("Key: {}\n", key);
                            info.push_str(&format!("Value: {}\n", value));
                            info.push_str(&format!(
                                "Expires: {}\n",
                                expires_at_ms
                                    .map(|e| e.to_string())
                                    .unwrap_or_else(|| "never".to_string())
                            ));
                            info.push_str("P-components (increments):\n");
                            for (replica, val) in &p_components {
                                info.push_str(&format!("  {}: {}\n", replica, val));
                            }
                            info.push_str("N-components (decrements):\n");
                            for (replica, val) in &n_components {
                                info.push_str(&format!("  {}: {}\n", replica, val));
                            }
                            let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
                        }
                        None => {
                            if let Some(val) = store.get_string(key) {
                                let info = format!("Key: {}\nType: string\nValue: {}\n", key, val);
                                let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
                            } else {
                                response.push_str("$-1\r\n");
                            }
                        }
                    }
                }
                "REPLICA" => {
                    let info = format!("Local replica ID: {}\n", store.local_replica_id());
                    let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
                }
                _ => response.push_str("+OK\r\n"),
            }
        }

        "MEMORY" => {
            if args.len() > 2 && args[1].to_uppercase() == "USAGE" {
                let key = &args[2];
                let keys = store.keys();
                if keys.iter().any(|k| k.as_ref() == key.as_str()) {
                    response.push_str(":64\r\n");
                } else {
                    response.push_str("$-1\r\n");
                }
            } else {
                response.push_str(":0\r\n");
            }
        }

        "OBJECT" => {
            if args.len() > 2 && args[1].to_uppercase() == "ENCODING" {
                response.push_str("+int\r\n");
            } else {
                response.push_str("$-1\r\n");
            }
        }

        _ => {
            let _ = write!(response, "-ERR unknown command '{}'\r\n", cmd);
        }
    }
}
