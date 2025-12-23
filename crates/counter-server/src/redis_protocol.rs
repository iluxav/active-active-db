use counter_core::{CounterStore, Delta};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, BufWriter as TokioBufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Redis-compatible protocol server
/// Supports: INCRBY, GET, MGET, PING, QUIT
pub struct RedisServer {
    store: Arc<CounterStore>,
    delta_tx: mpsc::Sender<Delta>,
}

impl RedisServer {
    pub fn new(store: Arc<CounterStore>, delta_tx: mpsc::Sender<Delta>) -> Self {
        Self { store, delta_tx }
    }

    pub async fn serve(self, addr: &str) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Redis-compatible server listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            debug!("Redis client connected: {}", peer_addr);

            let store = Arc::clone(&self.store);
            let delta_tx = self.delta_tx.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, store, delta_tx).await {
                    if !is_connection_closed(&e) {
                        error!("Connection error from {}: {}", peer_addr, e);
                    }
                }
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
) -> io::Result<()> {
    // Disable Nagle's algorithm for lower latency
    socket.set_nodelay(true)?;

    let (reader, writer) = socket.into_split();
    let mut reader = TokioBufReader::with_capacity(65536, reader);
    let mut writer = TokioBufWriter::with_capacity(65536, writer);
    let mut line = String::with_capacity(256);
    let mut response_buf = String::with_capacity(4096);

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(()); // Connection closed
        }

        response_buf.clear();

        if line.starts_with('*') {
            // RESP Array format (standard redis-cli)
            handle_resp_command(&mut reader, &line, &store, &delta_tx, &mut response_buf).await?;
        } else {
            // Inline command format (telnet-style)
            handle_inline_command(&line, &store, &delta_tx, &mut response_buf).await;
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
async fn handle_resp_command(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    first_line: &str,
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
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

    // Read all arguments - reuse a single buffer
    let mut args: Vec<String> = Vec::with_capacity(count);
    let mut line = String::with_capacity(64);

    for _ in 0..count {
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
        args.push(line.trim().to_string());
    }

    execute_command(&args, store, delta_tx, response);
    Ok(())
}

/// Handle inline command (telnet-style)
async fn handle_inline_command(
    line: &str,
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
) {
    let args: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
    if args.is_empty() {
        response.push_str("-ERR empty command\r\n");
        return;
    }
    execute_command(&args, store, delta_tx, response);
}

/// Execute a parsed command - writes response directly to buffer
fn execute_command(
    args: &[String],
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
) {
    use std::fmt::Write;

    if args.is_empty() {
        response.push_str("-ERR empty command\r\n");
        return;
    }

    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "PING" => response.push_str("+PONG\r\n"),

        "QUIT" => response.push_str("+OK\r\n"),

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
                response.push_str("-ERR INCRBY only supports positive values (G-Counter)\r\n");
                return;
            }

            let (value, delta) = store.increment_str(key, amount as u64);
            let _ = delta_tx.try_send(delta);

            let _ = write!(response, ":{}\r\n", value);
        }

        "INCR" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'incr' command\r\n");
                return;
            }
            let key = &args[1];
            let (value, delta) = store.increment_str(key, 1);
            let _ = delta_tx.try_send(delta);

            let _ = write!(response, ":{}\r\n", value);
        }

        "GET" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'get' command\r\n");
                return;
            }
            let key = &args[1];
            let value = store.get(key);
            // Fast path: format integer directly
            let _ = write!(response, ":{}\r\n", value);
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

        "SET" => {
            response.push_str("-ERR SET not supported, use INCRBY for counters\r\n");
        }

        "DEL" | "DELETE" => {
            response.push_str("-ERR DELETE not supported (G-Counter is grow-only)\r\n");
        }

        "DECR" | "DECRBY" => {
            response.push_str("-ERR DECR not supported (G-Counter is grow-only)\r\n");
        }

        "INFO" => {
            let info = format!(
                "# Server\r\nredis_version:counter-store-1.0\r\n# Keyspace\r\nkeys:{}\r\nreplica_id:{}\r\n",
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

        "COMMAND" => {
            response.push_str("*0\r\n");
        }

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
            let _ = write!(response, "*2\r\n${}\r\n{}\r\n", cursor_str.len(), next_cursor);
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
            let value = store.get(key);
            if value > 0 {
                response.push_str("+string\r\n");
            } else {
                let keys = store.keys();
                if keys.iter().any(|k| k.as_ref() == key.as_str()) {
                    response.push_str("+string\r\n");
                } else {
                    response.push_str("+none\r\n");
                }
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
            let result = store.expire(key, seconds * 1000);
            let _ = write!(response, ":{}\r\n", if result { 1 } else { 0 });
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
            let result = store.expire(key, ms);
            let _ = write!(response, ":{}\r\n", if result { 1 } else { 0 });
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
            let result = store.expire_at(key, unix_seconds * 1000);
            let _ = write!(response, ":{}\r\n", if result { 1 } else { 0 });
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
            let result = store.expire_at(key, unix_ms);
            let _ = write!(response, ":{}\r\n", if result { 1 } else { 0 });
        }

        "PERSIST" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'persist' command\r\n");
                return;
            }
            let key = &args[1];
            let result = store.persist(key);
            let _ = write!(response, ":{}\r\n", if result { 1 } else { 0 });
        }

        "EXISTS" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'exists' command\r\n");
                return;
            }
            let keys_to_check = &args[1..];
            let existing_keys = store.keys();
            let count = keys_to_check.iter()
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
            let value = store.get(key);
            let len = value.to_string().len();
            let _ = write!(response, ":{}\r\n", len);
        }

        "ECHO" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'echo' command\r\n");
                return;
            }
            let msg = &args[1];
            let _ = write!(response, "${}\r\n{}\r\n", msg.len(), msg);
        }

        "SELECT" => {
            response.push_str("+OK\r\n");
        }

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
            response.push_str("+OK\r\n");
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
