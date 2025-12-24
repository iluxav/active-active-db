use crate::gossip::SharedPeerRegistry;
use crate::metrics::Metrics;
use a2db_core::{CounterStore, Delta};
use std::io;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{
    AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader,
    BufWriter as TokioBufWriter,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Fast glob pattern matcher for Redis-style patterns
/// Supports: * (any sequence), ? (single char), [abc] (char class), [a-z] (range)
/// Compiled once and reused for matching multiple keys
#[derive(Clone)]
enum GlobToken {
    Literal(char),
    Any,                                // *
    Single,                             // ?
    CharClass(Vec<(char, char)>, bool), // [abc] or [^abc], ranges stored as (start, end)
}

struct GlobPattern {
    tokens: Vec<GlobToken>,
}

impl GlobPattern {
    /// Compile a glob pattern into tokens for fast matching
    fn compile(pattern: &str) -> Self {
        let mut tokens = Vec::with_capacity(pattern.len());
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '*' => tokens.push(GlobToken::Any),
                '?' => tokens.push(GlobToken::Single),
                '[' => {
                    let mut ranges = Vec::new();
                    let negated = chars.peek() == Some(&'^');
                    if negated {
                        chars.next();
                    }

                    while let Some(&ch) = chars.peek() {
                        if ch == ']' {
                            chars.next();
                            break;
                        }
                        let start = chars.next().unwrap();
                        if chars.peek() == Some(&'-') {
                            chars.next(); // consume '-'
                            if let Some(end) = chars.next() {
                                if end != ']' {
                                    ranges.push((start, end));
                                    continue;
                                }
                            }
                        }
                        ranges.push((start, start));
                    }
                    tokens.push(GlobToken::CharClass(ranges, negated));
                }
                '\\' => {
                    // Escape next character
                    if let Some(escaped) = chars.next() {
                        tokens.push(GlobToken::Literal(escaped));
                    }
                }
                _ => tokens.push(GlobToken::Literal(c)),
            }
        }

        Self { tokens }
    }

    /// Check if a string matches this pattern
    #[inline]
    fn matches(&self, s: &str) -> bool {
        self.match_recursive(&self.tokens, s.as_bytes())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn match_recursive(&self, tokens: &[GlobToken], s: &[u8]) -> bool {
        if tokens.is_empty() {
            return s.is_empty();
        }

        match &tokens[0] {
            GlobToken::Literal(c) => {
                if s.is_empty() || s[0] != *c as u8 {
                    return false;
                }
                self.match_recursive(&tokens[1..], &s[1..])
            }
            GlobToken::Single => {
                if s.is_empty() {
                    return false;
                }
                self.match_recursive(&tokens[1..], &s[1..])
            }
            GlobToken::Any => {
                // Try matching zero or more characters
                if self.match_recursive(&tokens[1..], s) {
                    return true;
                }
                if !s.is_empty() {
                    return self.match_recursive(tokens, &s[1..]);
                }
                false
            }
            GlobToken::CharClass(ranges, negated) => {
                if s.is_empty() {
                    return false;
                }
                let ch = s[0] as char;
                let mut in_class = false;
                for (start, end) in ranges {
                    if ch >= *start && ch <= *end {
                        in_class = true;
                        break;
                    }
                }
                if *negated {
                    in_class = !in_class;
                }
                if !in_class {
                    return false;
                }
                self.match_recursive(&tokens[1..], &s[1..])
            }
        }
    }
}

/// Redis-compatible protocol server
/// Supports: INCR, INCRBY, DECR, DECRBY, GET, SET, MGET, PING, QUIT, TTL, EXPIRE, and more
pub struct RedisServer {
    store: Arc<CounterStore>,
    delta_tx: mpsc::Sender<Delta>,
    metrics: Arc<Metrics>,
    peer_registry: Option<SharedPeerRegistry>,
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
            peer_registry: None,
        }
    }

    pub fn with_peer_registry(mut self, registry: SharedPeerRegistry) -> Self {
        self.peer_registry = Some(registry);
        self
    }

    pub async fn serve(self, addr: &str) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Redis-compatible server listening on {}", addr);

        let peer_registry = self.peer_registry;

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            debug!("Redis client connected: {}", peer_addr);

            let store = Arc::clone(&self.store);
            let delta_tx = self.delta_tx.clone();
            let metrics = Arc::clone(&self.metrics);
            let peers = peer_registry.clone();

            metrics.connection_opened();

            tokio::spawn(async move {
                if let Err(e) =
                    handle_connection(socket, store, delta_tx, Arc::clone(&metrics), peers).await
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
    peer_registry: Option<SharedPeerRegistry>,
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
                &peer_registry,
            )
            .await?;
        } else {
            // Inline command format (telnet-style)
            handle_inline_command(
                &line,
                &store,
                &delta_tx,
                &mut response_buf,
                &metrics,
                &mut args,
                &peer_registry,
            );
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
#[allow(clippy::too_many_arguments)]
async fn handle_resp_command(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    first_line: &str,
    store: &Arc<CounterStore>,
    delta_tx: &mpsc::Sender<Delta>,
    response: &mut String,
    metrics: &Arc<Metrics>,
    args: &mut Vec<String>,
    peer_registry: &Option<SharedPeerRegistry>,
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

        // Parse the bulk string length
        let len: usize = line.trim().trim_start_matches('$').parse().unwrap_or(0);

        // Read exactly `len` bytes of data
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;
        let value = String::from_utf8_lossy(&buf).into_owned();

        // Read and discard the trailing \r\n
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;

        // Reuse existing String capacity if available
        if i < prev_len {
            args[i].clear();
            args[i].push_str(&value);
        } else {
            args.push(value);
        }
    }
    // Truncate extra elements from previous larger commands
    args.truncate(count);

    let start = Instant::now();
    execute_command(args, store, delta_tx, response, metrics, peer_registry);
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
    peer_registry: &Option<SharedPeerRegistry>,
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
    execute_command(args, store, delta_tx, response, metrics, peer_registry);
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
    peer_registry: &Option<SharedPeerRegistry>,
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
                        if delta_tx.try_send(delta).is_err() {
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
                    if delta_tx.try_send(delta).is_err() {
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
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'del' command\r\n");
                return;
            }
            let mut deleted = 0;
            for key in &args[1..] {
                if let Some(delta) = store.delete(key) {
                    if let Err(e) = delta_tx.try_send(delta) {
                        tracing::warn!("Failed to queue DEL delta: {}", e);
                        metrics.delta_send_error();
                    }
                    deleted += 1;
                }
            }
            let _ = write!(response, ":{}\r\n", deleted);
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
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'keys' command\r\n");
                return;
            }
            let pattern_str = &args[1];
            let all_keys = store.keys();

            // Limit to prevent OOM on huge keyspaces
            const MAX_KEYS: usize = 10000;

            let matched: Vec<_> = if pattern_str == "*" {
                // Fast path: no filtering needed
                all_keys.iter().take(MAX_KEYS).collect()
            } else {
                // Compile pattern once, filter all keys
                let pattern = GlobPattern::compile(pattern_str);
                all_keys
                    .iter()
                    .filter(|k| pattern.matches(k.as_ref()))
                    .take(MAX_KEYS)
                    .collect()
            };

            let _ = write!(response, "*{}\r\n", matched.len());
            for key in matched {
                let _ = write!(response, "${}\r\n{}\r\n", key.len(), key);
            }
        }

        "COMMAND" => response.push_str("*0\r\n"),

        "SCAN" => {
            // Parse: SCAN cursor [MATCH pattern] [COUNT count]
            let cursor: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let mut count: usize = 100;
            let mut pattern: Option<GlobPattern> = None;

            // Parse optional MATCH and COUNT arguments
            let mut i = 2;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "MATCH" => {
                        if i + 1 < args.len() {
                            let pat = &args[i + 1];
                            // Only compile pattern if it's not just "*"
                            if pat != "*" {
                                pattern = Some(GlobPattern::compile(pat));
                            }
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    "COUNT" => {
                        if i + 1 < args.len() {
                            count = args[i + 1].parse().unwrap_or(100);
                            // Clamp count to reasonable bounds
                            count = count.clamp(1, 10000);
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    _ => i += 1,
                }
            }

            // Use efficient scan_keys with early termination
            let (matched_keys, next_cursor) = if let Some(ref pat) = pattern {
                // Scan with pattern filter
                store.scan_keys(cursor, count, |key| pat.matches(key))
            } else {
                // Scan without filter (match all)
                store.scan_keys(cursor, count, |_| true)
            };

            let cursor_str = next_cursor.to_string();
            let _ = write!(
                response,
                "*2\r\n${}\r\n{}\r\n",
                cursor_str.len(),
                next_cursor
            );
            let _ = write!(response, "*{}\r\n", matched_keys.len());
            for key in matched_keys {
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

        "CLUSTER" => {
            if args.len() < 2 {
                response.push_str("-ERR wrong number of arguments for 'cluster' command\r\n");
                return;
            }
            match args[1].to_uppercase().as_str() {
                "PEERS" => {
                    // Return list of known peers from gossip registry
                    match peer_registry {
                        Some(registry) => {
                            let peers: Vec<_> = registry.iter().collect();
                            let _ = write!(response, "*{}\r\n", peers.len());
                            for peer in peers {
                                let info = format!(
                                    "{}|{}|{:?}",
                                    peer.replica_id, peer.replication_addr, peer.state
                                );
                                let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
                            }
                        }
                        None => {
                            response.push_str("-ERR discovery not enabled\r\n");
                        }
                    }
                }
                "INFO" => {
                    // Return cluster info as bulk string
                    match peer_registry {
                        Some(registry) => {
                            let peer_count = registry.len();
                            let replica_id = store.local_replica_id();
                            let info =
                                format!("replica_id:{}\npeer_count:{}\n", replica_id, peer_count);
                            let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
                        }
                        None => {
                            let replica_id = store.local_replica_id();
                            let info = format!(
                                "replica_id:{}\npeer_count:0\ndiscovery:disabled\n",
                                replica_id
                            );
                            let _ = write!(response, "${}\r\n{}\r\n", info.len(), info);
                        }
                    }
                }
                _ => {
                    response.push_str("-ERR unknown CLUSTER subcommand. Use: PEERS, INFO\r\n");
                }
            }
        }

        _ => {
            let _ = write!(response, "-ERR unknown command '{}'\r\n", cmd);
        }
    }
}
