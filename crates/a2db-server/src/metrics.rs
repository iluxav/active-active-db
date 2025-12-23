use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

/// Thread-safe metrics for a2db
pub struct Metrics {
    // Connection metrics
    pub connections_active: AtomicU64,
    pub connections_total: AtomicU64,

    // Command metrics
    pub cmd_incr_total: AtomicU64,
    pub cmd_decr_total: AtomicU64,
    pub cmd_get_total: AtomicU64,
    pub cmd_set_total: AtomicU64,
    pub cmd_mget_total: AtomicU64,
    pub cmd_expire_total: AtomicU64,
    pub cmd_ttl_total: AtomicU64,
    pub cmd_keys_total: AtomicU64,
    pub cmd_ping_total: AtomicU64,
    pub cmd_info_total: AtomicU64,
    pub cmd_other_total: AtomicU64,

    // Latency tracking (sum of microseconds for averaging)
    pub cmd_incr_latency_us: AtomicU64,
    pub cmd_get_latency_us: AtomicU64,
    pub cmd_set_latency_us: AtomicU64,

    // Replication metrics
    pub deltas_sent_total: AtomicU64,
    pub deltas_received_total: AtomicU64,
    pub deltas_send_errors: AtomicU64,

    // Replication latency tracking (milliseconds)
    pub replication_latency_sum_ms: AtomicU64,
    pub replication_latency_count: AtomicU64,
    pub replication_latency_min_ms: AtomicU64,
    pub replication_latency_max_ms: AtomicU64,

    // Store metrics
    pub keys_total: AtomicU64,

    // Startup time for uptime calculation
    start_time: std::time::SystemTime,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            connections_active: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            cmd_incr_total: AtomicU64::new(0),
            cmd_decr_total: AtomicU64::new(0),
            cmd_get_total: AtomicU64::new(0),
            cmd_set_total: AtomicU64::new(0),
            cmd_mget_total: AtomicU64::new(0),
            cmd_expire_total: AtomicU64::new(0),
            cmd_ttl_total: AtomicU64::new(0),
            cmd_keys_total: AtomicU64::new(0),
            cmd_ping_total: AtomicU64::new(0),
            cmd_info_total: AtomicU64::new(0),
            cmd_other_total: AtomicU64::new(0),
            cmd_incr_latency_us: AtomicU64::new(0),
            cmd_get_latency_us: AtomicU64::new(0),
            cmd_set_latency_us: AtomicU64::new(0),
            deltas_sent_total: AtomicU64::new(0),
            deltas_received_total: AtomicU64::new(0),
            deltas_send_errors: AtomicU64::new(0),
            replication_latency_sum_ms: AtomicU64::new(0),
            replication_latency_count: AtomicU64::new(0),
            replication_latency_min_ms: AtomicU64::new(u64::MAX),
            replication_latency_max_ms: AtomicU64::new(0),
            keys_total: AtomicU64::new(0),
            start_time: std::time::SystemTime::now(),
        }
    }

    /// Record a command with latency tracking
    pub fn record_command(&self, cmd: &str, start: Instant) {
        let latency_us = start.elapsed().as_micros() as u64;

        match cmd.to_uppercase().as_str() {
            "INCR" | "INCRBY" => {
                self.cmd_incr_total.fetch_add(1, Ordering::Relaxed);
                self.cmd_incr_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
            }
            "DECR" | "DECRBY" => {
                self.cmd_decr_total.fetch_add(1, Ordering::Relaxed);
            }
            "GET" => {
                self.cmd_get_total.fetch_add(1, Ordering::Relaxed);
                self.cmd_get_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
            }
            "SET" => {
                self.cmd_set_total.fetch_add(1, Ordering::Relaxed);
                self.cmd_set_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
            }
            "MGET" => {
                self.cmd_mget_total.fetch_add(1, Ordering::Relaxed);
            }
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
                self.cmd_expire_total.fetch_add(1, Ordering::Relaxed);
            }
            "TTL" | "PTTL" => {
                self.cmd_ttl_total.fetch_add(1, Ordering::Relaxed);
            }
            "KEYS" | "SCAN" | "DBSIZE" => {
                self.cmd_keys_total.fetch_add(1, Ordering::Relaxed);
            }
            "PING" => {
                self.cmd_ping_total.fetch_add(1, Ordering::Relaxed);
            }
            "INFO" => {
                self.cmd_info_total.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.cmd_other_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn connection_opened(&self) {
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn delta_sent(&self) {
        self.deltas_sent_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn delta_send_error(&self) {
        self.deltas_send_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn delta_received(&self) {
        self.deltas_received_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record replication latency for a received delta
    pub fn record_replication_latency(&self, latency_ms: u64) {
        self.replication_latency_sum_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.replication_latency_count
            .fetch_add(1, Ordering::Relaxed);

        // Update min (CAS loop)
        loop {
            let current_min = self.replication_latency_min_ms.load(Ordering::Relaxed);
            if latency_ms >= current_min {
                break;
            }
            if self
                .replication_latency_min_ms
                .compare_exchange_weak(
                    current_min,
                    latency_ms,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // Update max (CAS loop)
        loop {
            let current_max = self.replication_latency_max_ms.load(Ordering::Relaxed);
            if latency_ms <= current_max {
                break;
            }
            if self
                .replication_latency_max_ms
                .compare_exchange_weak(
                    current_max,
                    latency_ms,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn set_keys_total(&self, count: u64) {
        self.keys_total.store(count, Ordering::Relaxed);
    }

    /// Get snapshot of all metrics for JSON serialization
    pub fn snapshot(&self) -> MetricsSnapshot {
        let uptime_secs = self.start_time.elapsed().map(|d| d.as_secs()).unwrap_or(0);

        let incr_count = self.cmd_incr_total.load(Ordering::Relaxed);
        let get_count = self.cmd_get_total.load(Ordering::Relaxed);
        let set_count = self.cmd_set_total.load(Ordering::Relaxed);

        MetricsSnapshot {
            uptime_seconds: uptime_secs,
            connections: ConnectionMetrics {
                active: self.connections_active.load(Ordering::Relaxed),
                total: self.connections_total.load(Ordering::Relaxed),
            },
            commands: CommandMetrics {
                incr: self.cmd_incr_total.load(Ordering::Relaxed),
                decr: self.cmd_decr_total.load(Ordering::Relaxed),
                get: self.cmd_get_total.load(Ordering::Relaxed),
                set: self.cmd_set_total.load(Ordering::Relaxed),
                mget: self.cmd_mget_total.load(Ordering::Relaxed),
                expire: self.cmd_expire_total.load(Ordering::Relaxed),
                ttl: self.cmd_ttl_total.load(Ordering::Relaxed),
                keys: self.cmd_keys_total.load(Ordering::Relaxed),
                ping: self.cmd_ping_total.load(Ordering::Relaxed),
                info: self.cmd_info_total.load(Ordering::Relaxed),
                other: self.cmd_other_total.load(Ordering::Relaxed),
            },
            latency_avg_us: LatencyMetrics {
                incr: if incr_count > 0 {
                    self.cmd_incr_latency_us.load(Ordering::Relaxed) / incr_count
                } else {
                    0
                },
                get: if get_count > 0 {
                    self.cmd_get_latency_us.load(Ordering::Relaxed) / get_count
                } else {
                    0
                },
                set: if set_count > 0 {
                    self.cmd_set_latency_us.load(Ordering::Relaxed) / set_count
                } else {
                    0
                },
            },
            replication: {
                let latency_count = self.replication_latency_count.load(Ordering::Relaxed);
                let latency_min = self.replication_latency_min_ms.load(Ordering::Relaxed);
                ReplicationMetrics {
                    deltas_sent: self.deltas_sent_total.load(Ordering::Relaxed),
                    deltas_received: self.deltas_received_total.load(Ordering::Relaxed),
                    send_errors: self.deltas_send_errors.load(Ordering::Relaxed),
                    latency_avg_ms: if latency_count > 0 {
                        self.replication_latency_sum_ms.load(Ordering::Relaxed) / latency_count
                    } else {
                        0
                    },
                    latency_min_ms: if latency_min == u64::MAX { 0 } else { latency_min },
                    latency_max_ms: self.replication_latency_max_ms.load(Ordering::Relaxed),
                }
            },
            store: StoreMetrics {
                keys: self.keys_total.load(Ordering::Relaxed),
            },
        }
    }
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub uptime_seconds: u64,
    pub connections: ConnectionMetrics,
    pub commands: CommandMetrics,
    pub latency_avg_us: LatencyMetrics,
    pub replication: ReplicationMetrics,
    pub store: StoreMetrics,
}

#[derive(Serialize)]
pub struct ConnectionMetrics {
    pub active: u64,
    pub total: u64,
}

#[derive(Serialize)]
pub struct CommandMetrics {
    pub incr: u64,
    pub decr: u64,
    pub get: u64,
    pub set: u64,
    pub mget: u64,
    pub expire: u64,
    pub ttl: u64,
    pub keys: u64,
    pub ping: u64,
    pub info: u64,
    pub other: u64,
}

#[derive(Serialize)]
pub struct LatencyMetrics {
    pub incr: u64,
    pub get: u64,
    pub set: u64,
}

#[derive(Serialize)]
pub struct ReplicationMetrics {
    pub deltas_sent: u64,
    pub deltas_received: u64,
    pub send_errors: u64,
    pub latency_avg_ms: u64,
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
}

#[derive(Serialize)]
pub struct StoreMetrics {
    pub keys: u64,
}

/// Simple HTTP server for metrics endpoint
pub struct MetricsServer {
    metrics: Arc<Metrics>,
}

impl MetricsServer {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }

    pub async fn serve(&self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!(addr = %addr, "Metrics server started");

        loop {
            let (mut socket, _) = listener.accept().await?;
            let metrics = Arc::clone(&self.metrics);

            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                if socket.read(&mut buf).await.is_err() {
                    return;
                }

                let request = String::from_utf8_lossy(&buf);

                // Simple HTTP routing
                let (status, body) =
                    if request.starts_with("GET /metrics") || request.starts_with("GET / ") {
                        let snapshot = metrics.snapshot();
                        match serde_json::to_string_pretty(&snapshot) {
                            Ok(json) => ("200 OK", json),
                            Err(e) => (
                                "500 Internal Server Error",
                                format!("{{\"error\": \"{}\"}}", e),
                            ),
                        }
                    } else if request.starts_with("GET /health") {
                        ("200 OK", "{\"status\": \"ok\"}".to_string())
                    } else {
                        ("404 Not Found", "{\"error\": \"not found\"}".to_string())
                    };

                let response = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    body.len(),
                    body
                );

                if let Err(e) = socket.write_all(response.as_bytes()).await {
                    error!("Failed to write metrics response: {}", e);
                }
            });
        }
    }
}
