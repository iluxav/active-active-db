use crate::metrics::Metrics;
use a2db_core::{CounterStore, Delta as CoreDelta, DeltaCompactor, DeltaType as CoreDeltaType};
use a2db_proto::replication::v1::{
    replication_message::Message, replication_service_client::ReplicationServiceClient,
    AntiEntropyRequest, DeltaBatch, DeltaType, Handshake, ReplicationMessage,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, watch, RwLock};
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Get current time in milliseconds since Unix epoch
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Client for connecting to a peer replica and exchanging deltas
pub struct ReplicationClient {
    /// Peer address to connect to
    peer_addr: String,
    /// Local replica ID
    local_replica_id: String,
    /// The counter store for applying received deltas
    store: Arc<CounterStore>,
    /// Channel to receive deltas for sending to peer
    delta_rx: broadcast::Receiver<CoreDelta>,
    /// Protocol version
    protocol_version: u32,
    /// Metrics for tracking replication stats
    metrics: Arc<Metrics>,
    /// Optional shutdown signal for dynamic peer management
    shutdown_rx: Option<watch::Receiver<bool>>,
}

impl ReplicationClient {
    pub fn new(
        peer_addr: String,
        local_replica_id: String,
        store: Arc<CounterStore>,
        delta_rx: broadcast::Receiver<CoreDelta>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            peer_addr,
            local_replica_id,
            store,
            delta_rx,
            protocol_version: 1,
            metrics,
            shutdown_rx: None,
        }
    }

    /// Create a new ReplicationClient with a shutdown signal for dynamic peer management
    pub fn new_with_shutdown(
        peer_addr: String,
        local_replica_id: String,
        store: Arc<CounterStore>,
        delta_rx: broadcast::Receiver<CoreDelta>,
        metrics: Arc<Metrics>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            peer_addr,
            local_replica_id,
            store,
            delta_rx,
            protocol_version: 1,
            metrics,
            shutdown_rx: Some(shutdown_rx),
        }
    }

    /// Check if shutdown was signaled
    fn is_shutdown(&self) -> bool {
        if let Some(ref rx) = self.shutdown_rx {
            *rx.borrow()
        } else {
            false
        }
    }

    /// Start the replication client with automatic reconnection
    pub async fn run(mut self) {
        let mut retry_delay = Duration::from_secs(1);
        let max_retry_delay = Duration::from_secs(60);

        loop {
            // Check for shutdown before attempting connection
            if self.is_shutdown() {
                info!(
                    "Shutdown signal received, stopping ReplicationClient for {}",
                    self.peer_addr
                );
                return;
            }

            info!("Connecting to peer: {}", self.peer_addr);

            match self.connect_and_sync().await {
                Ok(()) => {
                    info!("Disconnected from peer: {}", self.peer_addr);
                    retry_delay = Duration::from_secs(1); // Reset on successful connection
                }
                Err(e) => {
                    error!("Connection to {} failed: {}", self.peer_addr, e);
                }
            }

            // Check for shutdown before sleeping
            if self.is_shutdown() {
                info!(
                    "Shutdown signal received, stopping ReplicationClient for {}",
                    self.peer_addr
                );
                return;
            }

            // Exponential backoff with jitter, but also watch for shutdown
            let jitter = Duration::from_millis(rand_jitter(500));
            let delay = retry_delay + jitter;
            warn!("Reconnecting to {} in {:?}", self.peer_addr, delay);

            // Sleep but also check for shutdown signal
            if let Some(ref mut rx) = self.shutdown_rx {
                tokio::select! {
                    _ = sleep(delay) => {}
                    _ = rx.changed() => {
                        if *rx.borrow() {
                            info!(
                                "Shutdown signal received during backoff, stopping ReplicationClient for {}",
                                self.peer_addr
                            );
                            return;
                        }
                    }
                }
            } else {
                sleep(delay).await;
            }

            // Increase delay for next retry
            retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
        }
    }

    async fn connect_and_sync(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Connect to peer
        let channel = Channel::from_shared(self.peer_addr.clone())?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .connect()
            .await?;

        let mut client = ReplicationServiceClient::new(channel);

        // Create bidirectional stream
        let (tx, rx) = tokio::sync::mpsc::channel::<ReplicationMessage>(256);
        let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);

        let response = client.sync_stream(outbound).await?;
        let mut inbound = response.into_inner();

        // Send handshake
        let handshake = ReplicationMessage {
            message: Some(Message::Handshake(Handshake {
                replica_id: self.local_replica_id.clone(),
                protocol_version: self.protocol_version,
            })),
        };
        tx.send(handshake).await?;

        // Wait for peer's handshake
        let peer_handshake = match inbound.next().await {
            Some(Ok(msg)) => match msg.message {
                Some(Message::Handshake(h)) => h,
                _ => return Err("Expected handshake response".into()),
            },
            Some(Err(e)) => return Err(format!("Error receiving handshake: {}", e).into()),
            None => return Err("Stream closed before handshake".into()),
        };

        info!(
            "Connected to peer: {} (protocol v{})",
            peer_handshake.replica_id, peer_handshake.protocol_version
        );

        // Perform initial anti-entropy sync
        self.perform_anti_entropy(&mut client).await?;

        // Delta compactor and sequence number
        let compactor = Arc::new(RwLock::new(DeltaCompactor::new()));
        let sequence = Arc::new(AtomicU64::new(1));

        // Spawn task to receive deltas from broadcast and send to peer
        let compactor_clone = Arc::clone(&compactor);
        let sequence_clone = Arc::clone(&sequence);
        let tx_clone = tx.clone();
        let mut delta_rx = self.delta_rx.resubscribe();
        let metrics_clone = Arc::clone(&self.metrics);

        let sender_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));

            loop {
                tokio::select! {
                    delta_result = delta_rx.recv() => {
                        match delta_result {
                            Ok(delta) => {
                                compactor_clone.write().await.add(delta);
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("Lagged {} messages", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                        }
                    }

                    _ = interval.tick() => {
                        let mut comp = compactor_clone.write().await;
                        if !comp.is_empty() {
                            let deltas = comp.drain();
                            let delta_count = deltas.len();
                            let seq = sequence_clone.fetch_add(1, Ordering::SeqCst);

                            let batch = ReplicationMessage {
                                message: Some(Message::DeltaBatch(DeltaBatch {
                                    sequence: seq,
                                    deltas: deltas.iter().map(to_proto_delta).collect(),
                                })),
                            };

                            if tx_clone.send(batch).await.is_ok() {
                                for _ in 0..delta_count {
                                    metrics_clone.delta_sent();
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Process inbound messages
        let store = Arc::clone(&self.store);
        while let Some(result) = inbound.next().await {
            match result {
                Ok(msg) => {
                    if let Some(message) = msg.message {
                        match message {
                            Message::DeltaBatch(batch) => {
                                debug!(
                                    "Received batch seq={} with {} deltas",
                                    batch.sequence,
                                    batch.deltas.len()
                                );

                                let receive_time = now_ms();
                                for proto_delta in &batch.deltas {
                                    let delta = from_proto_delta(proto_delta);
                                    store.apply_delta(&delta);
                                    self.metrics.delta_received();
                                    // Record replication latency if timestamp is valid
                                    if proto_delta.created_at_ms > 0 {
                                        let latency =
                                            receive_time.saturating_sub(proto_delta.created_at_ms);
                                        self.metrics.record_replication_latency(latency);
                                    }
                                }

                                // Send ack
                                let ack = ReplicationMessage {
                                    message: Some(Message::Ack(a2db_proto::replication::v1::Ack {
                                        sequence: batch.sequence,
                                    })),
                                };
                                if tx.send(ack).await.is_err() {
                                    break;
                                }
                            }
                            Message::Ack(ack) => {
                                debug!("Received ack for seq={}", ack.sequence);
                            }
                            Message::Handshake(_) => {
                                warn!("Unexpected handshake");
                            }
                            Message::Gossip(gossip_msg) => {
                                // Gossip messages are handled by GossipManager when discovery is enabled
                                debug!("Received gossip message: {:?}", gossip_msg);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving: {}", e);
                    break;
                }
            }
        }

        sender_handle.abort();
        Ok(())
    }

    /// Perform anti-entropy sync to get full state from peer
    async fn perform_anti_entropy(
        &self,
        client: &mut ReplicationServiceClient<Channel>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Performing anti-entropy sync with peer");

        let request = AntiEntropyRequest {
            replica_id: self.local_replica_id.clone(),
        };

        let response = client.anti_entropy(request).await?;
        let mut stream = response.into_inner();

        let mut count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(proto_delta) => {
                    let receive_time = now_ms();
                    let core_delta = from_proto_delta(&proto_delta);
                    self.store.apply_delta(&core_delta);
                    self.metrics.delta_received();
                    // Record replication latency if timestamp is valid
                    if proto_delta.created_at_ms > 0 {
                        let latency = receive_time.saturating_sub(proto_delta.created_at_ms);
                        self.metrics.record_replication_latency(latency);
                    }
                    count += 1;
                }
                Err(e) => {
                    error!("Error during anti-entropy: {}", e);
                    break;
                }
            }
        }

        info!("Anti-entropy complete: applied {} deltas", count);
        Ok(())
    }
}

/// Generate random jitter in milliseconds
fn rand_jitter(max_ms: u64) -> u64 {
    // Simple PRNG based on time
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64;
    nanos % max_ms
}

/// Convert core DeltaType to proto DeltaType
fn to_proto_delta_type(dt: CoreDeltaType) -> i32 {
    match dt {
        CoreDeltaType::P => DeltaType::P as i32,
        CoreDeltaType::N => DeltaType::N as i32,
        CoreDeltaType::S => DeltaType::S as i32,
    }
}

/// Convert proto DeltaType to core DeltaType
fn from_proto_delta_type(dt: i32) -> CoreDeltaType {
    match DeltaType::try_from(dt) {
        Ok(DeltaType::N) => CoreDeltaType::N,
        Ok(DeltaType::S) => CoreDeltaType::S,
        _ => CoreDeltaType::P, // Default to P for backwards compatibility
    }
}

/// Convert core Delta to proto Delta
fn to_proto_delta(d: &CoreDelta) -> a2db_proto::replication::v1::Delta {
    a2db_proto::replication::v1::Delta {
        key: d.key.to_string(),
        origin_replica_id: d.origin_replica_id.to_string(),
        component_value: d.component_value,
        expires_at_ms: d.expires_at_ms,
        delta_type: to_proto_delta_type(d.delta_type),
        string_value: d.string_value.clone(),
        timestamp_ms: d.timestamp_ms,
        created_at_ms: d.created_at_ms,
    }
}

/// Convert proto Delta to core Delta
fn from_proto_delta(delta: &a2db_proto::replication::v1::Delta) -> CoreDelta {
    let delta_type = from_proto_delta_type(delta.delta_type);

    let mut core_delta = match delta_type {
        CoreDeltaType::S => CoreDelta::string(
            delta.key.as_str().into(),
            delta.origin_replica_id.as_str().into(),
            delta.string_value.clone().unwrap_or_default(),
            delta.timestamp_ms.unwrap_or(0),
            delta.expires_at_ms,
        ),
        _ => CoreDelta::with_type_and_expiration(
            delta.key.as_str().into(),
            delta.origin_replica_id.as_str().into(),
            delta.component_value,
            delta_type,
            delta.expires_at_ms,
        ),
    };
    // Preserve the original creation timestamp for latency measurement
    core_delta.created_at_ms = delta.created_at_ms;
    core_delta
}
