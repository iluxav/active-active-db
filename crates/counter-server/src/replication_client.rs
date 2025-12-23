use counter_core::{CounterStore, Delta as CoreDelta, DeltaCompactor};
use counter_proto::replication::v1::{
    replication_service_client::ReplicationServiceClient, AntiEntropyRequest, DeltaBatch,
    Handshake, ReplicationMessage, replication_message::Message,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

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
}

impl ReplicationClient {
    pub fn new(
        peer_addr: String,
        local_replica_id: String,
        store: Arc<CounterStore>,
        delta_rx: broadcast::Receiver<CoreDelta>,
    ) -> Self {
        Self {
            peer_addr,
            local_replica_id,
            store,
            delta_rx,
            protocol_version: 1,
        }
    }

    /// Start the replication client with automatic reconnection
    pub async fn run(mut self) {
        let mut retry_delay = Duration::from_secs(1);
        let max_retry_delay = Duration::from_secs(60);

        loop {
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

            // Exponential backoff with jitter
            let jitter = Duration::from_millis(rand_jitter(500));
            let delay = retry_delay + jitter;
            warn!(
                "Reconnecting to {} in {:?}",
                self.peer_addr, delay
            );
            sleep(delay).await;

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
                            let seq = sequence_clone.fetch_add(1, Ordering::SeqCst);

                            let batch = ReplicationMessage {
                                message: Some(Message::DeltaBatch(DeltaBatch {
                                    sequence: seq,
                                    deltas: deltas.iter().map(|d| {
                                        counter_proto::replication::v1::Delta {
                                            key: d.key.to_string(),
                                            origin_replica_id: d.origin_replica_id.to_string(),
                                            component_value: d.component_value,
                                            expires_at_ms: d.expires_at_ms,
                                        }
                                    }).collect(),
                                })),
                            };

                            if tx_clone.send(batch).await.is_err() {
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

                                for proto_delta in &batch.deltas {
                                    let delta = CoreDelta::from_strs_with_expiration(
                                        &proto_delta.key,
                                        &proto_delta.origin_replica_id,
                                        proto_delta.component_value,
                                        proto_delta.expires_at_ms,
                                    );
                                    store.apply_delta(&delta);
                                }

                                // Send ack
                                let ack = ReplicationMessage {
                                    message: Some(Message::Ack(
                                        counter_proto::replication::v1::Ack {
                                            sequence: batch.sequence,
                                        },
                                    )),
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
                Ok(delta) => {
                    let core_delta = CoreDelta::from_strs_with_expiration(
                        &delta.key,
                        &delta.origin_replica_id,
                        delta.component_value,
                        delta.expires_at_ms,
                    );
                    self.store.apply_delta(&core_delta);
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
