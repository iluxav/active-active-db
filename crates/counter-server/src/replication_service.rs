use counter_core::{CounterStore, Delta as CoreDelta, DeltaCompactor, DeltaType as CoreDeltaType};
use counter_proto::replication::v1::{
    replication_service_server::ReplicationService, Ack, AntiEntropyRequest, Delta, DeltaBatch,
    DeltaType, Handshake, ReplicationMessage, replication_message::Message,
};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn, instrument};

/// Implementation of the inter-replica ReplicationService
pub struct ReplicationServiceImpl {
    /// The underlying counter store
    store: Arc<CounterStore>,
    /// Broadcast channel for deltas to all connected peers
    delta_broadcast: broadcast::Sender<CoreDelta>,
    /// Protocol version for compatibility checking
    protocol_version: u32,
}

impl ReplicationServiceImpl {
    pub fn new(store: Arc<CounterStore>, delta_broadcast: broadcast::Sender<CoreDelta>) -> Self {
        Self {
            store,
            delta_broadcast,
            protocol_version: 1,
        }
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
    fn to_proto_delta(delta: &CoreDelta) -> Delta {
        Delta {
            key: delta.key.to_string(),
            origin_replica_id: delta.origin_replica_id.to_string(),
            component_value: delta.component_value,
            expires_at_ms: delta.expires_at_ms,
            delta_type: Self::to_proto_delta_type(delta.delta_type),
            string_value: delta.string_value.clone(),
            timestamp_ms: delta.timestamp_ms,
        }
    }

    /// Convert proto Delta to core Delta
    fn from_proto_delta(delta: &Delta) -> CoreDelta {
        let delta_type = Self::from_proto_delta_type(delta.delta_type);

        match delta_type {
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
        }
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationServiceImpl {
    type SyncStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReplicationMessage, Status>> + Send + 'static>>;

    #[instrument(skip(self, request))]
    async fn sync_stream(
        &self,
        request: Request<Streaming<ReplicationMessage>>,
    ) -> Result<Response<Self::SyncStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let store = Arc::clone(&self.store);
        let mut delta_rx = self.delta_broadcast.subscribe();
        let local_replica_id = self.store.local_replica_id().clone();
        let protocol_version = self.protocol_version;

        // Channel for outbound messages
        let (tx, rx) = mpsc::channel(256);

        // Track the connected peer
        let peer_replica_id = Arc::new(RwLock::new(None::<String>));
        let peer_id_clone = Arc::clone(&peer_replica_id);

        // Sequence number for outbound batches
        let sequence = Arc::new(AtomicU64::new(1));
        let sequence_clone = Arc::clone(&sequence);

        // Delta compactor for batching
        let compactor = Arc::new(RwLock::new(DeltaCompactor::new()));
        let compactor_clone = Arc::clone(&compactor);

        // Spawn task to handle inbound messages
        let tx_clone = tx.clone();
        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            // First message should be handshake
            let first_msg = match inbound.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    warn!("Error receiving first message: {}", e);
                    return;
                }
                None => {
                    warn!("Stream closed before handshake");
                    return;
                }
            };

            let peer_id = match first_msg.message {
                Some(Message::Handshake(h)) => {
                    if h.protocol_version != protocol_version {
                        warn!(
                            "Protocol version mismatch: expected {}, got {}",
                            protocol_version, h.protocol_version
                        );
                        return;
                    }
                    info!("Received handshake from replica: {}", h.replica_id);
                    h.replica_id
                }
                _ => {
                    warn!("Expected handshake as first message");
                    return;
                }
            };

            *peer_id_clone.write().await = Some(peer_id.clone());

            // Send our handshake response
            let handshake = ReplicationMessage {
                message: Some(Message::Handshake(Handshake {
                    replica_id: local_replica_id.to_string(),
                    protocol_version,
                })),
            };
            if tx_clone.send(Ok(handshake)).await.is_err() {
                return;
            }

            // Process subsequent messages
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(msg) => {
                        if let Some(message) = msg.message {
                            match message {
                                Message::DeltaBatch(batch) => {
                                    debug!(
                                        "Received delta batch seq={} with {} deltas from {}",
                                        batch.sequence,
                                        batch.deltas.len(),
                                        peer_id
                                    );

                                    // Apply deltas
                                    for proto_delta in &batch.deltas {
                                        let delta = Self::from_proto_delta(proto_delta);
                                        store_clone.apply_delta(&delta);
                                    }

                                    // Send ack
                                    let ack = ReplicationMessage {
                                        message: Some(Message::Ack(Ack {
                                            sequence: batch.sequence,
                                        })),
                                    };
                                    if tx_clone.send(Ok(ack)).await.is_err() {
                                        break;
                                    }
                                }
                                Message::Ack(ack) => {
                                    debug!("Received ack for sequence {} from {}", ack.sequence, peer_id);
                                }
                                Message::Handshake(_) => {
                                    warn!("Unexpected handshake after initial handshake");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Error receiving message from {}: {}", peer_id, e);
                        break;
                    }
                }
            }

            info!("Sync stream closed for peer: {}", peer_id);
        });

        // Spawn task to forward deltas to this peer
        let tx_for_deltas = tx.clone();
        tokio::spawn(async move {
            // Batch interval timer
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(50));

            loop {
                tokio::select! {
                    // Receive delta from broadcast
                    delta_result = delta_rx.recv() => {
                        match delta_result {
                            Ok(delta) => {
                                compactor_clone.write().await.add(delta);
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("Lagged {} messages in delta broadcast", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                debug!("Delta broadcast channel closed");
                                break;
                            }
                        }
                    }

                    // Periodic flush of batched deltas
                    _ = interval.tick() => {
                        let mut compactor = compactor_clone.write().await;
                        if !compactor.is_empty() {
                            let deltas = compactor.drain();
                            let seq = sequence_clone.fetch_add(1, Ordering::SeqCst);

                            let batch = ReplicationMessage {
                                message: Some(Message::DeltaBatch(DeltaBatch {
                                    sequence: seq,
                                    deltas: deltas.iter().map(Self::to_proto_delta).collect(),
                                })),
                            };

                            if tx_for_deltas.send(Ok(batch)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    type AntiEntropyStream =
        Pin<Box<dyn Stream<Item = Result<Delta, Status>> + Send + 'static>>;

    #[instrument(skip(self, request))]
    async fn anti_entropy(
        &self,
        request: Request<AntiEntropyRequest>,
    ) -> Result<Response<Self::AntiEntropyStream>, Status> {
        let req = request.into_inner();
        info!(
            "Anti-entropy request from replica: {}",
            req.replica_id
        );

        // Get all deltas from the store
        let all_deltas = self.store.all_deltas();
        info!("Sending {} deltas for anti-entropy", all_deltas.len());

        // Convert to stream
        let deltas: Vec<Result<Delta, Status>> = all_deltas
            .into_iter()
            .map(|d| Ok(Self::to_proto_delta(&d)))
            .collect();

        let stream = tokio_stream::iter(deltas);
        Ok(Response::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_conversion_p() {
        let core_delta = CoreDelta::new("key1".into(), "r1".into(), 100);
        let proto_delta = ReplicationServiceImpl::to_proto_delta(&core_delta);

        assert_eq!(proto_delta.key, "key1");
        assert_eq!(proto_delta.origin_replica_id, "r1");
        assert_eq!(proto_delta.component_value, 100);
        assert_eq!(proto_delta.delta_type, DeltaType::P as i32);

        let back = ReplicationServiceImpl::from_proto_delta(&proto_delta);
        assert_eq!(back.key, core_delta.key);
        assert_eq!(back.origin_replica_id, core_delta.origin_replica_id);
        assert_eq!(back.component_value, core_delta.component_value);
        assert_eq!(back.delta_type, CoreDeltaType::P);
    }

    #[test]
    fn test_delta_conversion_n() {
        let core_delta = CoreDelta::with_type("key1".into(), "r1".into(), 50, CoreDeltaType::N);
        let proto_delta = ReplicationServiceImpl::to_proto_delta(&core_delta);

        assert_eq!(proto_delta.delta_type, DeltaType::N as i32);
        assert_eq!(proto_delta.component_value, 50);

        let back = ReplicationServiceImpl::from_proto_delta(&proto_delta);
        assert_eq!(back.delta_type, CoreDeltaType::N);
        assert_eq!(back.component_value, 50);
    }

    #[test]
    fn test_delta_conversion_string() {
        let core_delta = CoreDelta::string(
            "mykey".into(),
            "r1".into(),
            "hello world".to_string(),
            1234567890,
            None,
        );
        let proto_delta = ReplicationServiceImpl::to_proto_delta(&core_delta);

        assert_eq!(proto_delta.key, "mykey");
        assert_eq!(proto_delta.delta_type, DeltaType::S as i32);
        assert_eq!(proto_delta.string_value, Some("hello world".to_string()));
        assert_eq!(proto_delta.timestamp_ms, Some(1234567890));

        let back = ReplicationServiceImpl::from_proto_delta(&proto_delta);
        assert_eq!(back.delta_type, CoreDeltaType::S);
        assert_eq!(back.string_value, Some("hello world".to_string()));
        assert_eq!(back.timestamp_ms, Some(1234567890));
    }
}
