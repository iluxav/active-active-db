//! Manages ReplicationClient lifecycle based on peer discovery events
//!
//! This module spawns and stops ReplicationClient instances as peers
//! join and leave the cluster through gossip-based discovery.

use crate::gossip::{PeerEvent, PeerInfo, PeerState};
use crate::metrics::Metrics;
use crate::replication_client::ReplicationClient;
use a2db_core::{CounterStore, Delta};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Handle for a running ReplicationClient
struct ClientHandle {
    /// Channel to signal shutdown
    shutdown_tx: watch::Sender<bool>,
    /// The spawned task
    task: JoinHandle<()>,
}

/// Manages active ReplicationClient connections based on gossip events
pub struct PeerManager {
    /// Local replica ID (to avoid connecting to self)
    local_replica_id: String,
    /// The counter store shared with clients
    store: Arc<CounterStore>,
    /// Broadcast sender for deltas (clients subscribe to this)
    delta_broadcast: broadcast::Sender<Delta>,
    /// Metrics for tracking
    metrics: Arc<Metrics>,
    /// Active client handles indexed by replica_id
    active_clients: HashMap<String, ClientHandle>,
    /// Channel to receive peer events from GossipManager
    event_rx: mpsc::Receiver<PeerEvent>,
}

impl PeerManager {
    pub fn new(
        local_replica_id: String,
        store: Arc<CounterStore>,
        delta_broadcast: broadcast::Sender<Delta>,
        metrics: Arc<Metrics>,
        event_rx: mpsc::Receiver<PeerEvent>,
    ) -> Self {
        Self {
            local_replica_id,
            store,
            delta_broadcast,
            metrics,
            active_clients: HashMap::new(),
            event_rx,
        }
    }

    /// Run the peer manager loop
    /// This processes peer events and manages ReplicationClient lifecycle
    pub async fn run(mut self) {
        info!("PeerManager started");

        while let Some(event) = self.event_rx.recv().await {
            match event {
                PeerEvent::Joined(peer) => {
                    self.handle_peer_joined(peer).await;
                }
                PeerEvent::Left(replica_id) => {
                    self.handle_peer_left(&replica_id).await;
                }
                PeerEvent::StateChanged {
                    replica_id,
                    new_state,
                } => {
                    self.handle_state_changed(&replica_id, new_state).await;
                }
            }
        }

        info!("PeerManager shutting down");

        // Stop all active clients
        for (replica_id, handle) in self.active_clients.drain() {
            info!("Stopping client for {}", replica_id);
            let _ = handle.shutdown_tx.send(true);
            handle.task.abort();
        }
    }

    /// Handle a new peer joining the cluster
    async fn handle_peer_joined(&mut self, peer: PeerInfo) {
        // Don't connect to ourselves
        if peer.replica_id.as_ref() == self.local_replica_id {
            return;
        }

        // Don't connect if we already have a client for this peer
        if self.active_clients.contains_key(peer.replica_id.as_ref()) {
            info!(
                "Already connected to peer {}, skipping",
                peer.replica_id
            );
            return;
        }

        // Don't connect if the peer address is empty
        if peer.replication_addr.is_empty() {
            warn!(
                "Peer {} has empty replication address, skipping",
                peer.replica_id
            );
            return;
        }

        self.start_client(peer).await;
    }

    /// Handle a peer leaving the cluster
    async fn handle_peer_left(&mut self, replica_id: &str) {
        self.stop_client(replica_id).await;
    }

    /// Handle a peer's state changing
    async fn handle_state_changed(&mut self, replica_id: &str, new_state: PeerState) {
        match new_state {
            PeerState::Dead => {
                // Stop the client when peer is dead
                self.stop_client(replica_id).await;
            }
            PeerState::Alive => {
                // Peer came back alive - the existing ReplicationClient should
                // automatically reconnect, so we don't need to do anything here
                info!("Peer {} is alive again", replica_id);
            }
            PeerState::Suspect => {
                // Keep the client running - it may recover
                info!("Peer {} is suspect, keeping connection", replica_id);
            }
        }
    }

    /// Start a ReplicationClient for a peer
    async fn start_client(&mut self, peer: PeerInfo) {
        let replica_id = peer.replica_id.to_string();

        info!(
            "Starting ReplicationClient for peer {} at {}",
            replica_id, peer.replication_addr
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Create the client
        let client = ReplicationClient::new_with_shutdown(
            peer.replication_addr.clone(),
            self.local_replica_id.clone(),
            Arc::clone(&self.store),
            self.delta_broadcast.subscribe(),
            Arc::clone(&self.metrics),
            shutdown_rx,
        );

        // Spawn the client task
        let task = tokio::spawn(async move {
            client.run().await;
        });

        // Store the handle
        self.active_clients.insert(
            replica_id.clone(),
            ClientHandle { shutdown_tx, task },
        );

        info!("ReplicationClient started for peer {}", replica_id);
    }

    /// Stop a ReplicationClient for a peer
    async fn stop_client(&mut self, replica_id: &str) {
        if let Some(handle) = self.active_clients.remove(replica_id) {
            info!("Stopping ReplicationClient for peer {}", replica_id);

            // Signal shutdown
            let _ = handle.shutdown_tx.send(true);

            // Give the task time to shutdown gracefully
            let timeout_result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                handle.task,
            )
            .await;

            match timeout_result {
                Ok(Ok(())) => {
                    info!("ReplicationClient for {} stopped gracefully", replica_id);
                }
                Ok(Err(e)) => {
                    warn!("ReplicationClient for {} panicked: {}", replica_id, e);
                }
                Err(_) => {
                    warn!(
                        "ReplicationClient for {} did not stop in time, aborting",
                        replica_id
                    );
                }
            }
        }
    }

    /// Get count of active clients
    #[allow(dead_code)]
    pub fn active_client_count(&self) -> usize {
        self.active_clients.len()
    }
}
