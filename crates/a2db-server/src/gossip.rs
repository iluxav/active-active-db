//! Gossip-based peer discovery and failure detection
//!
//! Implements a SWIM-like protocol for:
//! - Peer discovery via seed nodes
//! - Failure detection via heartbeats
//! - Cluster membership dissemination

use a2db_proto::replication::v1::{
    replication_service_client::ReplicationServiceClient, JoinRequest, PeerInfo as ProtoPeerInfo,
    PeerState as ProtoPeerState,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, sleep, timeout};
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

/// Get current time in milliseconds since Unix epoch
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub type ReplicaId = Arc<str>;

/// Shared peer registry that can be accessed by multiple components
pub type SharedPeerRegistry = Arc<DashMap<ReplicaId, PeerInfo>>;

/// Peer health state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    Alive,
    Suspect,
    Dead,
}

impl From<i32> for PeerState {
    fn from(v: i32) -> Self {
        match v {
            1 => PeerState::Alive,
            2 => PeerState::Suspect,
            3 => PeerState::Dead,
            _ => PeerState::Alive,
        }
    }
}

impl From<PeerState> for i32 {
    fn from(s: PeerState) -> Self {
        match s {
            PeerState::Alive => ProtoPeerState::Alive as i32,
            PeerState::Suspect => ProtoPeerState::Suspect as i32,
            PeerState::Dead => ProtoPeerState::Dead as i32,
        }
    }
}

/// Information about a discovered peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub replica_id: ReplicaId,
    pub replication_addr: String,
    pub incarnation: u64,
    pub state: PeerState,
    pub last_seen: Instant,
    pub suspect_since: Option<Instant>,
}

impl PeerInfo {
    pub fn new(replica_id: ReplicaId, replication_addr: String, incarnation: u64) -> Self {
        Self {
            replica_id,
            replication_addr,
            incarnation,
            state: PeerState::Alive,
            last_seen: Instant::now(),
            suspect_since: None,
        }
    }

    pub fn to_proto(&self) -> ProtoPeerInfo {
        ProtoPeerInfo {
            replica_id: self.replica_id.to_string(),
            replication_addr: self.replication_addr.clone(),
            incarnation: self.incarnation,
            state: self.state.into(),
            last_seen_ms: now_ms(),
        }
    }

    pub fn from_proto(proto: &ProtoPeerInfo) -> Self {
        Self {
            replica_id: Arc::from(proto.replica_id.as_str()),
            replication_addr: proto.replication_addr.clone(),
            incarnation: proto.incarnation,
            state: PeerState::from(proto.state),
            last_seen: Instant::now(),
            suspect_since: None,
        }
    }
}

/// Event sent when peer state changes
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// A new peer has joined the cluster
    Joined(PeerInfo),
    /// A peer has been removed from the cluster
    Left(ReplicaId),
    /// A peer's state has changed
    StateChanged {
        replica_id: ReplicaId,
        new_state: PeerState,
    },
}

/// Configuration for the gossip subsystem
#[derive(Debug, Clone)]
pub struct GossipConfig {
    pub enabled: bool,
    pub seeds: Vec<String>,
    pub gossip_interval: Duration,
    pub gossip_fanout: usize,
    pub heartbeat_interval: Duration,
    pub suspect_threshold: u32,
    pub suspect_timeout: Duration,
    pub dead_timeout: Duration,
    pub bootstrap_timeout: Duration,
    pub advertise_addr: String,
}

/// Manages gossip protocol and peer discovery
pub struct GossipManager {
    config: GossipConfig,
    local_replica_id: ReplicaId,
    local_incarnation: AtomicU64,
    peers: SharedPeerRegistry,
    event_tx: mpsc::Sender<PeerEvent>,
    shutdown_rx: watch::Receiver<bool>,
}

impl GossipManager {
    pub fn new(
        config: GossipConfig,
        local_replica_id: ReplicaId,
        peers: SharedPeerRegistry,
        event_tx: mpsc::Sender<PeerEvent>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            local_replica_id,
            local_incarnation: AtomicU64::new(now_ms()),
            peers,
            event_tx,
            shutdown_rx,
        }
    }

    /// Get current incarnation number
    pub fn incarnation(&self) -> u64 {
        self.local_incarnation.load(Ordering::SeqCst)
    }

    /// Bump incarnation number (e.g., when refuting a false death announcement)
    pub fn bump_incarnation(&self) {
        self.local_incarnation.fetch_add(1, Ordering::SeqCst);
    }

    /// Get current peer list (for gossip exchange)
    pub fn get_peer_list(&self) -> Vec<PeerInfo> {
        self.peers.iter().map(|e| e.value().clone()).collect()
    }

    /// Get active (Alive) peers for replication
    pub fn get_active_peers(&self) -> Vec<PeerInfo> {
        self.peers
            .iter()
            .filter(|e| e.value().state == PeerState::Alive)
            .map(|e| e.value().clone())
            .collect()
    }

    /// Get peer count by state
    pub fn peer_counts(&self) -> (u64, u64, u64) {
        let mut alive = 0u64;
        let mut suspect = 0u64;
        let mut dead = 0u64;
        for entry in self.peers.iter() {
            match entry.value().state {
                PeerState::Alive => alive += 1,
                PeerState::Suspect => suspect += 1,
                PeerState::Dead => dead += 1,
            }
        }
        (alive, suspect, dead)
    }

    /// Merge incoming peer info (from gossip exchange)
    /// Returns true if the peer was new or updated
    pub async fn merge_peer_info(&self, incoming: PeerInfo) -> bool {
        // Don't add ourselves
        if incoming.replica_id == self.local_replica_id {
            debug!(
                "Skipping peer {} (same as local replica)",
                incoming.replica_id
            );
            return false;
        }

        info!(
            "Merging peer info: {} at {}",
            incoming.replica_id, incoming.replication_addr
        );

        let replica_id = incoming.replica_id.clone();
        let mut is_new = false;

        // Check if we already know this peer
        if let Some(mut existing) = self.peers.get_mut(&replica_id) {
            let existing = existing.value_mut();

            // Higher incarnation always wins
            if incoming.incarnation > existing.incarnation {
                existing.incarnation = incoming.incarnation;
                existing.replication_addr = incoming.replication_addr.clone();
                existing.state = incoming.state;
                existing.last_seen = Instant::now();
                existing.suspect_since = None;
                return true;
            }

            // Same incarnation: prefer Alive > Suspect > Dead
            if incoming.incarnation == existing.incarnation {
                // If incoming says Alive and we think it's Suspect/Dead, update
                if incoming.state == PeerState::Alive
                    && existing.state != PeerState::Alive
                {
                    existing.state = PeerState::Alive;
                    existing.last_seen = Instant::now();
                    existing.suspect_since = None;
                    return true;
                }
            }

            false
        } else {
            // New peer discovered
            is_new = true;
            self.peers.insert(replica_id.clone(), incoming.clone());

            // Notify about new peer
            info!(
                "New peer discovered: {} at {}, sending Joined event",
                incoming.replica_id, incoming.replication_addr
            );
            if let Err(e) = self.event_tx.send(PeerEvent::Joined(incoming)).await {
                warn!("Failed to send peer joined event: {}", e);
            } else {
                info!("PeerEvent::Joined sent successfully");
            }

            is_new
        }
    }

    /// Mark peer as alive (received heartbeat or message)
    pub async fn mark_alive(&self, replica_id: &ReplicaId) {
        if let Some(mut entry) = self.peers.get_mut(replica_id) {
            let peer = entry.value_mut();
            let was_not_alive = peer.state != PeerState::Alive;
            peer.state = PeerState::Alive;
            peer.last_seen = Instant::now();
            peer.suspect_since = None;

            if was_not_alive {
                if let Err(e) = self
                    .event_tx
                    .send(PeerEvent::StateChanged {
                        replica_id: replica_id.clone(),
                        new_state: PeerState::Alive,
                    })
                    .await
                {
                    warn!("Failed to send state changed event: {}", e);
                }
            }
        }
    }

    /// Run the gossip loop (main entry point)
    pub async fn run(&self) {
        info!(
            "Starting GossipManager for replica {} with {} seeds",
            self.local_replica_id,
            self.config.seeds.len()
        );

        // Bootstrap from seeds
        self.bootstrap().await;

        // Run concurrent loops
        let mut gossip_interval = interval(self.config.gossip_interval);
        let mut failure_check_interval = interval(self.config.heartbeat_interval);
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("GossipManager shutting down");
                        return;
                    }
                }

                _ = gossip_interval.tick() => {
                    self.gossip_round().await;
                }

                _ = failure_check_interval.tick() => {
                    self.check_failures().await;
                }
            }
        }
    }

    /// Bootstrap from seed nodes
    async fn bootstrap(&self) {
        if self.config.seeds.is_empty() {
            info!("No seeds configured, starting as standalone node");
            return;
        }

        info!("Bootstrapping from {} seed nodes", self.config.seeds.len());

        let bootstrap_start = Instant::now();
        let mut connected_to_any = false;

        // Try ALL seeds to form a full mesh, not just the first one that responds
        while !connected_to_any && bootstrap_start.elapsed() < self.config.bootstrap_timeout {
            for seed in &self.config.seeds {
                // Skip if seed is ourselves
                if seed == &self.config.advertise_addr {
                    continue;
                }

                match self.try_join_seed(seed).await {
                    Ok(peers) => {
                        info!(
                            "Successfully joined cluster via seed {}, got {} peers",
                            seed,
                            peers.len()
                        );

                        for peer_proto in peers {
                            let peer = PeerInfo::from_proto(&peer_proto);
                            self.merge_peer_info(peer).await;
                        }

                        connected_to_any = true;
                        // Don't break - try all seeds to discover all peers
                    }
                    Err(e) => {
                        debug!("Failed to join via seed {}: {}", seed, e);
                    }
                }
            }

            if !connected_to_any {
                // Wait before retrying
                let remaining = self.config.bootstrap_timeout.saturating_sub(bootstrap_start.elapsed());
                if remaining > Duration::from_secs(1) {
                    debug!("Retrying bootstrap in 1s...");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        if !connected_to_any {
            info!(
                "Bootstrap timeout after {:?}, starting alone. Will wait for incoming connections.",
                self.config.bootstrap_timeout
            );
        }
    }

    /// Try to join the cluster via a seed node
    async fn try_join_seed(
        &self,
        seed_addr: &str,
    ) -> Result<Vec<ProtoPeerInfo>, Box<dyn std::error::Error + Send + Sync>> {
        let channel = Channel::from_shared(seed_addr.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .connect()
            .await?;

        let mut client = ReplicationServiceClient::new(channel);

        let request = JoinRequest {
            replica_id: self.local_replica_id.to_string(),
            replication_addr: self.config.advertise_addr.clone(),
            incarnation: self.incarnation(),
        };

        let response = client.join(request).await?;
        let join_response = response.into_inner();

        if join_response.accepted {
            Ok(join_response.peers)
        } else {
            Err("Join request was rejected".into())
        }
    }

    /// Perform one round of gossip with random peers
    async fn gossip_round(&self) {
        let peers: Vec<PeerInfo> = self.get_active_peers();
        if peers.is_empty() {
            return;
        }

        // Select random peers to gossip with
        let fanout = std::cmp::min(self.config.gossip_fanout, peers.len());
        let selected = select_random(&peers, fanout);

        for peer in selected {
            if let Err(e) = self.gossip_with_peer(&peer).await {
                debug!("Gossip with {} failed: {}", peer.replica_id, e);
            }
        }
    }

    /// Exchange peer list with a specific peer
    async fn gossip_with_peer(
        &self,
        peer: &PeerInfo,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For now, we just mark the peer as alive if we can connect
        // Full gossip exchange would happen over the replication stream
        let channel = timeout(
            Duration::from_secs(2),
            Channel::from_shared(peer.replication_addr.clone())?
                .connect_timeout(Duration::from_secs(2))
                .connect(),
        )
        .await??;

        let mut _client = ReplicationServiceClient::new(channel);

        // Mark peer as alive since we connected successfully
        self.mark_alive(&peer.replica_id).await;

        Ok(())
    }

    /// Check for failed peers and update their states
    async fn check_failures(&self) {
        let now = Instant::now();
        let suspect_after = self.config.heartbeat_interval * self.config.suspect_threshold;

        let mut to_remove = Vec::new();

        for mut entry in self.peers.iter_mut() {
            let peer = entry.value_mut();

            match peer.state {
                PeerState::Alive => {
                    // Check if we should mark as suspect
                    if now.duration_since(peer.last_seen) > suspect_after {
                        peer.state = PeerState::Suspect;
                        peer.suspect_since = Some(now);

                        info!("Peer {} marked as SUSPECT", peer.replica_id);

                        if let Err(e) = self
                            .event_tx
                            .send(PeerEvent::StateChanged {
                                replica_id: peer.replica_id.clone(),
                                new_state: PeerState::Suspect,
                            })
                            .await
                        {
                            warn!("Failed to send state changed event: {}", e);
                        }
                    }
                }

                PeerState::Suspect => {
                    // Check if we should mark as dead
                    if let Some(suspect_since) = peer.suspect_since {
                        if now.duration_since(suspect_since) > self.config.suspect_timeout {
                            peer.state = PeerState::Dead;

                            info!("Peer {} marked as DEAD", peer.replica_id);

                            if let Err(e) = self
                                .event_tx
                                .send(PeerEvent::StateChanged {
                                    replica_id: peer.replica_id.clone(),
                                    new_state: PeerState::Dead,
                                })
                                .await
                            {
                                warn!("Failed to send state changed event: {}", e);
                            }
                        }
                    }
                }

                PeerState::Dead => {
                    // Check if we should remove the peer entirely
                    if let Some(suspect_since) = peer.suspect_since {
                        if now.duration_since(suspect_since) > self.config.dead_timeout {
                            to_remove.push(peer.replica_id.clone());
                        }
                    }
                }
            }
        }

        // Remove dead peers
        for replica_id in to_remove {
            self.peers.remove(&replica_id);
            info!("Peer {} removed from registry", replica_id);

            if let Err(e) = self.event_tx.send(PeerEvent::Left(replica_id)).await {
                warn!("Failed to send peer left event: {}", e);
            }
        }
    }
}

/// Select n random items from a slice
fn select_random<T: Clone>(items: &[T], n: usize) -> Vec<T> {
    if items.len() <= n {
        return items.to_vec();
    }

    // Simple random selection using time-based seed
    let seed = now_ms();
    let mut selected = Vec::with_capacity(n);
    let mut indices: Vec<usize> = (0..items.len()).collect();

    for i in 0..n {
        // Simple LCG-style random
        let idx = ((seed.wrapping_mul(1103515245).wrapping_add(12345 + i as u64)) as usize)
            % (items.len() - i);
        selected.push(items[indices[idx]].clone());
        indices.swap(idx, items.len() - 1 - i);
    }

    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_state_conversion() {
        assert_eq!(PeerState::from(1), PeerState::Alive);
        assert_eq!(PeerState::from(2), PeerState::Suspect);
        assert_eq!(PeerState::from(3), PeerState::Dead);
        assert_eq!(PeerState::from(0), PeerState::Alive); // Unknown defaults to Alive
    }

    #[test]
    fn test_select_random() {
        let items = vec![1, 2, 3, 4, 5];
        let selected = select_random(&items, 3);
        assert_eq!(selected.len(), 3);

        // All items if n >= len
        let all = select_random(&items, 10);
        assert_eq!(all.len(), 5);
    }
}
