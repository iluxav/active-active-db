use crate::delta::Delta;
use crate::gcounter::{GCounter, Key, ReplicaId};
use crate::snapshot::Snapshot;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Entry in the counter store, containing the counter and optional expiration.
#[derive(Debug, Clone, Default)]
pub struct CounterEntry {
    /// The G-Counter CRDT
    pub counter: GCounter,
    /// Optional expiration timestamp in milliseconds since Unix epoch.
    /// None means the key never expires.
    pub expires_at_ms: Option<u64>,
}

impl CounterEntry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_expiration(counter: GCounter, expires_at_ms: Option<u64>) -> Self {
        Self { counter, expires_at_ms }
    }
}

/// Thread-safe in-memory counter store using lock-free DashMap.
///
/// DashMap provides fine-grained locking (sharded internally) which allows
/// concurrent reads and writes to different keys without contention.
/// This is significantly faster than RwLock<HashMap> for workloads with
/// many concurrent writers to different keys.
pub struct CounterStore {
    /// The local replica's ID - stable across restarts
    local_replica_id: ReplicaId,
    /// Map of key -> CounterEntry, using DashMap for concurrent access
    counters: DashMap<Key, CounterEntry>,
}

/// Get current time in milliseconds since Unix epoch
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl CounterStore {
    /// Create a new counter store for the given replica
    pub fn new(local_replica_id: ReplicaId) -> Self {
        Self {
            local_replica_id,
            counters: DashMap::new(),
        }
    }

    /// Create from a string slice (convenience method)
    pub fn from_str(local_replica_id: &str) -> Self {
        Self::new(Arc::from(local_replica_id))
    }

    /// Get the local replica's ID
    pub fn local_replica_id(&self) -> &ReplicaId {
        &self.local_replica_id
    }

    /// Check if an entry is expired
    #[inline]
    fn is_expired(entry: &CounterEntry) -> bool {
        match entry.expires_at_ms {
            Some(exp) => current_time_ms() >= exp,
            None => false,
        }
    }

    /// Increment a counter by amount.
    /// Returns (new total value, delta to replicate).
    ///
    /// The delta contains the new component value for this replica,
    /// which should be broadcast to other replicas.
    /// Note: Incrementing does NOT affect an existing TTL.
    pub fn increment(&self, key: &Key, amount: u64) -> (u64, Delta) {
        let mut entry = self.counters.entry(key.clone()).or_default();
        let counter_entry = entry.value_mut();
        let new_component = counter_entry.counter.increment(&self.local_replica_id, amount);
        let total = counter_entry.counter.value();

        // Include expiration in delta for replication
        let delta = Delta::with_expiration(
            key.clone(),
            self.local_replica_id.clone(),
            new_component,
            counter_entry.expires_at_ms,
        );

        (total, delta)
    }

    /// Increment using a string slice key (convenience method)
    pub fn increment_str(&self, key: &str, amount: u64) -> (u64, Delta) {
        let key: Key = Arc::from(key);
        self.increment(&key, amount)
    }

    /// Get a single counter's value.
    /// Returns 0 for keys that don't exist or are expired.
    pub fn get(&self, key: &str) -> u64 {
        self.counters
            .get(key)
            .and_then(|entry| {
                if Self::is_expired(entry.value()) {
                    None
                } else {
                    Some(entry.value().counter.value())
                }
            })
            .unwrap_or(0)
    }

    /// Get multiple counter values.
    /// Returns values in the same order as requested keys.
    /// Non-existent or expired keys return 0.
    pub fn mget(&self, keys: &[String]) -> Vec<u64> {
        keys.iter()
            .map(|key| self.get(key))
            .collect()
    }

    /// Apply a delta from replication.
    /// Returns true if the state changed (delta was newer or expiration extended).
    ///
    /// This operation is idempotent: applying the same delta multiple
    /// times has no effect after the first application.
    /// TTL merge uses MAX semantics: the later expiration always wins.
    pub fn apply_delta(&self, delta: &Delta) -> bool {
        let mut entry = self.counters.entry(delta.key.clone()).or_default();
        let counter_entry = entry.value_mut();

        // Apply counter component (existing CRDT logic)
        let counter_changed = counter_entry.counter.apply_delta(&delta.origin_replica_id, delta.component_value);

        // Apply expiration with MAX semantics (always take the later expiration)
        let expiry_changed = match (counter_entry.expires_at_ms, delta.expires_at_ms) {
            (None, Some(t)) => {
                counter_entry.expires_at_ms = Some(t);
                true
            }
            (Some(a), Some(b)) if b > a => {
                counter_entry.expires_at_ms = Some(b);
                true
            }
            _ => false,
        };

        counter_changed || expiry_changed
    }

    /// Apply multiple deltas from replication.
    /// Returns list of deltas that caused changes (for further propagation).
    pub fn apply_deltas(&self, deltas: &[Delta]) -> Vec<Delta> {
        let mut changed = Vec::new();

        for delta in deltas {
            if self.apply_delta(delta) {
                changed.push(delta.clone());
            }
        }

        changed
    }

    /// Get all deltas representing full state (for anti-entropy sync).
    /// This is used when a new replica joins or after a network partition
    /// to ensure eventual convergence. Excludes expired keys.
    pub fn all_deltas(&self) -> Vec<Delta> {
        let mut deltas = Vec::new();

        for entry in self.counters.iter() {
            let counter_entry = entry.value();
            // Skip expired entries
            if Self::is_expired(counter_entry) {
                continue;
            }
            let key = entry.key();
            for (replica_id, &value) in counter_entry.counter.components() {
                deltas.push(Delta::with_expiration(
                    key.clone(),
                    replica_id.clone(),
                    value,
                    counter_entry.expires_at_ms,
                ));
            }
        }

        deltas
    }

    /// Get the number of keys in the store (includes expired keys not yet cleaned up)
    pub fn key_count(&self) -> usize {
        self.counters.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Get all keys (for debugging/admin purposes, excludes expired keys)
    pub fn keys(&self) -> Vec<Key> {
        self.counters
            .iter()
            .filter(|entry| !Self::is_expired(entry.value()))
            .map(|entry| entry.key().clone())
            .collect()
    }

    // === TTL Operations ===

    /// Set expiration time using absolute Unix timestamp in milliseconds.
    /// Returns true if the key exists and expiration was set.
    pub fn expire_at(&self, key: &str, expires_at_ms: u64) -> bool {
        if let Some(mut entry) = self.counters.get_mut(key) {
            entry.value_mut().expires_at_ms = Some(expires_at_ms);
            true
        } else {
            false
        }
    }

    /// Set expiration using relative TTL in milliseconds.
    /// Returns true if the key exists and expiration was set.
    pub fn expire(&self, key: &str, ttl_ms: u64) -> bool {
        let expires_at = current_time_ms() + ttl_ms;
        self.expire_at(key, expires_at)
    }

    /// Remove expiration from a key (make it persistent).
    /// Returns true if the key exists and had an expiration that was removed.
    pub fn persist(&self, key: &str) -> bool {
        if let Some(mut entry) = self.counters.get_mut(key) {
            if entry.value().expires_at_ms.is_some() {
                entry.value_mut().expires_at_ms = None;
                return true;
            }
        }
        false
    }

    /// Get remaining TTL in milliseconds.
    /// Returns: remaining ms (positive), -1 (no TTL), or -2 (key not found/expired)
    pub fn pttl(&self, key: &str) -> i64 {
        match self.counters.get(key) {
            Some(entry) => {
                let counter_entry = entry.value();
                if Self::is_expired(counter_entry) {
                    -2 // Expired, treat as not found
                } else {
                    match counter_entry.expires_at_ms {
                        Some(exp) => {
                            let now = current_time_ms();
                            if exp > now {
                                (exp - now) as i64
                            } else {
                                -2 // Just expired
                            }
                        }
                        None => -1, // No TTL set
                    }
                }
            }
            None => -2, // Key not found
        }
    }

    /// Get remaining TTL in seconds.
    pub fn ttl(&self, key: &str) -> i64 {
        let pttl = self.pttl(key);
        if pttl > 0 {
            (pttl / 1000) as i64
        } else {
            pttl
        }
    }

    /// Cleanup expired keys.
    /// Returns the number of keys removed.
    /// Uses grace_ms to add a buffer before actually deleting (handles clock skew).
    pub fn cleanup_expired(&self, batch_size: usize, grace_ms: u64) -> usize {
        let now = current_time_ms();
        let threshold = now.saturating_sub(grace_ms);

        let mut removed = 0;
        let mut keys_to_remove = Vec::with_capacity(batch_size);

        // Collect keys to remove (can't remove while iterating)
        for entry in self.counters.iter() {
            if keys_to_remove.len() >= batch_size {
                break;
            }
            if let Some(exp) = entry.value().expires_at_ms {
                // Only remove if expired AND past grace period
                if exp <= threshold {
                    keys_to_remove.push(entry.key().clone());
                }
            }
        }

        // Remove collected keys
        for key in keys_to_remove {
            if self.counters.remove(&key).is_some() {
                removed += 1;
            }
        }

        removed
    }

    /// Export the current state as a snapshot for persistence.
    /// Excludes expired keys from the snapshot.
    pub fn export_snapshot(&self) -> Snapshot {
        let mut snapshot_counters = HashMap::new();
        let mut expirations = HashMap::new();

        for entry in self.counters.iter() {
            let counter_entry = entry.value();
            // Skip expired entries
            if Self::is_expired(counter_entry) {
                continue;
            }
            let key = entry.key();
            snapshot_counters.insert(key.to_string(), Snapshot::gcounter_to_map(&counter_entry.counter));
            if let Some(exp) = counter_entry.expires_at_ms {
                expirations.insert(key.to_string(), exp);
            }
        }

        Snapshot::from_counters_with_expirations(
            self.local_replica_id.to_string(),
            snapshot_counters,
            expirations,
        )
    }

    /// Import a snapshot, merging with existing state.
    /// Uses CRDT merge semantics (max per component), so this is idempotent.
    /// TTL uses MAX semantics (extends, never shortens).
    pub fn import_snapshot(&self, snapshot: &Snapshot) {
        for (key, components) in &snapshot.counters {
            let key_arc: Key = Arc::from(key.as_str());
            let mut entry = self.counters.entry(key_arc).or_default();
            let counter_entry = entry.value_mut();

            // Apply each component using CRDT merge (max)
            for (replica_id, &value) in components {
                let replica_id: ReplicaId = Arc::from(replica_id.as_str());
                counter_entry.counter.apply_delta(&replica_id, value);
            }

            // Apply expiration with MAX semantics
            if let Some(&exp) = snapshot.expirations.get(key) {
                match counter_entry.expires_at_ms {
                    None => counter_entry.expires_at_ms = Some(exp),
                    Some(existing) if exp > existing => counter_entry.expires_at_ms = Some(exp),
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Key {
        Arc::from(s)
    }

    #[test]
    fn test_new_store() {
        let store = CounterStore::from_str("replica-1");
        assert_eq!(store.local_replica_id().as_ref(), "replica-1");
        assert!(store.is_empty());
    }

    #[test]
    fn test_increment_and_get() {
        let store = CounterStore::from_str("r1");
        let k = key("counter:foo");

        let (value, delta) = store.increment(&k, 5);
        assert_eq!(value, 5);
        assert_eq!(delta.key.as_ref(), "counter:foo");
        assert_eq!(delta.origin_replica_id.as_ref(), "r1");
        assert_eq!(delta.component_value, 5);

        assert_eq!(store.get("counter:foo"), 5);

        let (value, delta) = store.increment(&k, 3);
        assert_eq!(value, 8);
        assert_eq!(delta.component_value, 8);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let store = CounterStore::from_str("r1");
        assert_eq!(store.get("nonexistent"), 0);
    }

    #[test]
    fn test_mget() {
        let store = CounterStore::from_str("r1");

        store.increment_str("k1", 10);
        store.increment_str("k2", 20);
        store.increment_str("k3", 30);

        let values = store.mget(&["k1".into(), "k2".into(), "k4".into(), "k3".into()]);
        assert_eq!(values, vec![10, 20, 0, 30]);
    }

    #[test]
    fn test_apply_delta() {
        let store = CounterStore::from_str("r1");

        // Apply delta from another replica
        let delta = Delta::from_strs("counter:foo", "r2", 100);
        assert!(store.apply_delta(&delta));
        assert_eq!(store.get("counter:foo"), 100);

        // Apply same delta again - idempotent
        assert!(!store.apply_delta(&delta));
        assert_eq!(store.get("counter:foo"), 100);

        // Apply lower value - no change
        let lower_delta = Delta::from_strs("counter:foo", "r2", 50);
        assert!(!store.apply_delta(&lower_delta));
        assert_eq!(store.get("counter:foo"), 100);

        // Apply higher value - should update
        let higher_delta = Delta::from_strs("counter:foo", "r2", 150);
        assert!(store.apply_delta(&higher_delta));
        assert_eq!(store.get("counter:foo"), 150);
    }

    #[test]
    fn test_apply_deltas_batch() {
        let store = CounterStore::from_str("r1");

        let deltas = vec![
            Delta::from_strs("k1", "r2", 10),
            Delta::from_strs("k1", "r3", 20),
            Delta::from_strs("k2", "r2", 30),
            Delta::from_strs("k1", "r2", 5), // Lower than existing, should not change
        ];

        let changed = store.apply_deltas(&deltas);

        // Only first 3 should have caused changes
        assert_eq!(changed.len(), 3);

        // k1 = 10 (r2) + 20 (r3) = 30
        assert_eq!(store.get("k1"), 30);
        // k2 = 30 (r2)
        assert_eq!(store.get("k2"), 30);
    }

    #[test]
    fn test_convergence_two_replicas() {
        let store1 = CounterStore::from_str("r1");
        let store2 = CounterStore::from_str("r2");

        // Both replicas increment the same key
        let k = key("shared");
        let (_, delta1) = store1.increment(&k, 10);
        let (_, delta2) = store2.increment(&k, 7);

        // Exchange deltas
        store1.apply_delta(&delta2);
        store2.apply_delta(&delta1);

        // Both should see the same value
        assert_eq!(store1.get("shared"), 17);
        assert_eq!(store2.get("shared"), 17);
    }

    #[test]
    fn test_all_deltas() {
        let store = CounterStore::from_str("r1");

        store.increment_str("k1", 10);
        store.increment_str("k2", 20);

        // Apply delta from another replica
        store.apply_delta(&Delta::from_strs("k1", "r2", 5));

        let deltas = store.all_deltas();

        // Should have 3 deltas: k1/r1, k1/r2, k2/r1
        assert_eq!(deltas.len(), 3);
    }

    #[test]
    fn test_key_count_and_keys() {
        let store = CounterStore::from_str("r1");

        assert_eq!(store.key_count(), 0);
        assert!(store.keys().is_empty());

        store.increment_str("k1", 1);
        store.increment_str("k2", 1);
        store.increment_str("k3", 1);

        assert_eq!(store.key_count(), 3);

        let mut keys: Vec<String> = store.keys().iter().map(|k| k.to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2", "k3"]);
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let store = StdArc::new(CounterStore::from_str("r1"));

        // Pre-populate
        store.increment_str("counter", 1000);

        // Spawn multiple reader threads
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let store = StdArc::clone(&store);
                thread::spawn(move || {
                    for _ in 0..100 {
                        let _ = store.get("counter");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.get("counter"), 1000);
    }

    #[test]
    fn test_concurrent_writes() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let store = StdArc::new(CounterStore::from_str("r1"));

        // Spawn multiple writer threads
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let store = StdArc::clone(&store);
                let key = key(&format!("counter:{}", i));
                thread::spawn(move || {
                    for _ in 0..100 {
                        store.increment(&key, 1);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Each counter should be 100
        for i in 0..10 {
            assert_eq!(store.get(&format!("counter:{}", i)), 100);
        }
    }

    #[test]
    fn test_export_snapshot() {
        let store = CounterStore::from_str("r1");

        store.increment_str("k1", 10);
        store.increment_str("k2", 20);
        store.apply_delta(&Delta::from_strs("k1", "r2", 5));

        let snapshot = store.export_snapshot();

        assert_eq!(snapshot.replica_id, "r1");
        assert_eq!(snapshot.key_count(), 2);

        let k1 = snapshot.counters.get("k1").unwrap();
        assert_eq!(k1.get("r1"), Some(&10));
        assert_eq!(k1.get("r2"), Some(&5));

        let k2 = snapshot.counters.get("k2").unwrap();
        assert_eq!(k2.get("r1"), Some(&20));
    }

    #[test]
    fn test_import_snapshot() {
        let store = CounterStore::from_str("r1");

        // Create a snapshot with some data
        let mut counters = HashMap::new();
        let mut k1 = HashMap::new();
        k1.insert("r1".to_string(), 100u64);
        k1.insert("r2".to_string(), 50u64);
        counters.insert("api:requests".to_string(), k1);

        let snapshot = Snapshot::from_counters("r2".to_string(), counters);

        // Import it
        store.import_snapshot(&snapshot);

        // Verify
        assert_eq!(store.get("api:requests"), 150); // 100 + 50
    }

    #[test]
    fn test_import_snapshot_merges() {
        let store = CounterStore::from_str("r1");

        // Pre-existing data
        store.increment_str("k1", 100);

        // Snapshot with older data for same key
        let mut counters = HashMap::new();
        let mut k1 = HashMap::new();
        k1.insert("r1".to_string(), 50u64); // Lower than existing
        k1.insert("r2".to_string(), 75u64); // New replica
        counters.insert("k1".to_string(), k1);

        let snapshot = Snapshot::from_counters("r2".to_string(), counters);

        // Import it
        store.import_snapshot(&snapshot);

        // r1 should still be 100 (max), r2 should be 75
        assert_eq!(store.get("k1"), 175); // 100 + 75
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let store1 = CounterStore::from_str("r1");

        store1.increment_str("counter:a", 100);
        store1.increment_str("counter:b", 200);
        store1.apply_delta(&Delta::from_strs("counter:a", "r2", 50));

        // Export
        let snapshot = store1.export_snapshot();
        let json = snapshot.to_json().unwrap();

        // Import into new store
        let store2 = CounterStore::from_str("r2");
        let restored = Snapshot::from_json(&json).unwrap();
        store2.import_snapshot(&restored);

        // Values should match
        assert_eq!(store2.get("counter:a"), store1.get("counter:a"));
        assert_eq!(store2.get("counter:b"), store1.get("counter:b"));
    }

    // === TTL Tests ===

    #[test]
    fn test_expire_and_ttl() {
        let store = CounterStore::from_str("r1");

        // Create a key
        store.increment_str("k1", 10);

        // No TTL initially
        assert_eq!(store.ttl("k1"), -1);
        assert_eq!(store.pttl("k1"), -1);

        // Set TTL (10 seconds)
        assert!(store.expire("k1", 10000));

        // TTL should be positive
        let pttl = store.pttl("k1");
        assert!(pttl > 0 && pttl <= 10000);

        // Non-existent key
        assert_eq!(store.ttl("nonexistent"), -2);
        assert!(!store.expire("nonexistent", 10000));
    }

    #[test]
    fn test_expire_at() {
        let store = CounterStore::from_str("r1");
        store.increment_str("k1", 10);

        // Set absolute expiration 1 second in the future
        let future_ms = current_time_ms() + 1000;
        assert!(store.expire_at("k1", future_ms));

        let pttl = store.pttl("k1");
        assert!(pttl > 0 && pttl <= 1000);
    }

    #[test]
    fn test_persist() {
        let store = CounterStore::from_str("r1");
        store.increment_str("k1", 10);

        // Set TTL
        store.expire("k1", 10000);
        assert!(store.pttl("k1") > 0);

        // Persist (remove TTL)
        assert!(store.persist("k1"));
        assert_eq!(store.pttl("k1"), -1);

        // Persist again - no change
        assert!(!store.persist("k1"));
    }

    #[test]
    fn test_expired_key_returns_zero() {
        let store = CounterStore::from_str("r1");
        store.increment_str("k1", 10);

        // Set expiration in the past
        let past_ms = current_time_ms().saturating_sub(1000);
        store.expire_at("k1", past_ms);

        // Key should appear as non-existent
        assert_eq!(store.get("k1"), 0);
        assert_eq!(store.pttl("k1"), -2);
    }

    #[test]
    fn test_ttl_merge_takes_max() {
        let store = CounterStore::from_str("r1");
        store.increment_str("k1", 10);

        let now = current_time_ms();
        let exp1 = now + 60000; // 60 seconds
        let exp2 = now + 30000; // 30 seconds

        // Set initial expiration
        store.expire_at("k1", exp1);

        // Apply delta with shorter expiration - should not change
        let delta = Delta::from_strs_with_expiration("k1", "r2", 5, Some(exp2));
        store.apply_delta(&delta);

        // TTL should still be around 60 seconds
        let pttl = store.pttl("k1");
        assert!(pttl > 50000 && pttl <= 60000);

        // Apply delta with longer expiration - should extend
        let exp3 = now + 120000; // 120 seconds
        let delta = Delta::from_strs_with_expiration("k1", "r2", 6, Some(exp3));
        store.apply_delta(&delta);

        let pttl = store.pttl("k1");
        assert!(pttl > 110000 && pttl <= 120000);
    }

    #[test]
    fn test_cleanup_expired() {
        let store = CounterStore::from_str("r1");

        // Create some keys with past expiration
        store.increment_str("k1", 10);
        store.increment_str("k2", 20);
        store.increment_str("k3", 30); // No TTL

        let past = current_time_ms().saturating_sub(10000); // 10 seconds ago
        store.expire_at("k1", past);
        store.expire_at("k2", past);

        // Key count before cleanup (includes expired)
        assert_eq!(store.key_count(), 3);

        // Cleanup with 0 grace period
        let removed = store.cleanup_expired(100, 0);
        assert_eq!(removed, 2);

        // Only k3 should remain
        assert_eq!(store.key_count(), 1);
        assert_eq!(store.get("k3"), 30);
    }

    #[test]
    fn test_keys_excludes_expired() {
        let store = CounterStore::from_str("r1");

        store.increment_str("k1", 10);
        store.increment_str("k2", 20);

        let past = current_time_ms().saturating_sub(1000);
        store.expire_at("k1", past);

        // keys() should exclude expired
        let keys = store.keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref(), "k2");
    }

    #[test]
    fn test_all_deltas_excludes_expired() {
        let store = CounterStore::from_str("r1");

        store.increment_str("k1", 10);
        store.increment_str("k2", 20);

        let past = current_time_ms().saturating_sub(1000);
        store.expire_at("k1", past);

        // all_deltas() should exclude expired
        let deltas = store.all_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].key.as_ref(), "k2");
    }

    #[test]
    fn test_increment_preserves_ttl() {
        let store = CounterStore::from_str("r1");

        store.increment_str("k1", 10);
        store.expire("k1", 60000); // 60 seconds

        let pttl_before = store.pttl("k1");

        // Increment doesn't change TTL
        store.increment_str("k1", 5);

        let pttl_after = store.pttl("k1");
        assert!(pttl_after <= pttl_before); // Should be same or slightly less
        assert!(pttl_after > 59000); // Still around 60 seconds
    }
}
