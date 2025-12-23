use crate::gcounter::{Key, ReplicaId};
use std::collections::HashMap;
use std::sync::Arc;

/// A single delta representing one component update.
/// This is the unit of replication between replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    /// The counter key
    pub key: Key,
    /// The replica that originated this increment
    pub origin_replica_id: ReplicaId,
    /// The current component value for this replica
    pub component_value: u64,
    /// Optional expiration timestamp in milliseconds since Unix epoch.
    /// None means no expiration. Uses MAX merge semantics across replicas.
    pub expires_at_ms: Option<u64>,
}

impl Delta {
    pub fn new(key: Key, origin_replica_id: ReplicaId, component_value: u64) -> Self {
        Self {
            key,
            origin_replica_id,
            component_value,
            expires_at_ms: None,
        }
    }

    /// Create a delta with expiration
    pub fn with_expiration(
        key: Key,
        origin_replica_id: ReplicaId,
        component_value: u64,
        expires_at_ms: Option<u64>,
    ) -> Self {
        Self {
            key,
            origin_replica_id,
            component_value,
            expires_at_ms,
        }
    }

    /// Create a delta from string slices (convenience method)
    pub fn from_strs(key: &str, origin_replica_id: &str, component_value: u64) -> Self {
        Self {
            key: Arc::from(key),
            origin_replica_id: Arc::from(origin_replica_id),
            component_value,
            expires_at_ms: None,
        }
    }

    /// Create a delta from string slices with expiration
    pub fn from_strs_with_expiration(
        key: &str,
        origin_replica_id: &str,
        component_value: u64,
        expires_at_ms: Option<u64>,
    ) -> Self {
        Self {
            key: Arc::from(key),
            origin_replica_id: Arc::from(origin_replica_id),
            component_value,
            expires_at_ms,
        }
    }
}

/// Compacted entry storing both component value and expiration.
#[derive(Debug, Clone)]
struct CompactedEntry {
    component_value: u64,
    expires_at_ms: Option<u64>,
}

/// Delta buffer for compaction before sending.
/// Keeps only the latest (highest) value for each (key, replica_id) pair.
/// For expiration, uses MAX semantics (always extend, never shorten).
///
/// This is critical for efficiency: if a key is incremented 1000 times/second,
/// we only need to send the final value, not all intermediate values.
#[derive(Debug, Default)]
pub struct DeltaCompactor {
    /// Map: (key, replica_id) -> compacted entry (value + expiration)
    pending: HashMap<(Key, ReplicaId), CompactedEntry>,
}

impl DeltaCompactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a delta, compacting with existing if present.
    /// Takes MAX of component_value and MAX of expires_at_ms.
    pub fn add(&mut self, delta: Delta) {
        let key = (delta.key, delta.origin_replica_id);
        self.pending
            .entry(key)
            .and_modify(|entry| {
                // Take max of component values
                entry.component_value = entry.component_value.max(delta.component_value);
                // Take max of expirations (extend, never shorten)
                entry.expires_at_ms = match (entry.expires_at_ms, delta.expires_at_ms) {
                    (None, exp) => exp,
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (exp, None) => exp,
                };
            })
            .or_insert(CompactedEntry {
                component_value: delta.component_value,
                expires_at_ms: delta.expires_at_ms,
            });
    }

    /// Add multiple deltas
    pub fn add_all(&mut self, deltas: impl IntoIterator<Item = Delta>) {
        for delta in deltas {
            self.add(delta);
        }
    }

    /// Drain all compacted deltas, clearing the buffer.
    /// Returns deltas in arbitrary order.
    pub fn drain(&mut self) -> Vec<Delta> {
        self.pending
            .drain()
            .map(|((key, replica_id), entry)| {
                Delta::with_expiration(key, replica_id, entry.component_value, entry.expires_at_ms)
            })
            .collect()
    }

    /// Peek at all pending deltas without consuming them
    pub fn peek(&self) -> Vec<Delta> {
        self.pending
            .iter()
            .map(|((key, replica_id), entry)| {
                Delta::with_expiration(
                    key.clone(),
                    replica_id.clone(),
                    entry.component_value,
                    entry.expires_at_ms,
                )
            })
            .collect()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get the number of unique (key, replica_id) pairs
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Clear all pending deltas
    pub fn clear(&mut self) {
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Key {
        Arc::from(s)
    }

    fn replica(s: &str) -> ReplicaId {
        Arc::from(s)
    }

    #[test]
    fn test_delta_creation() {
        let delta = Delta::new(key("key1"), replica("r1"), 42);
        assert_eq!(delta.key.as_ref(), "key1");
        assert_eq!(delta.origin_replica_id.as_ref(), "r1");
        assert_eq!(delta.component_value, 42);
    }

    #[test]
    fn test_delta_from_strs() {
        let delta = Delta::from_strs("key1", "r1", 42);
        assert_eq!(delta.key.as_ref(), "key1");
        assert_eq!(delta.origin_replica_id.as_ref(), "r1");
        assert_eq!(delta.component_value, 42);
    }

    #[test]
    fn test_compactor_single_delta() {
        let mut compactor = DeltaCompactor::new();
        compactor.add(Delta::new(key("key1"), replica("r1"), 10));

        assert!(!compactor.is_empty());
        assert_eq!(compactor.len(), 1);

        let deltas = compactor.drain();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].component_value, 10);
        assert!(compactor.is_empty());
    }

    #[test]
    fn test_compactor_takes_max_for_same_key_replica() {
        let mut compactor = DeltaCompactor::new();

        compactor.add(Delta::new(key("key1"), replica("r1"), 5));
        compactor.add(Delta::new(key("key1"), replica("r1"), 10));
        compactor.add(Delta::new(key("key1"), replica("r1"), 7)); // Lower, should be ignored

        assert_eq!(compactor.len(), 1);

        let deltas = compactor.drain();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].component_value, 10);
    }

    #[test]
    fn test_compactor_different_keys() {
        let mut compactor = DeltaCompactor::new();

        compactor.add(Delta::new(key("key1"), replica("r1"), 10));
        compactor.add(Delta::new(key("key2"), replica("r1"), 20));

        assert_eq!(compactor.len(), 2);

        let deltas = compactor.drain();
        assert_eq!(deltas.len(), 2);
    }

    #[test]
    fn test_compactor_different_replicas() {
        let mut compactor = DeltaCompactor::new();

        compactor.add(Delta::new(key("key1"), replica("r1"), 10));
        compactor.add(Delta::new(key("key1"), replica("r2"), 20));

        assert_eq!(compactor.len(), 2);

        let deltas = compactor.drain();
        assert_eq!(deltas.len(), 2);
    }

    #[test]
    fn test_compactor_complex_scenario() {
        let mut compactor = DeltaCompactor::new();

        // Multiple updates to same (key, replica)
        compactor.add(Delta::new(key("counter:foo"), replica("us-west"), 100));
        compactor.add(Delta::new(key("counter:foo"), replica("us-west"), 150));
        compactor.add(Delta::new(key("counter:foo"), replica("us-west"), 120)); // Lower

        // Different key, same replica
        compactor.add(Delta::new(key("counter:bar"), replica("us-west"), 50));

        // Same key, different replica
        compactor.add(Delta::new(key("counter:foo"), replica("eu-west"), 75));

        assert_eq!(compactor.len(), 3);

        let deltas = compactor.drain();
        assert_eq!(deltas.len(), 3);

        // Find specific deltas
        let foo_us_west = deltas
            .iter()
            .find(|d| d.key.as_ref() == "counter:foo" && d.origin_replica_id.as_ref() == "us-west")
            .unwrap();
        assert_eq!(foo_us_west.component_value, 150);

        let bar_us_west = deltas
            .iter()
            .find(|d| d.key.as_ref() == "counter:bar" && d.origin_replica_id.as_ref() == "us-west")
            .unwrap();
        assert_eq!(bar_us_west.component_value, 50);

        let foo_eu_west = deltas
            .iter()
            .find(|d| d.key.as_ref() == "counter:foo" && d.origin_replica_id.as_ref() == "eu-west")
            .unwrap();
        assert_eq!(foo_eu_west.component_value, 75);
    }

    #[test]
    fn test_compactor_add_all() {
        let mut compactor = DeltaCompactor::new();

        let deltas = vec![
            Delta::new(key("k1"), replica("r1"), 10),
            Delta::new(key("k1"), replica("r1"), 20),
            Delta::new(key("k2"), replica("r1"), 30),
        ];

        compactor.add_all(deltas);

        assert_eq!(compactor.len(), 2);
    }

    #[test]
    fn test_compactor_peek() {
        let mut compactor = DeltaCompactor::new();
        compactor.add(Delta::new(key("k1"), replica("r1"), 10));

        let peeked = compactor.peek();
        assert_eq!(peeked.len(), 1);
        assert!(!compactor.is_empty()); // peek doesn't consume

        let drained = compactor.drain();
        assert_eq!(drained.len(), 1);
        assert!(compactor.is_empty()); // drain consumes
    }

    #[test]
    fn test_compactor_clear() {
        let mut compactor = DeltaCompactor::new();
        compactor.add(Delta::new(key("k1"), replica("r1"), 10));
        compactor.add(Delta::new(key("k2"), replica("r1"), 20));

        assert_eq!(compactor.len(), 2);

        compactor.clear();

        assert!(compactor.is_empty());
        assert_eq!(compactor.len(), 0);
    }
}
