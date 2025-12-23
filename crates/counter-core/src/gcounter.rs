use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;

/// Replica identifier - Arc<str> for zero-copy sharing
pub type ReplicaId = Arc<str>;

/// Key type - Arc<str> for zero-copy sharing
pub type Key = Arc<str>;

/// Maximum number of replicas stored inline (before heap allocation)
const INLINE_REPLICAS: usize = 4;

/// A single G-Counter (grow-only counter) CRDT.
///
/// Internally stores (ReplicaId, u64) pairs in a SmallVec.
/// For typical deployments with 1-4 replicas, this avoids heap allocation.
/// The total value is the sum of all components.
/// Merge takes the max of each component (commutative, associative, idempotent).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GCounter {
    components: SmallVec<[(ReplicaId, u64); INLINE_REPLICAS]>,
}

impl GCounter {
    /// Create a new empty G-Counter
    pub fn new() -> Self {
        Self::default()
    }

    /// Find the index of a replica in the components list
    #[inline]
    fn find_replica(&self, replica_id: &str) -> Option<usize> {
        self.components
            .iter()
            .position(|(id, _)| id.as_ref() == replica_id)
    }

    /// Increment the counter for a specific replica.
    /// Returns the new component value for that replica.
    pub fn increment(&mut self, replica_id: &ReplicaId, amount: u64) -> u64 {
        if let Some(idx) = self.find_replica(replica_id) {
            let entry = &mut self.components[idx].1;
            *entry = entry.saturating_add(amount);
            *entry
        } else {
            self.components.push((replica_id.clone(), amount));
            amount
        }
    }

    /// Get the total value (sum of all components)
    #[inline]
    pub fn value(&self) -> u64 {
        self.components.iter().map(|(_, v)| v).sum()
    }

    /// Get a specific replica's component value
    pub fn component(&self, replica_id: &str) -> u64 {
        self.find_replica(replica_id)
            .map(|idx| self.components[idx].1)
            .unwrap_or(0)
    }

    /// Merge another counter into this one (max per component).
    /// Returns true if any component was updated.
    pub fn merge(&mut self, other: &GCounter) -> bool {
        let mut changed = false;
        for (replica_id, remote_value) in other.components.iter() {
            if let Some(idx) = self.find_replica(replica_id) {
                if *remote_value > self.components[idx].1 {
                    self.components[idx].1 = *remote_value;
                    changed = true;
                }
            } else {
                self.components.push((replica_id.clone(), *remote_value));
                changed = true;
            }
        }
        changed
    }

    /// Apply a single delta (replica_id, value).
    /// This is the idempotent merge operation.
    /// Returns true if the value was updated.
    pub fn apply_delta(&mut self, replica_id: &ReplicaId, value: u64) -> bool {
        if let Some(idx) = self.find_replica(replica_id) {
            if value > self.components[idx].1 {
                self.components[idx].1 = value;
                true
            } else {
                false
            }
        } else {
            self.components.push((replica_id.clone(), value));
            true
        }
    }

    /// Get all components as an iterator (for replication/serialization)
    pub fn components(&self) -> impl Iterator<Item = (&ReplicaId, &u64)> {
        self.components.iter().map(|(id, v)| (id, v))
    }

    /// Check if the counter has any data
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// Get the number of replicas that have contributed to this counter
    pub fn replica_count(&self) -> usize {
        self.components.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica(s: &str) -> ReplicaId {
        Arc::from(s)
    }

    #[test]
    fn test_new_counter_is_zero() {
        let counter = GCounter::new();
        assert_eq!(counter.value(), 0);
        assert!(counter.is_empty());
    }

    #[test]
    fn test_increment_single_replica() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        let new_val = counter.increment(&r1, 5);
        assert_eq!(new_val, 5);
        assert_eq!(counter.value(), 5);
        assert_eq!(counter.component("r1"), 5);

        let new_val = counter.increment(&r1, 3);
        assert_eq!(new_val, 8);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_increment_multiple_replicas() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");
        let r2 = replica("r2");

        counter.increment(&r1, 10);
        counter.increment(&r2, 7);

        assert_eq!(counter.value(), 17);
        assert_eq!(counter.component("r1"), 10);
        assert_eq!(counter.component("r2"), 7);
        assert_eq!(counter.replica_count(), 2);
    }

    #[test]
    fn test_increment_saturating() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        counter.increment(&r1, u64::MAX);
        counter.increment(&r1, 1);

        assert_eq!(counter.component("r1"), u64::MAX);
    }

    #[test]
    fn test_merge_takes_max() {
        let mut c1 = GCounter::new();
        let mut c2 = GCounter::new();

        let r1 = replica("r1");
        let r2 = replica("r2");

        c1.increment(&r1, 10);
        c2.increment(&r1, 5);
        c2.increment(&r2, 7);

        let changed = c1.merge(&c2);
        assert!(changed);

        // r1 should be max(10, 5) = 10
        assert_eq!(c1.component("r1"), 10);
        // r2 should be 7 (from c2)
        assert_eq!(c1.component("r2"), 7);
        // Total: 10 + 7 = 17
        assert_eq!(c1.value(), 17);
    }

    #[test]
    fn test_merge_no_change() {
        let mut c1 = GCounter::new();
        let mut c2 = GCounter::new();

        let r1 = replica("r1");

        c1.increment(&r1, 10);
        c2.increment(&r1, 5);

        let changed = c1.merge(&c2);
        assert!(!changed);
        assert_eq!(c1.component("r1"), 10);
    }

    #[test]
    fn test_merge_is_commutative() {
        let mut c1 = GCounter::new();
        let mut c2 = GCounter::new();

        let r1 = replica("r1");
        let r2 = replica("r2");

        c1.increment(&r1, 10);
        c1.increment(&r2, 3);
        c2.increment(&r1, 5);
        c2.increment(&r2, 7);

        let mut c1_then_c2 = c1.clone();
        c1_then_c2.merge(&c2);

        let mut c2_then_c1 = c2.clone();
        c2_then_c1.merge(&c1);

        assert_eq!(c1_then_c2.value(), c2_then_c1.value());
    }

    #[test]
    fn test_merge_is_idempotent() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");
        let r2 = replica("r2");

        counter.increment(&r1, 10);
        counter.increment(&r2, 7);

        let before = counter.value();
        let clone = counter.clone();
        let changed = counter.merge(&clone);

        assert!(!changed);
        assert_eq!(counter.value(), before);
    }

    #[test]
    fn test_apply_delta_updates_when_larger() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        // First delta: should apply
        assert!(counter.apply_delta(&r1, 10));
        assert_eq!(counter.component("r1"), 10);

        // Same value: no change
        assert!(!counter.apply_delta(&r1, 10));
        assert_eq!(counter.component("r1"), 10);

        // Lower value: no change
        assert!(!counter.apply_delta(&r1, 5));
        assert_eq!(counter.component("r1"), 10);

        // Higher value: should apply
        assert!(counter.apply_delta(&r1, 15));
        assert_eq!(counter.component("r1"), 15);
    }

    #[test]
    fn test_component_for_unknown_replica() {
        let counter = GCounter::new();
        assert_eq!(counter.component("unknown"), 0);
    }

    #[test]
    fn test_inline_storage() {
        let mut counter = GCounter::new();

        // Add up to INLINE_REPLICAS (4) - should stay inline
        for i in 0..4 {
            let r = replica(&format!("r{}", i));
            counter.increment(&r, 1);
        }

        assert_eq!(counter.replica_count(), 4);
        assert_eq!(counter.value(), 4);
    }
}
