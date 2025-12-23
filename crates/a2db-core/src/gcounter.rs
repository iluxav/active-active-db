use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;

/// Replica identifier - Arc<str> for zero-copy sharing
pub type ReplicaId = Arc<str>;

/// Key type - Arc<str> for zero-copy sharing
pub type Key = Arc<str>;

/// Maximum number of replicas stored inline (before heap allocation)
const INLINE_REPLICAS: usize = 4;

/// A PN-Counter (positive-negative counter) CRDT.
///
/// Internally stores separate P (positive/increment) and N (negative/decrement)
/// components per replica. The total value is sum(P) - sum(N).
/// Merge takes the max of each component (commutative, associative, idempotent).
///
/// This allows both increment and decrement operations while maintaining
/// CRDT properties for conflict-free replication.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GCounter {
    /// P (positive) components: increments
    p_components: SmallVec<[(ReplicaId, u64); INLINE_REPLICAS]>,
    /// N (negative) components: decrements
    n_components: SmallVec<[(ReplicaId, u64); INLINE_REPLICAS]>,
}

impl GCounter {
    /// Create a new empty PN-Counter
    pub fn new() -> Self {
        Self::default()
    }

    /// Find the index of a replica in a components list
    #[inline]
    fn find_replica(components: &[(ReplicaId, u64)], replica_id: &str) -> Option<usize> {
        components
            .iter()
            .position(|(id, _)| id.as_ref() == replica_id)
    }

    /// Increment the counter for a specific replica.
    /// Returns the new P component value for that replica.
    pub fn increment(&mut self, replica_id: &ReplicaId, amount: u64) -> u64 {
        if let Some(idx) = Self::find_replica(&self.p_components, replica_id) {
            let entry = &mut self.p_components[idx].1;
            *entry = entry.saturating_add(amount);
            *entry
        } else {
            self.p_components.push((replica_id.clone(), amount));
            amount
        }
    }

    /// Decrement the counter for a specific replica.
    /// Returns the new N component value for that replica.
    pub fn decrement(&mut self, replica_id: &ReplicaId, amount: u64) -> u64 {
        if let Some(idx) = Self::find_replica(&self.n_components, replica_id) {
            let entry = &mut self.n_components[idx].1;
            *entry = entry.saturating_add(amount);
            *entry
        } else {
            self.n_components.push((replica_id.clone(), amount));
            amount
        }
    }

    /// Get the total value (sum of P components - sum of N components)
    /// Returns signed value to handle cases where decrements exceed increments
    #[inline]
    pub fn value(&self) -> i64 {
        let p_sum: u64 = self.p_components.iter().map(|(_, v)| v).sum();
        let n_sum: u64 = self.n_components.iter().map(|(_, v)| v).sum();
        (p_sum as i64).saturating_sub(n_sum as i64)
    }

    /// Get the total value as u64 (clamped to 0 if negative)
    #[inline]
    pub fn value_u64(&self) -> u64 {
        self.value().max(0) as u64
    }

    /// Get a specific replica's P (increment) component value
    pub fn p_component(&self, replica_id: &str) -> u64 {
        Self::find_replica(&self.p_components, replica_id)
            .map(|idx| self.p_components[idx].1)
            .unwrap_or(0)
    }

    /// Get a specific replica's N (decrement) component value
    pub fn n_component(&self, replica_id: &str) -> u64 {
        Self::find_replica(&self.n_components, replica_id)
            .map(|idx| self.n_components[idx].1)
            .unwrap_or(0)
    }

    /// Get a specific replica's component value (for backwards compatibility)
    /// Returns P component only
    pub fn component(&self, replica_id: &str) -> u64 {
        self.p_component(replica_id)
    }

    /// Merge another counter into this one (max per component).
    /// Returns true if any component was updated.
    pub fn merge(&mut self, other: &GCounter) -> bool {
        let mut changed = false;

        // Merge P components
        for (replica_id, remote_value) in other.p_components.iter() {
            if let Some(idx) = Self::find_replica(&self.p_components, replica_id) {
                if *remote_value > self.p_components[idx].1 {
                    self.p_components[idx].1 = *remote_value;
                    changed = true;
                }
            } else {
                self.p_components.push((replica_id.clone(), *remote_value));
                changed = true;
            }
        }

        // Merge N components
        for (replica_id, remote_value) in other.n_components.iter() {
            if let Some(idx) = Self::find_replica(&self.n_components, replica_id) {
                if *remote_value > self.n_components[idx].1 {
                    self.n_components[idx].1 = *remote_value;
                    changed = true;
                }
            } else {
                self.n_components.push((replica_id.clone(), *remote_value));
                changed = true;
            }
        }

        changed
    }

    /// Apply a P (increment) delta.
    /// Returns true if the value was updated.
    pub fn apply_p_delta(&mut self, replica_id: &ReplicaId, value: u64) -> bool {
        if let Some(idx) = Self::find_replica(&self.p_components, replica_id) {
            if value > self.p_components[idx].1 {
                self.p_components[idx].1 = value;
                true
            } else {
                false
            }
        } else {
            self.p_components.push((replica_id.clone(), value));
            true
        }
    }

    /// Apply an N (decrement) delta.
    /// Returns true if the value was updated.
    pub fn apply_n_delta(&mut self, replica_id: &ReplicaId, value: u64) -> bool {
        if let Some(idx) = Self::find_replica(&self.n_components, replica_id) {
            if value > self.n_components[idx].1 {
                self.n_components[idx].1 = value;
                true
            } else {
                false
            }
        } else {
            self.n_components.push((replica_id.clone(), value));
            true
        }
    }

    /// Apply a single delta (backwards compatible - assumes P delta)
    pub fn apply_delta(&mut self, replica_id: &ReplicaId, value: u64) -> bool {
        self.apply_p_delta(replica_id, value)
    }

    /// Get all P components as an iterator
    pub fn p_components(&self) -> impl Iterator<Item = (&ReplicaId, &u64)> {
        self.p_components.iter().map(|(id, v)| (id, v))
    }

    /// Get all N components as an iterator
    pub fn n_components(&self) -> impl Iterator<Item = (&ReplicaId, &u64)> {
        self.n_components.iter().map(|(id, v)| (id, v))
    }

    /// Get all components (backwards compatible - returns P components)
    pub fn components(&self) -> impl Iterator<Item = (&ReplicaId, &u64)> {
        self.p_components()
    }

    /// Check if the counter has any data
    pub fn is_empty(&self) -> bool {
        self.p_components.is_empty() && self.n_components.is_empty()
    }

    /// Get the number of replicas that have contributed (P or N)
    pub fn replica_count(&self) -> usize {
        // Count unique replicas across both P and N
        let mut count = self.p_components.len();
        for (replica_id, _) in &self.n_components {
            if Self::find_replica(&self.p_components, replica_id).is_none() {
                count += 1;
            }
        }
        count
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
        assert_eq!(counter.p_component("r1"), 5);

        let new_val = counter.increment(&r1, 3);
        assert_eq!(new_val, 8);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_decrement_single_replica() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        // First increment
        counter.increment(&r1, 10);
        assert_eq!(counter.value(), 10);

        // Then decrement
        let new_val = counter.decrement(&r1, 3);
        assert_eq!(new_val, 3);
        assert_eq!(counter.value(), 7); // 10 - 3 = 7
        assert_eq!(counter.n_component("r1"), 3);

        // Decrement more
        counter.decrement(&r1, 2);
        assert_eq!(counter.value(), 5); // 10 - 5 = 5
    }

    #[test]
    fn test_decrement_below_zero() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        counter.increment(&r1, 5);
        counter.decrement(&r1, 10);

        // Value goes negative
        assert_eq!(counter.value(), -5);
        // But value_u64 clamps to 0
        assert_eq!(counter.value_u64(), 0);
    }

    #[test]
    fn test_increment_multiple_replicas() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");
        let r2 = replica("r2");

        counter.increment(&r1, 10);
        counter.increment(&r2, 7);

        assert_eq!(counter.value(), 17);
        assert_eq!(counter.p_component("r1"), 10);
        assert_eq!(counter.p_component("r2"), 7);
        assert_eq!(counter.replica_count(), 2);
    }

    #[test]
    fn test_increment_saturating() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        counter.increment(&r1, u64::MAX);
        counter.increment(&r1, 1);

        assert_eq!(counter.p_component("r1"), u64::MAX);
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
        assert_eq!(c1.p_component("r1"), 10);
        // r2 should be 7 (from c2)
        assert_eq!(c1.p_component("r2"), 7);
        // Total: 10 + 7 = 17
        assert_eq!(c1.value(), 17);
    }

    #[test]
    fn test_merge_pn_counters() {
        let mut c1 = GCounter::new();
        let mut c2 = GCounter::new();

        let r1 = replica("r1");

        c1.increment(&r1, 10);
        c1.decrement(&r1, 3);

        c2.increment(&r1, 8);
        c2.decrement(&r1, 5);

        c1.merge(&c2);

        // P: max(10, 8) = 10
        // N: max(3, 5) = 5
        // Value: 10 - 5 = 5
        assert_eq!(c1.p_component("r1"), 10);
        assert_eq!(c1.n_component("r1"), 5);
        assert_eq!(c1.value(), 5);
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
        assert_eq!(c1.p_component("r1"), 10);
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
    fn test_apply_p_delta_updates_when_larger() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        // First delta: should apply
        assert!(counter.apply_p_delta(&r1, 10));
        assert_eq!(counter.p_component("r1"), 10);

        // Same value: no change
        assert!(!counter.apply_p_delta(&r1, 10));
        assert_eq!(counter.p_component("r1"), 10);

        // Lower value: no change
        assert!(!counter.apply_p_delta(&r1, 5));
        assert_eq!(counter.p_component("r1"), 10);

        // Higher value: should apply
        assert!(counter.apply_p_delta(&r1, 15));
        assert_eq!(counter.p_component("r1"), 15);
    }

    #[test]
    fn test_apply_n_delta_updates_when_larger() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");

        // First delta: should apply
        assert!(counter.apply_n_delta(&r1, 10));
        assert_eq!(counter.n_component("r1"), 10);

        // Same value: no change
        assert!(!counter.apply_n_delta(&r1, 10));

        // Lower value: no change
        assert!(!counter.apply_n_delta(&r1, 5));

        // Higher value: should apply
        assert!(counter.apply_n_delta(&r1, 15));
        assert_eq!(counter.n_component("r1"), 15);
    }

    #[test]
    fn test_component_for_unknown_replica() {
        let counter = GCounter::new();
        assert_eq!(counter.p_component("unknown"), 0);
        assert_eq!(counter.n_component("unknown"), 0);
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

    #[test]
    fn test_replica_count_with_pn() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");
        let r2 = replica("r2");

        counter.increment(&r1, 10);
        counter.decrement(&r1, 3); // Same replica, shouldn't increase count
        counter.decrement(&r2, 5); // New replica (only in N)

        assert_eq!(counter.replica_count(), 2);
    }
}
