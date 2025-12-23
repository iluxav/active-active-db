use crate::gcounter::ReplicaId;
use serde::{Deserialize, Serialize};

/// A Last-Writer-Wins Register CRDT for string values.
///
/// Uses (timestamp, replica_id) as the ordering key.
/// Higher timestamp wins; on tie, lexicographically higher replica_id wins.
/// This ensures deterministic conflict resolution across all replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister {
    /// The current value
    value: String,
    /// Timestamp when the value was set (milliseconds since epoch)
    timestamp_ms: u64,
    /// Replica that set the value (for tie-breaking)
    origin_replica: ReplicaId,
}

impl Default for LWWRegister {
    fn default() -> Self {
        Self {
            value: String::new(),
            timestamp_ms: 0,
            origin_replica: "".into(),
        }
    }
}

impl LWWRegister {
    /// Create a new LWW Register with an initial value
    pub fn new(value: String, timestamp_ms: u64, origin_replica: ReplicaId) -> Self {
        Self {
            value,
            timestamp_ms,
            origin_replica,
        }
    }

    /// Get the current value
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Get the timestamp
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    /// Get the origin replica
    pub fn origin_replica(&self) -> &ReplicaId {
        &self.origin_replica
    }

    /// Check if this register should accept a new value based on LWW semantics.
    /// Returns true if the new value has a higher (timestamp, replica_id) pair.
    fn should_accept(&self, new_timestamp_ms: u64, new_origin_replica: &ReplicaId) -> bool {
        if new_timestamp_ms > self.timestamp_ms {
            return true;
        }
        if new_timestamp_ms == self.timestamp_ms {
            // Tie-break by replica ID (lexicographic order)
            return new_origin_replica.as_ref() > self.origin_replica.as_ref();
        }
        false
    }

    /// Update the value if the new (timestamp, replica) is higher.
    /// Returns true if the value was updated.
    pub fn update(
        &mut self,
        new_value: String,
        new_timestamp_ms: u64,
        new_origin_replica: ReplicaId,
    ) -> bool {
        if self.should_accept(new_timestamp_ms, &new_origin_replica) {
            self.value = new_value;
            self.timestamp_ms = new_timestamp_ms;
            self.origin_replica = new_origin_replica;
            true
        } else {
            false
        }
    }

    /// Merge another register into this one.
    /// Returns true if this register was updated.
    pub fn merge(&mut self, other: &LWWRegister) -> bool {
        if self.should_accept(other.timestamp_ms, &other.origin_replica) {
            self.value = other.value.clone();
            self.timestamp_ms = other.timestamp_ms;
            self.origin_replica = other.origin_replica.clone();
            true
        } else {
            false
        }
    }

    /// Check if the register has been set (non-default)
    pub fn is_empty(&self) -> bool {
        self.timestamp_ms == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn replica(s: &str) -> ReplicaId {
        Arc::from(s)
    }

    #[test]
    fn test_new_register() {
        let reg = LWWRegister::new("hello".to_string(), 1000, replica("r1"));
        assert_eq!(reg.value(), "hello");
        assert_eq!(reg.timestamp_ms(), 1000);
        assert_eq!(reg.origin_replica().as_ref(), "r1");
    }

    #[test]
    fn test_default_is_empty() {
        let reg = LWWRegister::default();
        assert!(reg.is_empty());
        assert_eq!(reg.value(), "");
    }

    #[test]
    fn test_update_higher_timestamp_wins() {
        let mut reg = LWWRegister::new("old".to_string(), 1000, replica("r1"));

        // Higher timestamp should win
        assert!(reg.update("new".to_string(), 2000, replica("r1")));
        assert_eq!(reg.value(), "new");
        assert_eq!(reg.timestamp_ms(), 2000);
    }

    #[test]
    fn test_update_lower_timestamp_rejected() {
        let mut reg = LWWRegister::new("current".to_string(), 2000, replica("r1"));

        // Lower timestamp should be rejected
        assert!(!reg.update("old".to_string(), 1000, replica("r1")));
        assert_eq!(reg.value(), "current");
        assert_eq!(reg.timestamp_ms(), 2000);
    }

    #[test]
    fn test_update_same_timestamp_higher_replica_wins() {
        let mut reg = LWWRegister::new("from_a".to_string(), 1000, replica("a"));

        // Same timestamp, but higher replica ID should win
        assert!(reg.update("from_b".to_string(), 1000, replica("b")));
        assert_eq!(reg.value(), "from_b");
        assert_eq!(reg.origin_replica().as_ref(), "b");

        // Lower replica ID should be rejected
        assert!(!reg.update("from_a_again".to_string(), 1000, replica("a")));
        assert_eq!(reg.value(), "from_b");
    }

    #[test]
    fn test_merge_lww_semantics() {
        let mut reg1 = LWWRegister::new("value1".to_string(), 1000, replica("r1"));
        let reg2 = LWWRegister::new("value2".to_string(), 2000, replica("r2"));

        // reg2 has higher timestamp, should win
        assert!(reg1.merge(&reg2));
        assert_eq!(reg1.value(), "value2");
        assert_eq!(reg1.timestamp_ms(), 2000);
    }

    #[test]
    fn test_merge_is_commutative() {
        let reg1 = LWWRegister::new("v1".to_string(), 1000, replica("r1"));
        let reg2 = LWWRegister::new("v2".to_string(), 2000, replica("r2"));

        let mut result1 = reg1.clone();
        result1.merge(&reg2);

        let mut result2 = reg2.clone();
        result2.merge(&reg1);

        // Both should converge to the same value
        assert_eq!(result1.value(), result2.value());
        assert_eq!(result1.timestamp_ms(), result2.timestamp_ms());
    }

    #[test]
    fn test_merge_is_idempotent() {
        let mut reg = LWWRegister::new("value".to_string(), 1000, replica("r1"));
        let original = reg.clone();

        // Merging with itself should not change anything
        assert!(!reg.merge(&original));
        assert_eq!(reg.value(), original.value());
        assert_eq!(reg.timestamp_ms(), original.timestamp_ms());
    }

    #[test]
    fn test_json_value() {
        let json = r#"{"name":"test","count":42}"#;
        let reg = LWWRegister::new(json.to_string(), 1000, replica("r1"));
        assert_eq!(reg.value(), json);
    }
}
