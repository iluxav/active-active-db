use crate::gcounter::{GCounter, ReplicaId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Current snapshot format version
/// Version 2: Added expirations field for TTL support
/// Version 3: Added decrements (N components) and strings (LWW-Register)
pub const SNAPSHOT_VERSION: u32 = 3;

/// Snapshot of a string value (LWW-Register)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringSnapshot {
    /// The string value
    pub value: String,
    /// Timestamp when the value was set (milliseconds since epoch)
    pub timestamp_ms: u64,
    /// Replica that set the value
    pub origin_replica: String,
}

/// A point-in-time snapshot of the counter store state.
/// Used for persistence and recovery.
///
/// Note: The snapshot uses String for serialization compatibility,
/// but the store uses Arc<str> internally for efficiency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Format version for compatibility checking
    pub version: u32,
    /// The replica ID that created this snapshot
    pub replica_id: String,
    /// When the snapshot was taken (ISO 8601)
    pub timestamp: DateTime<Utc>,
    /// All counters (P components): key -> (replica_id -> component_value)
    pub counters: HashMap<String, HashMap<String, u64>>,
    /// Decrements (N components): key -> (replica_id -> component_value)
    /// Only present for keys with decrement operations.
    #[serde(default)]
    pub decrements: HashMap<String, HashMap<String, u64>>,
    /// String values (LWW-Register): key -> StringSnapshot
    #[serde(default)]
    pub strings: HashMap<String, StringSnapshot>,
    /// Key expirations: key -> expiration timestamp in ms since Unix epoch
    /// Only present for keys that have TTL set. Absent keys never expire.
    #[serde(default)]
    pub expirations: HashMap<String, u64>,
}

impl Snapshot {
    /// Create a new empty snapshot
    pub fn new(replica_id: String) -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            replica_id,
            timestamp: Utc::now(),
            counters: HashMap::new(),
            decrements: HashMap::new(),
            strings: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    /// Create a snapshot from a map of counters (without expirations)
    pub fn from_counters(
        replica_id: String,
        counters: HashMap<String, HashMap<String, u64>>,
    ) -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            replica_id,
            timestamp: Utc::now(),
            counters,
            decrements: HashMap::new(),
            strings: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    /// Create a snapshot from counters with expirations
    pub fn from_counters_with_expirations(
        replica_id: String,
        counters: HashMap<String, HashMap<String, u64>>,
        expirations: HashMap<String, u64>,
    ) -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            replica_id,
            timestamp: Utc::now(),
            counters,
            decrements: HashMap::new(),
            strings: HashMap::new(),
            expirations,
        }
    }

    /// Create a full snapshot with counters, decrements, strings, and expirations
    pub fn from_full(
        replica_id: String,
        counters: HashMap<String, HashMap<String, u64>>,
        decrements: HashMap<String, HashMap<String, u64>>,
        strings: HashMap<String, StringSnapshot>,
        expirations: HashMap<String, u64>,
    ) -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            replica_id,
            timestamp: Utc::now(),
            counters,
            decrements,
            strings,
            expirations,
        }
    }

    /// Serialize to JSON string
    pub fn to_json(&self) -> Result<String, SnapshotError> {
        serde_json::to_string_pretty(self).map_err(SnapshotError::JsonSerializeError)
    }

    /// Deserialize from JSON string
    pub fn from_json(json: &str) -> Result<Self, SnapshotError> {
        let snapshot: Self =
            serde_json::from_str(json).map_err(SnapshotError::JsonDeserializeError)?;
        Self::validate_version(&snapshot)?;
        Ok(snapshot)
    }

    /// Serialize to bincode bytes (compact binary format)
    pub fn to_bincode(&self) -> Result<Vec<u8>, SnapshotError> {
        bincode::serialize(self).map_err(SnapshotError::BincodeSerializeError)
    }

    /// Deserialize from bincode bytes
    pub fn from_bincode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let snapshot: Self =
            bincode::deserialize(bytes).map_err(SnapshotError::BincodeDeserializeError)?;
        Self::validate_version(&snapshot)?;
        Ok(snapshot)
    }

    /// Validate snapshot version
    fn validate_version(snapshot: &Self) -> Result<(), SnapshotError> {
        if snapshot.version > SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion {
                found: snapshot.version,
                max_supported: SNAPSHOT_VERSION,
            });
        }
        Ok(())
    }

    /// Get the number of keys in this snapshot (counters + strings)
    pub fn key_count(&self) -> usize {
        self.counters.len() + self.strings.len()
    }

    /// Check if the snapshot is empty
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty() && self.strings.is_empty()
    }

    /// Convert a GCounter to the snapshot format (String-based HashMap)
    pub fn gcounter_to_map(counter: &GCounter) -> HashMap<String, u64> {
        counter
            .components()
            .map(|(replica_id, &value)| (replica_id.to_string(), value))
            .collect()
    }

    /// Convert snapshot format back to a GCounter
    pub fn map_to_gcounter(components: &HashMap<String, u64>) -> GCounter {
        let mut counter = GCounter::new();
        for (replica_id, &value) in components {
            let replica_id: ReplicaId = Arc::from(replica_id.as_str());
            counter.apply_delta(&replica_id, value);
        }
        counter
    }
}

/// Errors that can occur during snapshot operations
#[derive(Debug)]
pub enum SnapshotError {
    JsonSerializeError(serde_json::Error),
    JsonDeserializeError(serde_json::Error),
    BincodeSerializeError(bincode::Error),
    BincodeDeserializeError(bincode::Error),
    UnsupportedVersion { found: u32, max_supported: u32 },
    IoError(std::io::Error),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::JsonSerializeError(e) => {
                write!(f, "Failed to serialize snapshot to JSON: {}", e)
            }
            SnapshotError::JsonDeserializeError(e) => {
                write!(f, "Failed to deserialize snapshot from JSON: {}", e)
            }
            SnapshotError::BincodeSerializeError(e) => {
                write!(f, "Failed to serialize snapshot to bincode: {}", e)
            }
            SnapshotError::BincodeDeserializeError(e) => {
                write!(f, "Failed to deserialize snapshot from bincode: {}", e)
            }
            SnapshotError::UnsupportedVersion {
                found,
                max_supported,
            } => write!(
                f,
                "Unsupported snapshot version: found {}, max supported {}",
                found, max_supported
            ),
            SnapshotError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<std::io::Error> for SnapshotError {
    fn from(e: std::io::Error) -> Self {
        SnapshotError::IoError(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica(s: &str) -> ReplicaId {
        Arc::from(s)
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = Snapshot::new("replica-1".to_string());
        assert_eq!(snapshot.version, SNAPSHOT_VERSION);
        assert_eq!(snapshot.replica_id, "replica-1");
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.key_count(), 0);
    }

    #[test]
    fn test_snapshot_serialization_roundtrip() {
        let mut counters = HashMap::new();

        let mut counter1 = HashMap::new();
        counter1.insert("r1".to_string(), 100u64);
        counter1.insert("r2".to_string(), 50u64);
        counters.insert("api:requests".to_string(), counter1);

        let mut counter2 = HashMap::new();
        counter2.insert("r1".to_string(), 42u64);
        counters.insert("errors:svc1".to_string(), counter2);

        let snapshot = Snapshot::from_counters("replica-1".to_string(), counters);

        // Serialize
        let json = snapshot.to_json().unwrap();
        assert!(json.contains("api:requests"));
        assert!(json.contains("errors:svc1"));

        // Deserialize
        let restored = Snapshot::from_json(&json).unwrap();
        assert_eq!(restored.version, snapshot.version);
        assert_eq!(restored.replica_id, snapshot.replica_id);
        assert_eq!(restored.key_count(), 2);

        // Verify counter values
        let requests = restored.counters.get("api:requests").unwrap();
        assert_eq!(requests.get("r1"), Some(&100));
        assert_eq!(requests.get("r2"), Some(&50));
    }

    #[test]
    fn test_gcounter_conversion() {
        let mut counter = GCounter::new();
        let r1 = replica("r1");
        let r2 = replica("r2");
        counter.increment(&r1, 100);
        counter.increment(&r2, 50);

        // Convert to map
        let map = Snapshot::gcounter_to_map(&counter);
        assert_eq!(map.get("r1"), Some(&100));
        assert_eq!(map.get("r2"), Some(&50));

        // Convert back
        let restored = Snapshot::map_to_gcounter(&map);
        assert_eq!(restored.value(), 150);
        assert_eq!(restored.component("r1"), 100);
        assert_eq!(restored.component("r2"), 50);
    }

    #[test]
    fn test_unsupported_version() {
        let json = r#"{
            "version": 999,
            "replica_id": "r1",
            "timestamp": "2024-01-15T10:30:00Z",
            "counters": {}
        }"#;

        let result = Snapshot::from_json(json);
        assert!(matches!(
            result,
            Err(SnapshotError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn test_invalid_json() {
        let result = Snapshot::from_json("not valid json");
        assert!(matches!(
            result,
            Err(SnapshotError::JsonDeserializeError(_))
        ));
    }

    #[test]
    fn test_bincode_serialization_roundtrip() {
        let mut counters = HashMap::new();

        let mut counter1 = HashMap::new();
        counter1.insert("r1".to_string(), 100u64);
        counter1.insert("r2".to_string(), 50u64);
        counters.insert("api:requests".to_string(), counter1);

        let mut counter2 = HashMap::new();
        counter2.insert("r1".to_string(), 42u64);
        counters.insert("errors:svc1".to_string(), counter2);

        let snapshot = Snapshot::from_counters("replica-1".to_string(), counters);

        // Serialize to bincode
        let bytes = snapshot.to_bincode().unwrap();

        // Bincode should be much smaller than JSON
        let json = snapshot.to_json().unwrap();
        assert!(
            bytes.len() < json.len(),
            "bincode ({} bytes) should be smaller than JSON ({} bytes)",
            bytes.len(),
            json.len()
        );

        // Deserialize from bincode
        let restored = Snapshot::from_bincode(&bytes).unwrap();
        assert_eq!(restored.version, snapshot.version);
        assert_eq!(restored.replica_id, snapshot.replica_id);
        assert_eq!(restored.key_count(), 2);

        // Verify counter values
        let requests = restored.counters.get("api:requests").unwrap();
        assert_eq!(requests.get("r1"), Some(&100));
        assert_eq!(requests.get("r2"), Some(&50));
    }

    #[test]
    fn test_invalid_bincode() {
        let result = Snapshot::from_bincode(&[0, 1, 2, 3]);
        assert!(matches!(
            result,
            Err(SnapshotError::BincodeDeserializeError(_))
        ));
    }
}
