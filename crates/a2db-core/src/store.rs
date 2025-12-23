use crate::delta::{Delta, DeltaType};
use crate::gcounter::{GCounter, Key, ReplicaId};
use crate::lww_register::LWWRegister;
use crate::snapshot::Snapshot;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Entry in the counter store, containing the counter and optional expiration.
#[derive(Debug, Clone, Default)]
pub struct CounterEntry {
    /// The PN-Counter CRDT
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
        Self {
            counter,
            expires_at_ms,
        }
    }
}

/// Entry for string values using LWW-Register
#[derive(Debug, Clone)]
pub struct StringEntry {
    /// The LWW-Register CRDT
    pub register: LWWRegister,
    /// Optional expiration timestamp in milliseconds since Unix epoch.
    pub expires_at_ms: Option<u64>,
}

impl StringEntry {
    pub fn new(register: LWWRegister) -> Self {
        Self {
            register,
            expires_at_ms: None,
        }
    }

    pub fn with_expiration(register: LWWRegister, expires_at_ms: Option<u64>) -> Self {
        Self {
            register,
            expires_at_ms,
        }
    }
}

/// Value type stored in the database
#[derive(Debug, Clone)]
pub enum ValueType {
    Counter(CounterEntry),
    String(StringEntry),
}

impl Default for ValueType {
    fn default() -> Self {
        ValueType::Counter(CounterEntry::default())
    }
}

impl ValueType {
    /// Get the expiration timestamp
    pub fn expires_at_ms(&self) -> Option<u64> {
        match self {
            ValueType::Counter(e) => e.expires_at_ms,
            ValueType::String(e) => e.expires_at_ms,
        }
    }

    /// Set the expiration timestamp
    pub fn set_expires_at_ms(&mut self, expires_at_ms: Option<u64>) {
        match self {
            ValueType::Counter(e) => e.expires_at_ms = expires_at_ms,
            ValueType::String(e) => e.expires_at_ms = expires_at_ms,
        }
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
    /// Map of key -> ValueType, using DashMap for concurrent access
    entries: DashMap<Key, ValueType>,
}

/// Get current time in milliseconds since Unix epoch
pub fn current_time_ms() -> u64 {
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
            entries: DashMap::new(),
        }
    }

    /// Create with a replica ID string (convenience method)
    pub fn with_replica_id(local_replica_id: &str) -> Self {
        Self::new(Arc::from(local_replica_id))
    }

    /// Get the local replica's ID
    pub fn local_replica_id(&self) -> &ReplicaId {
        &self.local_replica_id
    }

    /// Check if an entry is expired
    #[inline]
    fn is_entry_expired(entry: &ValueType) -> bool {
        match entry.expires_at_ms() {
            Some(exp) => current_time_ms() >= exp,
            None => false,
        }
    }

    /// Increment a counter by amount.
    /// Returns (new total value, delta to replicate).
    ///
    /// Returns None if the key exists but holds a string value.
    /// If the key is expired, it's treated as a fresh key (starts from 0).
    pub fn increment(&self, key: &Key, amount: u64) -> Option<(i64, Delta)> {
        let mut entry = self.entries.entry(key.clone()).or_default();

        // Check for expired string and replace with counter
        if let ValueType::String(string_entry) = entry.value() {
            if let Some(exp) = string_entry.expires_at_ms {
                if current_time_ms() >= exp {
                    *entry.value_mut() = ValueType::Counter(CounterEntry::default());
                }
            }
        }

        match entry.value_mut() {
            ValueType::Counter(counter_entry) => {
                // Reset if expired (only checks timestamp when TTL is set)
                if let Some(exp) = counter_entry.expires_at_ms {
                    if current_time_ms() >= exp {
                        *counter_entry = CounterEntry::default();
                    }
                }

                let new_component = counter_entry
                    .counter
                    .increment(&self.local_replica_id, amount);
                let total = counter_entry.counter.value();

                let delta = Delta::with_type_and_expiration(
                    key.clone(),
                    self.local_replica_id.clone(),
                    new_component,
                    DeltaType::P,
                    counter_entry.expires_at_ms,
                );

                Some((total, delta))
            }
            ValueType::String(_) => None, // Type mismatch (non-expired string)
        }
    }

    /// Increment using a string slice key (convenience method)
    pub fn increment_str(&self, key: &str, amount: u64) -> Option<(i64, Delta)> {
        let key: Key = Arc::from(key);
        self.increment(&key, amount)
    }

    /// Decrement a counter by amount.
    /// Returns (new total value, delta to replicate).
    ///
    /// Returns None if the key exists but holds a string value.
    /// If the key is expired, it's treated as a fresh key (starts from 0).
    pub fn decrement(&self, key: &Key, amount: u64) -> Option<(i64, Delta)> {
        let mut entry = self.entries.entry(key.clone()).or_default();

        // Check for expired string and replace with counter
        if let ValueType::String(string_entry) = entry.value() {
            if let Some(exp) = string_entry.expires_at_ms {
                if current_time_ms() >= exp {
                    *entry.value_mut() = ValueType::Counter(CounterEntry::default());
                }
            }
        }

        match entry.value_mut() {
            ValueType::Counter(counter_entry) => {
                // Reset if expired (only checks timestamp when TTL is set)
                if let Some(exp) = counter_entry.expires_at_ms {
                    if current_time_ms() >= exp {
                        *counter_entry = CounterEntry::default();
                    }
                }

                let new_component = counter_entry
                    .counter
                    .decrement(&self.local_replica_id, amount);
                let total = counter_entry.counter.value();

                let delta = Delta::with_type_and_expiration(
                    key.clone(),
                    self.local_replica_id.clone(),
                    new_component,
                    DeltaType::N,
                    counter_entry.expires_at_ms,
                );

                Some((total, delta))
            }
            ValueType::String(_) => None, // Type mismatch (non-expired string)
        }
    }

    /// Decrement using a string slice key (convenience method)
    pub fn decrement_str(&self, key: &str, amount: u64) -> Option<(i64, Delta)> {
        let key: Key = Arc::from(key);
        self.decrement(&key, amount)
    }

    /// Set a string value using LWW-Register.
    /// Returns the delta to replicate, or None if key holds a counter.
    /// If the key is expired, it's treated as a fresh key.
    pub fn set_string(&self, key: &Key, value: String) -> Option<Delta> {
        let timestamp = current_time_ms();
        let register = LWWRegister::new(value.clone(), timestamp, self.local_replica_id.clone());

        let mut entry = self
            .entries
            .entry(key.clone())
            .or_insert_with(|| ValueType::String(StringEntry::new(register.clone())));

        // Check for expired counter and replace with string
        if let ValueType::Counter(counter_entry) = entry.value() {
            if let Some(exp) = counter_entry.expires_at_ms {
                if timestamp >= exp {
                    *entry.value_mut() = ValueType::String(StringEntry::new(register.clone()));
                }
            }
        }

        match entry.value_mut() {
            ValueType::String(string_entry) => {
                // Reset if expired
                if let Some(exp) = string_entry.expires_at_ms {
                    if timestamp >= exp {
                        *string_entry = StringEntry::new(register.clone());
                    }
                }
                string_entry.register = register;
                Some(Delta::string(
                    key.clone(),
                    self.local_replica_id.clone(),
                    value,
                    timestamp,
                    string_entry.expires_at_ms,
                ))
            }
            ValueType::Counter(_) => None, // Type mismatch (non-expired counter)
        }
    }

    /// Set string using a string slice key (convenience method)
    pub fn set_string_str(&self, key: &str, value: String) -> Option<Delta> {
        let key: Key = Arc::from(key);
        self.set_string(&key, value)
    }

    /// Get a counter's value (returns 0 for non-existent, expired, or string keys)
    pub fn get(&self, key: &str) -> i64 {
        self.entries
            .get(key)
            .and_then(|entry| {
                if Self::is_entry_expired(entry.value()) {
                    None
                } else {
                    match entry.value() {
                        ValueType::Counter(e) => Some(e.counter.value()),
                        ValueType::String(_) => None,
                    }
                }
            })
            .unwrap_or(0)
    }

    /// Debug: Get raw CRDT components for a counter key.
    /// Returns (p_components, n_components, value, expires_at_ms) or None if not found/expired/string.
    #[allow(clippy::type_complexity)]
    pub fn debug_counter_state(
        &self,
        key: &str,
    ) -> Option<(Vec<(String, u64)>, Vec<(String, u64)>, i64, Option<u64>)> {
        self.entries.get(key).and_then(|entry| {
            if Self::is_entry_expired(entry.value()) {
                return None;
            }
            match entry.value() {
                ValueType::Counter(e) => {
                    let p: Vec<(String, u64)> = e
                        .counter
                        .p_components()
                        .map(|(r, v)| (r.to_string(), *v))
                        .collect();
                    let n: Vec<(String, u64)> = e
                        .counter
                        .n_components()
                        .map(|(r, v)| (r.to_string(), *v))
                        .collect();
                    let value = e.counter.value();
                    Some((p, n, value, e.expires_at_ms))
                }
                ValueType::String(_) => None,
            }
        })
    }

    /// Get a string value (returns None for non-existent, expired, or counter keys)
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.entries.get(key).and_then(|entry| {
            if Self::is_entry_expired(entry.value()) {
                None
            } else {
                match entry.value() {
                    ValueType::String(e) => Some(e.register.value().to_string()),
                    ValueType::Counter(_) => None,
                }
            }
        })
    }

    /// Get the type of a key ("string", "counter", or None if not found/expired)
    pub fn get_type(&self, key: &str) -> Option<&'static str> {
        self.entries.get(key).and_then(|entry| {
            if Self::is_entry_expired(entry.value()) {
                None
            } else {
                match entry.value() {
                    ValueType::Counter(_) => Some("counter"),
                    ValueType::String(_) => Some("string"),
                }
            }
        })
    }

    /// Check if a key exists (and is not expired)
    pub fn exists(&self, key: &str) -> bool {
        self.entries
            .get(key)
            .map(|entry| !Self::is_entry_expired(entry.value()))
            .unwrap_or(false)
    }

    /// Get multiple counter values.
    /// Returns values in the same order as requested keys.
    /// Non-existent, expired, or string keys return 0.
    pub fn mget(&self, keys: &[String]) -> Vec<i64> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    /// Apply a delta from replication.
    /// Returns true if the state changed (delta was newer or expiration extended).
    pub fn apply_delta(&self, delta: &Delta) -> bool {
        match delta.delta_type {
            DeltaType::P | DeltaType::N => self.apply_counter_delta(delta),
            DeltaType::S => self.apply_string_delta(delta),
        }
    }

    /// Apply a counter (P or N) delta
    fn apply_counter_delta(&self, delta: &Delta) -> bool {
        let mut entry = self.entries.entry(delta.key.clone()).or_default();

        match entry.value_mut() {
            ValueType::Counter(counter_entry) => {
                // Apply counter component based on delta type
                let counter_changed = match delta.delta_type {
                    DeltaType::P => counter_entry
                        .counter
                        .apply_p_delta(&delta.origin_replica_id, delta.component_value),
                    DeltaType::N => counter_entry
                        .counter
                        .apply_n_delta(&delta.origin_replica_id, delta.component_value),
                    DeltaType::S => false, // Should not happen
                };

                // Apply expiration with MAX semantics
                let expiry_changed =
                    Self::merge_expiration(&mut counter_entry.expires_at_ms, delta.expires_at_ms);

                counter_changed || expiry_changed
            }
            ValueType::String(_) => false, // Type mismatch, ignore
        }
    }

    /// Apply a string (S) delta
    fn apply_string_delta(&self, delta: &Delta) -> bool {
        let timestamp = delta.timestamp_ms.unwrap_or(0);
        let value = delta.string_value.clone().unwrap_or_default();

        let mut entry = self
            .entries
            .entry(delta.key.clone())
            .or_insert_with(|| ValueType::String(StringEntry::new(LWWRegister::default())));

        match entry.value_mut() {
            ValueType::String(string_entry) => {
                let register_changed =
                    string_entry
                        .register
                        .update(value, timestamp, delta.origin_replica_id.clone());

                let expiry_changed =
                    Self::merge_expiration(&mut string_entry.expires_at_ms, delta.expires_at_ms);

                register_changed || expiry_changed
            }
            ValueType::Counter(_) => false, // Type mismatch, ignore
        }
    }

    /// Merge expiration timestamps using MAX semantics.
    /// u64::MAX is treated as "never expires" (converts to None locally).
    fn merge_expiration(current: &mut Option<u64>, incoming: Option<u64>) -> bool {
        match (*current, incoming) {
            // u64::MAX means "persist" - remove expiration
            (Some(_), Some(u64::MAX)) | (None, Some(u64::MAX)) => {
                if current.is_some() {
                    *current = None;
                    true
                } else {
                    false
                }
            }
            (None, Some(t)) => {
                *current = Some(t);
                true
            }
            (Some(a), Some(b)) if b > a => {
                *current = Some(b);
                true
            }
            _ => false,
        }
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

        for entry in self.entries.iter() {
            if Self::is_entry_expired(entry.value()) {
                continue;
            }

            let key = entry.key();

            match entry.value() {
                ValueType::Counter(counter_entry) => {
                    // P deltas
                    for (replica_id, &value) in counter_entry.counter.p_components() {
                        deltas.push(Delta::with_type_and_expiration(
                            key.clone(),
                            replica_id.clone(),
                            value,
                            DeltaType::P,
                            counter_entry.expires_at_ms,
                        ));
                    }
                    // N deltas
                    for (replica_id, &value) in counter_entry.counter.n_components() {
                        deltas.push(Delta::with_type_and_expiration(
                            key.clone(),
                            replica_id.clone(),
                            value,
                            DeltaType::N,
                            counter_entry.expires_at_ms,
                        ));
                    }
                }
                ValueType::String(string_entry) => {
                    deltas.push(Delta::string(
                        key.clone(),
                        string_entry.register.origin_replica().clone(),
                        string_entry.register.value().to_string(),
                        string_entry.register.timestamp_ms(),
                        string_entry.expires_at_ms,
                    ));
                }
            }
        }

        deltas
    }

    /// Get the number of keys in the store (includes expired keys not yet cleaned up)
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all keys (for debugging/admin purposes, excludes expired keys)
    pub fn keys(&self) -> Vec<Key> {
        self.entries
            .iter()
            .filter(|entry| !Self::is_entry_expired(entry.value()))
            .map(|entry| entry.key().clone())
            .collect()
    }

    // === TTL Operations ===

    /// Set expiration time using absolute Unix timestamp in milliseconds.
    /// Returns a delta for replication if the key exists, None otherwise.
    pub fn expire_at(&self, key: &str, expires_at_ms: u64) -> Option<Delta> {
        let key_arc: Key = Arc::from(key);
        if let Some(mut entry) = self.entries.get_mut(key) {
            // Skip if already expired
            if Self::is_entry_expired(entry.value()) {
                return None;
            }

            entry.value_mut().set_expires_at_ms(Some(expires_at_ms));

            // Create delta with current value + new expiration
            match entry.value() {
                ValueType::Counter(counter_entry) => {
                    // Get current P component for this replica
                    let component = counter_entry.counter.component(&self.local_replica_id);
                    Some(Delta::with_type_and_expiration(
                        key_arc,
                        self.local_replica_id.clone(),
                        component,
                        DeltaType::P,
                        Some(expires_at_ms),
                    ))
                }
                ValueType::String(string_entry) => Some(Delta::string(
                    key_arc,
                    string_entry.register.origin_replica().clone(),
                    string_entry.register.value().to_string(),
                    string_entry.register.timestamp_ms(),
                    Some(expires_at_ms),
                )),
            }
        } else {
            None
        }
    }

    /// Set expiration using relative TTL in milliseconds.
    /// Returns a delta for replication if the key exists, None otherwise.
    pub fn expire(&self, key: &str, ttl_ms: u64) -> Option<Delta> {
        let expires_at = current_time_ms() + ttl_ms;
        self.expire_at(key, expires_at)
    }

    /// Remove expiration from a key (make it persistent).
    /// Returns a delta for replication if the key exists, None otherwise.
    /// Uses u64::MAX as "never expires" to win with MAX semantics.
    pub fn persist(&self, key: &str) -> Option<Delta> {
        let key_arc: Key = Arc::from(key);
        if let Some(mut entry) = self.entries.get_mut(key) {
            // Skip if already expired
            if Self::is_entry_expired(entry.value()) {
                return None;
            }

            // Set to None locally (no expiration)
            entry.value_mut().set_expires_at_ms(None);

            // Create delta with u64::MAX to win with MAX semantics
            match entry.value() {
                ValueType::Counter(counter_entry) => {
                    let component = counter_entry.counter.component(&self.local_replica_id);
                    Some(Delta::with_type_and_expiration(
                        key_arc,
                        self.local_replica_id.clone(),
                        component,
                        DeltaType::P,
                        Some(u64::MAX), // "Never expires" wins over any finite TTL
                    ))
                }
                ValueType::String(string_entry) => Some(Delta::string(
                    key_arc,
                    string_entry.register.origin_replica().clone(),
                    string_entry.register.value().to_string(),
                    string_entry.register.timestamp_ms(),
                    Some(u64::MAX),
                )),
            }
        } else {
            None
        }
    }

    /// Get remaining TTL in milliseconds.
    /// Returns: remaining ms (positive), -1 (no TTL), or -2 (key not found/expired)
    pub fn pttl(&self, key: &str) -> i64 {
        match self.entries.get(key) {
            Some(entry) => {
                if Self::is_entry_expired(entry.value()) {
                    -2 // Expired, treat as not found
                } else {
                    match entry.value().expires_at_ms() {
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
            pttl / 1000
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
        for entry in self.entries.iter() {
            if keys_to_remove.len() >= batch_size {
                break;
            }
            if let Some(exp) = entry.value().expires_at_ms() {
                // Only remove if expired AND past grace period
                if exp <= threshold {
                    keys_to_remove.push(entry.key().clone());
                }
            }
        }

        // Remove collected keys
        for key in keys_to_remove {
            if self.entries.remove(&key).is_some() {
                removed += 1;
            }
        }

        removed
    }

    /// Export the current state as a snapshot for persistence.
    /// Excludes expired keys from the snapshot.
    pub fn export_snapshot(&self) -> Snapshot {
        let mut snapshot_counters = HashMap::new();
        let mut snapshot_decrements = HashMap::new();
        let mut snapshot_strings = HashMap::new();
        let mut expirations = HashMap::new();

        for entry in self.entries.iter() {
            if Self::is_entry_expired(entry.value()) {
                continue;
            }
            let key = entry.key().to_string();

            match entry.value() {
                ValueType::Counter(counter_entry) => {
                    // P components
                    let p_map: HashMap<String, u64> = counter_entry
                        .counter
                        .p_components()
                        .map(|(r, v)| (r.to_string(), *v))
                        .collect();
                    if !p_map.is_empty() {
                        snapshot_counters.insert(key.clone(), p_map);
                    }

                    // N components
                    let n_map: HashMap<String, u64> = counter_entry
                        .counter
                        .n_components()
                        .map(|(r, v)| (r.to_string(), *v))
                        .collect();
                    if !n_map.is_empty() {
                        snapshot_decrements.insert(key.clone(), n_map);
                    }

                    if let Some(exp) = counter_entry.expires_at_ms {
                        expirations.insert(key, exp);
                    }
                }
                ValueType::String(string_entry) => {
                    snapshot_strings.insert(
                        key.clone(),
                        crate::snapshot::StringSnapshot {
                            value: string_entry.register.value().to_string(),
                            timestamp_ms: string_entry.register.timestamp_ms(),
                            origin_replica: string_entry.register.origin_replica().to_string(),
                        },
                    );
                    if let Some(exp) = string_entry.expires_at_ms {
                        expirations.insert(key, exp);
                    }
                }
            }
        }

        Snapshot::from_full(
            self.local_replica_id.to_string(),
            snapshot_counters,
            snapshot_decrements,
            snapshot_strings,
            expirations,
        )
    }

    /// Import a snapshot, merging with existing state.
    /// Uses CRDT merge semantics (max per component), so this is idempotent.
    /// TTL uses MAX semantics (extends, never shortens).
    pub fn import_snapshot(&self, snapshot: &Snapshot) {
        // Import counters (P components)
        for (key, components) in &snapshot.counters {
            let key_arc: Key = Arc::from(key.as_str());
            let mut entry = self.entries.entry(key_arc.clone()).or_default();

            if let ValueType::Counter(counter_entry) = entry.value_mut() {
                for (replica_id, &value) in components {
                    let replica_id: ReplicaId = Arc::from(replica_id.as_str());
                    counter_entry.counter.apply_p_delta(&replica_id, value);
                }

                // Apply expiration with MAX semantics
                if let Some(&exp) = snapshot.expirations.get(key) {
                    Self::merge_expiration(&mut counter_entry.expires_at_ms, Some(exp));
                }
            }
        }

        // Import decrements (N components)
        for (key, components) in &snapshot.decrements {
            let key_arc: Key = Arc::from(key.as_str());
            let mut entry = self.entries.entry(key_arc.clone()).or_default();

            if let ValueType::Counter(counter_entry) = entry.value_mut() {
                for (replica_id, &value) in components {
                    let replica_id: ReplicaId = Arc::from(replica_id.as_str());
                    counter_entry.counter.apply_n_delta(&replica_id, value);
                }
            }
        }

        // Import strings
        for (key, string_snapshot) in &snapshot.strings {
            let key_arc: Key = Arc::from(key.as_str());
            let replica: ReplicaId = Arc::from(string_snapshot.origin_replica.as_str());
            let register = LWWRegister::new(
                string_snapshot.value.clone(),
                string_snapshot.timestamp_ms,
                replica,
            );

            let mut entry = self
                .entries
                .entry(key_arc)
                .or_insert_with(|| ValueType::String(StringEntry::new(LWWRegister::default())));

            if let ValueType::String(string_entry) = entry.value_mut() {
                string_entry.register.merge(&register);

                if let Some(&exp) = snapshot.expirations.get(key) {
                    Self::merge_expiration(&mut string_entry.expires_at_ms, Some(exp));
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
        let store = CounterStore::with_replica_id("replica-1");
        assert_eq!(store.local_replica_id().as_ref(), "replica-1");
        assert!(store.is_empty());
    }

    #[test]
    fn test_increment_and_get() {
        let store = CounterStore::with_replica_id("r1");
        let k = key("counter:foo");

        let (value, delta) = store.increment(&k, 5).unwrap();
        assert_eq!(value, 5);
        assert_eq!(delta.key.as_ref(), "counter:foo");
        assert_eq!(delta.origin_replica_id.as_ref(), "r1");
        assert_eq!(delta.component_value, 5);
        assert_eq!(delta.delta_type, DeltaType::P);

        assert_eq!(store.get("counter:foo"), 5);

        let (value, delta) = store.increment(&k, 3).unwrap();
        assert_eq!(value, 8);
        assert_eq!(delta.component_value, 8);
    }

    #[test]
    fn test_decrement_and_get() {
        let store = CounterStore::with_replica_id("r1");
        let k = key("counter:foo");

        // Increment first
        store.increment(&k, 10).unwrap();
        assert_eq!(store.get("counter:foo"), 10);

        // Decrement
        let (value, delta) = store.decrement(&k, 3).unwrap();
        assert_eq!(value, 7);
        assert_eq!(delta.delta_type, DeltaType::N);
        assert_eq!(delta.component_value, 3);

        assert_eq!(store.get("counter:foo"), 7);
    }

    #[test]
    fn test_decrement_below_zero() {
        let store = CounterStore::with_replica_id("r1");
        let k = key("counter:foo");

        store.increment(&k, 5).unwrap();
        store.decrement(&k, 10).unwrap();

        // Value can go negative
        assert_eq!(store.get("counter:foo"), -5);
    }

    #[test]
    fn test_string_set_and_get() {
        let store = CounterStore::with_replica_id("r1");

        let delta = store.set_string_str("mykey", "hello".to_string()).unwrap();
        assert_eq!(delta.delta_type, DeltaType::S);
        assert_eq!(delta.string_value, Some("hello".to_string()));

        assert_eq!(store.get_string("mykey"), Some("hello".to_string()));
        assert_eq!(store.get_type("mykey"), Some("string"));
    }

    #[test]
    fn test_type_mismatch_counter_to_string() {
        let store = CounterStore::with_replica_id("r1");
        let k = key("mykey");

        // Set as counter
        store.increment(&k, 10).unwrap();
        assert_eq!(store.get_type("mykey"), Some("counter"));

        // Try to set as string - should fail
        assert!(store.set_string(&k, "hello".to_string()).is_none());

        // Counter should still work
        assert_eq!(store.get("mykey"), 10);
    }

    #[test]
    fn test_type_mismatch_string_to_counter() {
        let store = CounterStore::with_replica_id("r1");
        let k = key("mykey");

        // Set as string
        store.set_string(&k, "hello".to_string()).unwrap();
        assert_eq!(store.get_type("mykey"), Some("string"));

        // Try to increment - should fail
        assert!(store.increment(&k, 10).is_none());
        assert!(store.decrement(&k, 5).is_none());

        // String should still work
        assert_eq!(store.get_string("mykey"), Some("hello".to_string()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let store = CounterStore::with_replica_id("r1");
        assert_eq!(store.get("nonexistent"), 0);
        assert_eq!(store.get_string("nonexistent"), None);
        assert_eq!(store.get_type("nonexistent"), None);
    }

    #[test]
    fn test_mget() {
        let store = CounterStore::with_replica_id("r1");

        store.increment_str("k1", 10);
        store.increment_str("k2", 20);
        store.increment_str("k3", 30);

        let values = store.mget(&["k1".into(), "k2".into(), "k4".into(), "k3".into()]);
        assert_eq!(values, vec![10, 20, 0, 30]);
    }

    #[test]
    fn test_apply_delta() {
        let store = CounterStore::with_replica_id("r1");

        // Apply P delta from another replica
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
    fn test_apply_n_delta() {
        let store = CounterStore::with_replica_id("r1");

        // First increment
        store.increment_str("k1", 100);

        // Apply N delta from another replica
        let delta = Delta::with_type(key("k1"), Arc::from("r2"), 30, DeltaType::N);
        assert!(store.apply_delta(&delta));

        // Value = 100 - 30 = 70
        assert_eq!(store.get("k1"), 70);
    }

    #[test]
    fn test_apply_string_delta() {
        let store = CounterStore::with_replica_id("r1");

        let delta = Delta::string(
            key("mykey"),
            Arc::from("r2"),
            "hello from r2".to_string(),
            1000,
            None,
        );

        assert!(store.apply_delta(&delta));
        assert_eq!(store.get_string("mykey"), Some("hello from r2".to_string()));
    }

    #[test]
    fn test_apply_deltas_batch() {
        let store = CounterStore::with_replica_id("r1");

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
        let store1 = CounterStore::with_replica_id("r1");
        let store2 = CounterStore::with_replica_id("r2");

        // Both replicas increment the same key
        let k = key("shared");
        let (_, delta1) = store1.increment(&k, 10).unwrap();
        let (_, delta2) = store2.increment(&k, 7).unwrap();

        // Exchange deltas
        store1.apply_delta(&delta2);
        store2.apply_delta(&delta1);

        // Both should see the same value
        assert_eq!(store1.get("shared"), 17);
        assert_eq!(store2.get("shared"), 17);
    }

    #[test]
    fn test_simultaneous_increment_convergence() {
        // Simulates the user's scenario:
        // - Two replicas both have counter at 10
        // - Both increment by 1 simultaneously
        // - After sync, both should show 12

        let store1 = CounterStore::with_replica_id("nyc");
        let store2 = CounterStore::with_replica_id("sfo");

        let k = key("counter");

        // Step 1: Build up to value 10 on both replicas
        // NYC increments 5 times
        for _ in 0..5 {
            let (_, delta) = store1.increment(&k, 1).unwrap();
            store2.apply_delta(&delta); // Sync to SFO
        }
        // SFO increments 5 times
        for _ in 0..5 {
            let (_, delta) = store2.increment(&k, 1).unwrap();
            store1.apply_delta(&delta); // Sync to NYC
        }

        // Both should now show 10
        assert_eq!(
            store1.get("counter"),
            10,
            "NYC should have 10 before simultaneous increment"
        );
        assert_eq!(
            store2.get("counter"),
            10,
            "SFO should have 10 before simultaneous increment"
        );

        // Step 2: Simulate SIMULTANEOUS increment by 1 on both
        // (Before they exchange deltas)
        let (local_val1, delta1) = store1.increment(&k, 1).unwrap();
        let (local_val2, delta2) = store2.increment(&k, 1).unwrap();

        // Each sees their LOCAL value (11) before sync
        assert_eq!(
            local_val1, 11,
            "NYC local value should be 11 immediately after INCR"
        );
        assert_eq!(
            local_val2, 11,
            "SFO local value should be 11 immediately after INCR"
        );

        // GET also shows local value before sync
        assert_eq!(
            store1.get("counter"),
            11,
            "NYC GET should return 11 before sync"
        );
        assert_eq!(
            store2.get("counter"),
            11,
            "SFO GET should return 11 before sync"
        );

        // Step 3: Exchange deltas (simulate replication)
        let changed1 = store1.apply_delta(&delta2);
        let changed2 = store2.apply_delta(&delta1);

        assert!(changed1, "NYC should have applied SFO's delta");
        assert!(changed2, "SFO should have applied NYC's delta");

        // Step 4: After sync, BOTH should show 12
        assert_eq!(store1.get("counter"), 12, "NYC should have 12 after sync");
        assert_eq!(store2.get("counter"), 12, "SFO should have 12 after sync");

        // Verify internal state
        // NYC should have: {nyc: 6, sfo: 6} = 12
        // SFO should have: {nyc: 6, sfo: 6} = 12
        println!("Test passed: simultaneous increments correctly converge to 12");
    }

    #[test]
    fn test_simultaneous_increment_multiple_rounds() {
        // More rigorous test: multiple rounds of simultaneous increments

        let store1 = CounterStore::with_replica_id("nyc");
        let store2 = CounterStore::with_replica_id("sfo");

        let k = key("counter");

        // 10 rounds of simultaneous increments
        for round in 1..=10 {
            // Both increment simultaneously (before exchanging deltas)
            let (_, delta1) = store1.increment(&k, 1).unwrap();
            let (_, delta2) = store2.increment(&k, 1).unwrap();

            // Exchange deltas
            store1.apply_delta(&delta2);
            store2.apply_delta(&delta1);

            // After each round, value should be round * 2
            let expected = (round * 2) as i64;
            assert_eq!(
                store1.get("counter"),
                expected,
                "Round {}: NYC should have {}",
                round,
                expected
            );
            assert_eq!(
                store2.get("counter"),
                expected,
                "Round {}: SFO should have {}",
                round,
                expected
            );
        }

        // Final value should be 20 (10 rounds * 2 increments per round)
        assert_eq!(store1.get("counter"), 20);
        assert_eq!(store2.get("counter"), 20);
    }

    #[test]
    fn test_convergence_with_decrement() {
        let store1 = CounterStore::with_replica_id("r1");
        let store2 = CounterStore::with_replica_id("r2");

        let k = key("shared");

        // r1 increments, r2 decrements
        let (_, delta1) = store1.increment(&k, 10).unwrap();
        store2.apply_delta(&delta1); // r2 sees increment first

        let (_, delta2) = store2.decrement(&k, 3).unwrap();
        store1.apply_delta(&delta2);

        // Both should see 10 - 3 = 7
        assert_eq!(store1.get("shared"), 7);
        assert_eq!(store2.get("shared"), 7);
    }

    #[test]
    fn test_all_deltas() {
        let store = CounterStore::with_replica_id("r1");

        store.increment_str("k1", 10);
        store.decrement_str("k1", 3);
        store.increment_str("k2", 20);

        // Apply delta from another replica
        store.apply_delta(&Delta::from_strs("k1", "r2", 5));

        let deltas = store.all_deltas();

        // Should have: k1/r1/P, k1/r1/N, k1/r2/P, k2/r1/P
        assert_eq!(deltas.len(), 4);
    }

    #[test]
    fn test_key_count_and_keys() {
        let store = CounterStore::with_replica_id("r1");

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
    fn test_exists() {
        let store = CounterStore::with_replica_id("r1");

        assert!(!store.exists("k1"));

        store.increment_str("k1", 1);
        assert!(store.exists("k1"));

        store.set_string_str("k2", "hello".to_string());
        assert!(store.exists("k2"));
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let store = StdArc::new(CounterStore::with_replica_id("r1"));

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

        let store = StdArc::new(CounterStore::with_replica_id("r1"));

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
        let store = CounterStore::with_replica_id("r1");

        store.increment_str("k1", 10);
        store.decrement_str("k1", 3);
        store.increment_str("k2", 20);
        store.apply_delta(&Delta::from_strs("k1", "r2", 5));

        let snapshot = store.export_snapshot();

        assert_eq!(snapshot.replica_id, "r1");
        assert_eq!(snapshot.key_count(), 2);

        let k1 = snapshot.counters.get("k1").unwrap();
        assert_eq!(k1.get("r1"), Some(&10));
        assert_eq!(k1.get("r2"), Some(&5));

        let k1_dec = snapshot.decrements.get("k1").unwrap();
        assert_eq!(k1_dec.get("r1"), Some(&3));

        let k2 = snapshot.counters.get("k2").unwrap();
        assert_eq!(k2.get("r1"), Some(&20));
    }

    #[test]
    fn test_import_snapshot() {
        let store = CounterStore::with_replica_id("r1");

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
        let store = CounterStore::with_replica_id("r1");

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
        let store1 = CounterStore::with_replica_id("r1");

        store1.increment_str("counter:a", 100);
        store1.increment_str("counter:b", 200);
        store1.decrement_str("counter:a", 20);
        store1.apply_delta(&Delta::from_strs("counter:a", "r2", 50));

        // Export
        let snapshot = store1.export_snapshot();
        let json = snapshot.to_json().unwrap();

        // Import into new store
        let store2 = CounterStore::with_replica_id("r2");
        let restored = Snapshot::from_json(&json).unwrap();
        store2.import_snapshot(&restored);

        // Values should match
        assert_eq!(store2.get("counter:a"), store1.get("counter:a"));
        assert_eq!(store2.get("counter:b"), store1.get("counter:b"));
    }

    // === TTL Tests ===

    #[test]
    fn test_expire_and_ttl() {
        let store = CounterStore::with_replica_id("r1");

        // Create a key
        store.increment_str("k1", 10);

        // No TTL initially
        assert_eq!(store.ttl("k1"), -1);
        assert_eq!(store.pttl("k1"), -1);

        // Set TTL (10 seconds) - returns delta
        assert!(store.expire("k1", 10000).is_some());

        // TTL should be positive
        let pttl = store.pttl("k1");
        assert!(pttl > 0 && pttl <= 10000);

        // Non-existent key - returns None
        assert_eq!(store.ttl("nonexistent"), -2);
        assert!(store.expire("nonexistent", 10000).is_none());
    }

    #[test]
    fn test_expire_at() {
        let store = CounterStore::with_replica_id("r1");
        store.increment_str("k1", 10);

        // Set absolute expiration 1 second in the future
        let future_ms = current_time_ms() + 1000;
        assert!(store.expire_at("k1", future_ms).is_some());

        let pttl = store.pttl("k1");
        assert!(pttl > 0 && pttl <= 1000);
    }

    #[test]
    fn test_persist() {
        let store = CounterStore::with_replica_id("r1");
        store.increment_str("k1", 10);

        // Set TTL
        store.expire("k1", 10000);
        assert!(store.pttl("k1") > 0);

        // Persist (remove TTL) - returns delta
        let delta = store.persist("k1");
        assert!(delta.is_some());
        assert_eq!(store.pttl("k1"), -1);

        // Persist on key without TTL still returns delta (for replication)
        assert!(store.persist("k1").is_some());

        // Persist on non-existent key returns None
        assert!(store.persist("nonexistent").is_none());
    }

    #[test]
    fn test_expired_key_returns_zero() {
        let store = CounterStore::with_replica_id("r1");
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
        let store = CounterStore::with_replica_id("r1");
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
        let store = CounterStore::with_replica_id("r1");

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
        let store = CounterStore::with_replica_id("r1");

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
        let store = CounterStore::with_replica_id("r1");

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
        let store = CounterStore::with_replica_id("r1");

        store.increment_str("k1", 10);
        store.expire("k1", 60000); // 60 seconds

        let pttl_before = store.pttl("k1");

        // Increment doesn't change TTL
        store.increment_str("k1", 5);

        let pttl_after = store.pttl("k1");
        assert!(pttl_after <= pttl_before); // Should be same or slightly less
        assert!(pttl_after > 59000); // Still around 60 seconds
    }

    #[test]
    fn test_increment_expired_key_resets_to_zero() {
        let store = CounterStore::with_replica_id("r1");

        // Create key with value 100
        store.increment_str("k1", 100);
        assert_eq!(store.get("k1"), 100);

        // Expire the key
        let past_ms = current_time_ms().saturating_sub(1000);
        store.expire_at("k1", past_ms);

        // GET should return 0 (expired)
        assert_eq!(store.get("k1"), 0);

        // INCR should start fresh from 0, not continue from 100
        let (value, _) = store.increment_str("k1", 5).unwrap();
        assert_eq!(value, 5); // Should be 5, not 105

        // Verify via GET
        assert_eq!(store.get("k1"), 5);
    }

    #[test]
    fn test_decrement_expired_key_resets_to_zero() {
        let store = CounterStore::with_replica_id("r1");

        // Create key with value 100
        store.increment_str("k1", 100);

        // Expire the key
        let past_ms = current_time_ms().saturating_sub(1000);
        store.expire_at("k1", past_ms);

        // DECR should start fresh from 0
        let (value, _) = store.decrement_str("k1", 5).unwrap();
        assert_eq!(value, -5); // Should be -5, not 95

        assert_eq!(store.get("k1"), -5);
    }

    #[test]
    fn test_set_expired_counter_allows_string() {
        let store = CounterStore::with_replica_id("r1");

        // Create counter
        store.increment_str("k1", 100);
        assert_eq!(store.get_type("k1"), Some("counter"));

        // Expire it
        let past_ms = current_time_ms().saturating_sub(1000);
        store.expire_at("k1", past_ms);

        // SET should now work (expired counter is removed)
        assert!(store.set_string_str("k1", "hello".to_string()).is_some());
        assert_eq!(store.get_type("k1"), Some("string"));
        assert_eq!(store.get_string("k1"), Some("hello".to_string()));
    }
}
