use crate::config::{PersistenceConfig, SnapshotFormat};
use a2db_core::{CounterStore, Snapshot, SnapshotError};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Maximum keys per chunk file (keeps each chunk ~10-20MB typically)
const KEYS_PER_CHUNK: usize = 5000;

/// Manages snapshot persistence for the counter store.
pub struct PersistenceManager {
    store: Arc<CounterStore>,
    config: PersistenceConfig,
    data_dir: PathBuf,
}

impl PersistenceManager {
    /// Create a new persistence manager.
    pub fn new(store: Arc<CounterStore>, config: PersistenceConfig) -> Self {
        let data_dir = PathBuf::from(&config.data_dir);
        Self {
            store,
            config,
            data_dir,
        }
    }

    /// Initialize persistence: create data directory if needed.
    pub fn init(&self) -> Result<(), PersistenceError> {
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir).map_err(|e| PersistenceError::IoError {
                path: self.data_dir.display().to_string(),
                source: e,
            })?;
            info!("Created data directory: {}", self.data_dir.display());
        }
        Ok(())
    }

    /// Load the latest snapshot on startup.
    /// Returns true if a snapshot was loaded, false if no snapshot exists.
    /// Supports both single-file and chunked snapshots.
    pub fn load_latest(&self) -> Result<bool, PersistenceError> {
        // First try to find chunked snapshots (new format)
        if let Some(chunks) = self.find_latest_chunks()? {
            if !chunks.is_empty() {
                info!(
                    "Loading {} snapshot chunks from: {}",
                    chunks.len(),
                    chunks[0].display()
                );

                let mut total_keys = 0;
                for (i, chunk_path) in chunks.iter().enumerate() {
                    debug!(
                        "Loading chunk {}/{}: {}",
                        i + 1,
                        chunks.len(),
                        chunk_path.display()
                    );
                    let snapshot = self.read_snapshot(chunk_path)?;
                    total_keys += snapshot.key_count();
                    self.store.import_snapshot(&snapshot);
                    // Memory from this chunk is freed here before loading next
                }

                info!("Loaded {} keys from {} chunks", total_keys, chunks.len());
                return Ok(true);
            }
        }

        // Fall back to single-file snapshots (legacy format)
        let snapshots = self.list_single_snapshots()?;

        if snapshots.is_empty() {
            info!("No existing snapshots found");
            return Ok(false);
        }

        // Snapshots are sorted by name (timestamp), so last is newest
        let latest = snapshots.last().unwrap();
        info!("Loading snapshot: {}", latest.display());

        let snapshot = self.read_snapshot(latest)?;
        self.store.import_snapshot(&snapshot);

        info!(
            "Loaded {} keys from snapshot (replica: {}, timestamp: {})",
            snapshot.key_count(),
            snapshot.replica_id,
            snapshot.timestamp
        );

        Ok(true)
    }

    /// Find all chunks for the latest chunked snapshot
    fn find_latest_chunks(&self) -> Result<Option<Vec<PathBuf>>, PersistenceError> {
        if !self.data_dir.exists() {
            return Ok(None);
        }

        let mut chunk_files: Vec<PathBuf> = fs::read_dir(&self.data_dir)
            .map_err(|e| PersistenceError::IoError {
                path: self.data_dir.display().to_string(),
                source: e,
            })?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                // Match chunked format: snapshot-TIMESTAMP-NNNN.bin.gz
                name.starts_with("snapshot-") && name.contains("-0") && name.ends_with(".bin.gz")
            })
            .map(|entry| entry.path())
            .collect();

        if chunk_files.is_empty() {
            return Ok(None);
        }

        chunk_files.sort();

        // Group by timestamp (extract timestamp from filename)
        // snapshot-20251224T194005-0001.bin.gz -> 20251224T194005
        let latest_timestamp = chunk_files
            .last()
            .and_then(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix("snapshot-"))
                    .and_then(|n| n.split('-').next())
            })
            .map(|s| s.to_string());

        if let Some(timestamp) = latest_timestamp {
            let prefix = format!("snapshot-{}-", timestamp);
            let chunks: Vec<PathBuf> = chunk_files
                .into_iter()
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.starts_with(&prefix))
                        .unwrap_or(false)
                })
                .collect();
            Ok(Some(chunks))
        } else {
            Ok(None)
        }
    }

    /// Get file extension for the configured format.
    fn file_extension(&self) -> &'static str {
        match self.config.format {
            SnapshotFormat::Json => "json",
            SnapshotFormat::Bincode => "bin.gz", // Now compressed by default
        }
    }

    /// Save a snapshot to disk in chunks.
    /// Each chunk contains up to KEYS_PER_CHUNK keys, keeping memory usage low.
    pub fn save_snapshot(&self) -> Result<PathBuf, PersistenceError> {
        let full_snapshot = self.store.export_snapshot();
        let timestamp = full_snapshot.timestamp.format("%Y%m%dT%H%M%S");
        let total_keys = full_snapshot.key_count();

        // If small enough, save as single file (legacy compatible)
        if total_keys <= KEYS_PER_CHUNK {
            return self.save_single_snapshot(&full_snapshot, &timestamp.to_string());
        }

        // Split into chunks
        let replica_id = full_snapshot.replica_id.clone();
        let counter_keys: Vec<_> = full_snapshot.counters.keys().cloned().collect();
        let string_keys: Vec<_> = full_snapshot.strings.keys().cloned().collect();

        let mut chunk_num = 0;
        let mut first_chunk_path = None;

        // Process counter keys in chunks
        for chunk_keys in counter_keys.chunks(KEYS_PER_CHUNK) {
            chunk_num += 1;
            let mut chunk_counters = std::collections::HashMap::new();
            let mut chunk_decrements = std::collections::HashMap::new();
            let mut chunk_expirations = std::collections::HashMap::new();

            for key in chunk_keys {
                if let Some(counter) = full_snapshot.counters.get(key) {
                    chunk_counters.insert(key.clone(), counter.clone());
                }
                if let Some(decrement) = full_snapshot.decrements.get(key) {
                    chunk_decrements.insert(key.clone(), decrement.clone());
                }
                if let Some(exp) = full_snapshot.expirations.get(key) {
                    chunk_expirations.insert(key.clone(), *exp);
                }
            }

            let chunk_snapshot = Snapshot::from_full(
                replica_id.clone(),
                chunk_counters,
                chunk_decrements,
                std::collections::HashMap::new(),
                chunk_expirations,
            );

            let path = self.save_chunk(&chunk_snapshot, &timestamp.to_string(), chunk_num)?;
            if first_chunk_path.is_none() {
                first_chunk_path = Some(path);
            }
        }

        // Process string keys in chunks
        for chunk_keys in string_keys.chunks(KEYS_PER_CHUNK) {
            chunk_num += 1;
            let mut chunk_strings = std::collections::HashMap::new();
            let mut chunk_expirations = std::collections::HashMap::new();

            for key in chunk_keys {
                if let Some(string_val) = full_snapshot.strings.get(key) {
                    chunk_strings.insert(key.clone(), string_val.clone());
                }
                if let Some(exp) = full_snapshot.expirations.get(key) {
                    chunk_expirations.insert(key.clone(), *exp);
                }
            }

            let chunk_snapshot = Snapshot::from_full(
                replica_id.clone(),
                std::collections::HashMap::new(),
                std::collections::HashMap::new(),
                chunk_strings,
                chunk_expirations,
            );

            let path = self.save_chunk(&chunk_snapshot, &timestamp.to_string(), chunk_num)?;
            if first_chunk_path.is_none() {
                first_chunk_path = Some(path);
            }
        }

        info!(
            "Saved snapshot in {} chunks ({} keys total)",
            chunk_num, total_keys
        );

        // Clean up old snapshots
        if let Err(e) = self.cleanup_old_snapshots() {
            warn!("Failed to cleanup old snapshots: {}", e);
        }

        Ok(first_chunk_path.unwrap_or_else(|| self.data_dir.join("snapshot")))
    }

    /// Save a single chunk file
    fn save_chunk(
        &self,
        snapshot: &Snapshot,
        timestamp: &str,
        chunk_num: usize,
    ) -> Result<PathBuf, PersistenceError> {
        let filename = format!("snapshot-{}-{:04}.bin.gz", timestamp, chunk_num);
        let path = self.data_dir.join(&filename);
        let temp_path = self.data_dir.join(format!(".{}.tmp", filename));

        let file = File::create(&temp_path).map_err(|e| PersistenceError::IoError {
            path: temp_path.display().to_string(),
            source: e,
        })?;

        let data = snapshot
            .to_bincode()
            .map_err(PersistenceError::SnapshotError)?;
        let mut encoder = GzEncoder::new(BufWriter::new(file), Compression::fast());
        encoder
            .write_all(&data)
            .map_err(|e| PersistenceError::IoError {
                path: temp_path.display().to_string(),
                source: e,
            })?;
        encoder.finish().map_err(|e| PersistenceError::IoError {
            path: temp_path.display().to_string(),
            source: e,
        })?;

        fs::rename(&temp_path, &path).map_err(|e| PersistenceError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        debug!(
            "Saved chunk {}: {} ({} keys)",
            chunk_num,
            path.display(),
            snapshot.key_count()
        );

        Ok(path)
    }

    /// Save a single snapshot file (for small datasets or legacy format)
    fn save_single_snapshot(
        &self,
        snapshot: &Snapshot,
        timestamp: &str,
    ) -> Result<PathBuf, PersistenceError> {
        let ext = self.file_extension();
        let filename = format!("snapshot-{}.{}", timestamp, ext);
        let path = self.data_dir.join(&filename);
        let temp_path = self.data_dir.join(format!(".{}.tmp", filename));

        let file = File::create(&temp_path).map_err(|e| PersistenceError::IoError {
            path: temp_path.display().to_string(),
            source: e,
        })?;

        match self.config.format {
            SnapshotFormat::Json => {
                let json = snapshot
                    .to_json()
                    .map_err(PersistenceError::SnapshotError)?;
                let mut writer = BufWriter::new(file);
                writer
                    .write_all(json.as_bytes())
                    .map_err(|e| PersistenceError::IoError {
                        path: temp_path.display().to_string(),
                        source: e,
                    })?;
                writer.flush().map_err(|e| PersistenceError::IoError {
                    path: temp_path.display().to_string(),
                    source: e,
                })?;
            }
            SnapshotFormat::Bincode => {
                let data = snapshot
                    .to_bincode()
                    .map_err(PersistenceError::SnapshotError)?;
                let mut encoder = GzEncoder::new(BufWriter::new(file), Compression::fast());
                encoder
                    .write_all(&data)
                    .map_err(|e| PersistenceError::IoError {
                        path: temp_path.display().to_string(),
                        source: e,
                    })?;
                encoder.finish().map_err(|e| PersistenceError::IoError {
                    path: temp_path.display().to_string(),
                    source: e,
                })?;
            }
        }

        fs::rename(&temp_path, &path).map_err(|e| PersistenceError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        debug!(
            "Saved snapshot: {} ({} keys)",
            path.display(),
            snapshot.key_count()
        );

        // Clean up old snapshots
        if let Err(e) = self.cleanup_old_snapshots() {
            warn!("Failed to cleanup old snapshots: {}", e);
        }

        Ok(path)
    }

    /// Run the background snapshot task.
    pub async fn run(&self) {
        let interval = Duration::from_secs(self.config.snapshot_interval_s);
        info!(
            "Starting snapshot task (interval: {}s)",
            self.config.snapshot_interval_s
        );

        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;

            match self.save_snapshot() {
                Ok(path) => {
                    debug!("Snapshot saved: {}", path.display());
                }
                Err(e) => {
                    error!("Failed to save snapshot: {}", e);
                }
            }
        }
    }

    /// List single-file snapshots (not chunked), sorted by name (oldest first).
    /// Supports .json, .bin, and .bin.gz formats for backwards compatibility.
    fn list_single_snapshots(&self) -> Result<Vec<PathBuf>, PersistenceError> {
        if !self.data_dir.exists() {
            return Ok(Vec::new());
        }

        let mut snapshots: Vec<PathBuf> = fs::read_dir(&self.data_dir)
            .map_err(|e| PersistenceError::IoError {
                path: self.data_dir.display().to_string(),
                source: e,
            })?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                // Match single-file snapshots (not chunked)
                // Chunked files have format: snapshot-TIMESTAMP-NNNN.bin.gz
                // Single files have format: snapshot-TIMESTAMP.ext
                name.starts_with("snapshot-")
                    && (name.ends_with(".json") || name.ends_with(".bin"))
                    && !name.contains("-0") // Exclude chunked files
            })
            .map(|entry| entry.path())
            .collect();

        snapshots.sort();
        Ok(snapshots)
    }

    /// List all snapshot files (both single and chunked)
    fn list_all_snapshots(&self) -> Result<Vec<PathBuf>, PersistenceError> {
        if !self.data_dir.exists() {
            return Ok(Vec::new());
        }

        let mut snapshots: Vec<PathBuf> = fs::read_dir(&self.data_dir)
            .map_err(|e| PersistenceError::IoError {
                path: self.data_dir.display().to_string(),
                source: e,
            })?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                name.starts_with("snapshot-")
                    && (name.ends_with(".json")
                        || name.ends_with(".bin")
                        || name.ends_with(".bin.gz"))
            })
            .map(|entry| entry.path())
            .collect();

        snapshots.sort();
        Ok(snapshots)
    }

    /// Read a snapshot from disk.
    /// Automatically detects format based on file extension.
    /// Supports: .json (uncompressed JSON), .bin (uncompressed bincode), .bin.gz (gzip-compressed bincode)
    fn read_snapshot(&self, path: &Path) -> Result<Snapshot, PersistenceError> {
        let file = File::open(path).map_err(|e| PersistenceError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        let filename = path.to_string_lossy();

        if filename.ends_with(".bin.gz") {
            // Gzip-compressed bincode - decompress while reading
            let decoder = GzDecoder::new(BufReader::new(file));
            let mut reader = BufReader::new(decoder);
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .map_err(|e| PersistenceError::IoError {
                    path: path.display().to_string(),
                    source: e,
                })?;
            Snapshot::from_bincode(&bytes).map_err(PersistenceError::SnapshotError)
        } else if filename.ends_with(".bin") {
            // Uncompressed bincode (legacy)
            let mut reader = BufReader::new(file);
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .map_err(|e| PersistenceError::IoError {
                    path: path.display().to_string(),
                    source: e,
                })?;
            Snapshot::from_bincode(&bytes).map_err(PersistenceError::SnapshotError)
        } else {
            // JSON format
            let mut reader = BufReader::new(file);
            let mut json = String::new();
            reader
                .read_to_string(&mut json)
                .map_err(|e| PersistenceError::IoError {
                    path: path.display().to_string(),
                    source: e,
                })?;
            Snapshot::from_json(&json).map_err(PersistenceError::SnapshotError)
        }
    }

    /// Delete old snapshots, keeping only the most recent N snapshot sets.
    /// For chunked snapshots, all chunks of a snapshot set are counted as one.
    fn cleanup_old_snapshots(&self) -> Result<(), PersistenceError> {
        let all_files = self.list_all_snapshots()?;
        if all_files.is_empty() {
            return Ok(());
        }

        // Group files by timestamp
        let mut timestamps: Vec<String> = all_files
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix("snapshot-"))
                    .and_then(|n| {
                        // Extract timestamp: either "TIMESTAMP.ext" or "TIMESTAMP-NNNN.bin.gz"
                        let parts: Vec<&str> = n.split('.').next()?.split('-').collect();
                        if !parts.is_empty() {
                            Some(parts[0].to_string())
                        } else {
                            None
                        }
                    })
            })
            .collect();

        timestamps.sort();
        timestamps.dedup();

        let retain = self.config.snapshot_retain_count;
        if timestamps.len() <= retain {
            return Ok(());
        }

        // Delete snapshots for old timestamps
        let to_delete_count = timestamps.len() - retain;
        let timestamps_to_delete: Vec<_> = timestamps.into_iter().take(to_delete_count).collect();

        for path in all_files {
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                for ts in &timestamps_to_delete {
                    if filename.starts_with(&format!("snapshot-{}", ts)) {
                        debug!("Deleting old snapshot: {}", path.display());
                        fs::remove_file(&path).map_err(|e| PersistenceError::IoError {
                            path: path.display().to_string(),
                            source: e,
                        })?;
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Errors that can occur during persistence operations.
#[derive(Debug)]
pub enum PersistenceError {
    IoError {
        path: String,
        source: std::io::Error,
    },
    SnapshotError(SnapshotError),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistenceError::IoError { path, source } => {
                write!(f, "IO error at '{}': {}", path, source)
            }
            PersistenceError::SnapshotError(e) => write!(f, "Snapshot error: {}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}
