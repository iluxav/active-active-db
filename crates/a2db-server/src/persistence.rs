use crate::config::{PersistenceConfig, SnapshotFormat};
use a2db_core::{CounterStore, Snapshot, SnapshotError};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

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
    pub fn load_latest(&self) -> Result<bool, PersistenceError> {
        let snapshots = self.list_snapshots()?;

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

    /// Get file extension for the configured format.
    fn file_extension(&self) -> &'static str {
        match self.config.format {
            SnapshotFormat::Json => "json",
            SnapshotFormat::Bincode => "bin",
        }
    }

    /// Save a snapshot to disk.
    pub fn save_snapshot(&self) -> Result<PathBuf, PersistenceError> {
        let snapshot = self.store.export_snapshot();
        let timestamp = snapshot.timestamp.format("%Y%m%dT%H%M%S");
        let ext = self.file_extension();
        let filename = format!("snapshot-{}.{}", timestamp, ext);
        let path = self.data_dir.join(&filename);

        // Write to temp file first for atomic operation
        let temp_path = self.data_dir.join(format!(".{}.tmp", filename));

        // Serialize based on configured format
        let data: Vec<u8> = match self.config.format {
            SnapshotFormat::Json => {
                let json = snapshot
                    .to_json()
                    .map_err(PersistenceError::SnapshotError)?;
                json.into_bytes()
            }
            SnapshotFormat::Bincode => snapshot
                .to_bincode()
                .map_err(PersistenceError::SnapshotError)?,
        };

        // Write to temp file
        {
            let file = File::create(&temp_path).map_err(|e| PersistenceError::IoError {
                path: temp_path.display().to_string(),
                source: e,
            })?;
            let mut writer = BufWriter::new(file);
            writer
                .write_all(&data)
                .map_err(|e| PersistenceError::IoError {
                    path: temp_path.display().to_string(),
                    source: e,
                })?;
            writer.flush().map_err(|e| PersistenceError::IoError {
                path: temp_path.display().to_string(),
                source: e,
            })?;
        }

        // Atomic rename
        fs::rename(&temp_path, &path).map_err(|e| PersistenceError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        debug!(
            "Saved snapshot: {} ({} keys, format: {:?})",
            path.display(),
            snapshot.key_count(),
            self.config.format
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

    /// List all snapshot files, sorted by name (oldest first).
    /// Supports both .json and .bin formats for backwards compatibility.
    fn list_snapshots(&self) -> Result<Vec<PathBuf>, PersistenceError> {
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
                name.starts_with("snapshot-") && (name.ends_with(".json") || name.ends_with(".bin"))
            })
            .map(|entry| entry.path())
            .collect();

        snapshots.sort();
        Ok(snapshots)
    }

    /// Read a snapshot from disk.
    /// Automatically detects format based on file extension.
    fn read_snapshot(&self, path: &Path) -> Result<Snapshot, PersistenceError> {
        let file = File::open(path).map_err(|e| PersistenceError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        let mut reader = BufReader::new(file);

        // Detect format from file extension
        let is_bincode = path.extension().map(|ext| ext == "bin").unwrap_or(false);

        if is_bincode {
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .map_err(|e| PersistenceError::IoError {
                    path: path.display().to_string(),
                    source: e,
                })?;
            Snapshot::from_bincode(&bytes).map_err(PersistenceError::SnapshotError)
        } else {
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

    /// Delete old snapshots, keeping only the most recent N.
    fn cleanup_old_snapshots(&self) -> Result<(), PersistenceError> {
        let snapshots = self.list_snapshots()?;
        let retain = self.config.snapshot_retain_count;

        if snapshots.len() <= retain {
            return Ok(());
        }

        let to_delete = snapshots.len() - retain;
        for path in snapshots.into_iter().take(to_delete) {
            debug!("Deleting old snapshot: {}", path.display());
            fs::remove_file(&path).map_err(|e| PersistenceError::IoError {
                path: path.display().to_string(),
                source: e,
            })?;
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
