use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Command-line arguments for the a2db server
#[derive(Parser, Debug)]
#[command(name = "a2db")]
#[command(about = "Active-Active Database Server", long_about = None)]
#[command(version)]
pub struct CliArgs {
    /// Path to TOML configuration file (optional if all required args provided)
    #[arg(short, long, env = "A2DB_CONFIG")]
    pub config: Option<String>,

    /// Unique replica ID for this instance
    #[arg(long, env = "A2DB_REPLICA_ID")]
    pub replica_id: Option<String>,

    /// gRPC client listen address (e.g., 0.0.0.0:9000)
    #[arg(long, env = "A2DB_CLIENT_ADDR")]
    pub client_addr: Option<String>,

    /// Replication listen address (e.g., 0.0.0.0:9001)
    #[arg(long, env = "A2DB_REPLICATION_ADDR")]
    pub replication_addr: Option<String>,

    /// Redis-compatible listen address (e.g., 0.0.0.0:6379)
    #[arg(long, env = "A2DB_REDIS_ADDR")]
    pub redis_addr: Option<String>,

    /// Peer replica addresses for replication (can be specified multiple times)
    #[arg(long = "peer", env = "A2DB_PEERS", value_delimiter = ',')]
    pub peers: Option<Vec<String>>,

    /// Enable persistence
    #[arg(long, env = "A2DB_PERSISTENCE")]
    pub persistence: bool,

    /// Data directory for persistence
    #[arg(long, env = "A2DB_DATA_DIR")]
    pub data_dir: Option<String>,

    /// Snapshot interval in seconds
    #[arg(long, env = "A2DB_SNAPSHOT_INTERVAL")]
    pub snapshot_interval: Option<u64>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "A2DB_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Log format (pretty, json)
    #[arg(long, env = "A2DB_LOG_FORMAT", default_value = "pretty")]
    pub log_format: String,
}

/// Server configuration loaded from TOML file
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub identity: IdentityConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub expiration: ExpirationConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Address to listen on for gRPC client connections (e.g., "0.0.0.0:9000")
    pub client_listen_addr: String,
    /// Address to listen on for replication connections (e.g., "0.0.0.0:9001")
    pub replication_listen_addr: String,
    /// Address to listen on for Redis-compatible connections (e.g., "0.0.0.0:6379")
    #[serde(default)]
    pub redis_listen_addr: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentityConfig {
    /// Stable replica ID - must be unique across all replicas
    pub replica_id: String,
    /// Optional path to persist replica ID for auto-generation
    #[serde(default)]
    #[allow(dead_code)]
    pub identity_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplicationConfig {
    /// Peer replicas to connect to
    #[serde(default)]
    pub peers: Vec<String>,
    /// How often to send batched deltas in milliseconds
    #[serde(default = "default_batch_interval")]
    #[allow(dead_code)]
    pub batch_interval_ms: u64,
    /// Maximum deltas per batch
    #[serde(default = "default_max_batch_size")]
    #[allow(dead_code)]
    pub max_batch_size: usize,
    /// Anti-entropy full sync interval in seconds (0 to disable)
    #[serde(default)]
    #[allow(dead_code)]
    pub anti_entropy_interval_s: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            batch_interval_ms: default_batch_interval(),
            max_batch_size: default_max_batch_size(),
            anti_entropy_interval_s: 0,
        }
    }
}

fn default_batch_interval() -> u64 {
    50
}

fn default_max_batch_size() -> usize {
    1000
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Log format: "json" or "pretty"
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

/// Snapshot format for persistence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotFormat {
    /// JSON format - human readable, larger files
    Json,
    /// Bincode format - compact binary, faster (default)
    #[default]
    Bincode,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceConfig {
    /// Enable/disable persistence
    #[serde(default)]
    pub enabled: bool,
    /// Directory for snapshot files
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Snapshot interval in seconds
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval_s: u64,
    /// Keep N old snapshots for safety
    #[serde(default = "default_snapshot_retain_count")]
    pub snapshot_retain_count: usize,
    /// Snapshot format: "json" or "bincode" (default: bincode)
    #[serde(default)]
    pub format: SnapshotFormat,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_dir: default_data_dir(),
            snapshot_interval_s: default_snapshot_interval(),
            snapshot_retain_count: default_snapshot_retain_count(),
            format: SnapshotFormat::default(),
        }
    }
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_snapshot_interval() -> u64 {
    5
}

fn default_snapshot_retain_count() -> usize {
    3
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExpirationConfig {
    /// Enable/disable expiration cleanup
    #[serde(default = "default_expiration_enabled")]
    pub enabled: bool,
    /// How often to run cleanup in seconds
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_s: u64,
    /// Maximum keys to clean per batch
    #[serde(default = "default_cleanup_batch_size")]
    pub cleanup_batch_size: usize,
    /// Grace period before physical deletion in seconds
    #[serde(default = "default_grace_period")]
    pub grace_period_s: u64,
}

impl Default for ExpirationConfig {
    fn default() -> Self {
        Self {
            enabled: default_expiration_enabled(),
            cleanup_interval_s: default_cleanup_interval(),
            cleanup_batch_size: default_cleanup_batch_size(),
            grace_period_s: default_grace_period(),
        }
    }
}

fn default_expiration_enabled() -> bool {
    true
}

fn default_cleanup_interval() -> u64 {
    10
}

fn default_cleanup_batch_size() -> usize {
    10000
}

fn default_grace_period() -> u64 {
    5
}

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(&path).map_err(|e| ConfigError::Read {
            path: path.as_ref().display().to_string(),
            source: e,
        })?;

        toml::from_str(&content).map_err(|e| ConfigError::Parse {
            path: path.as_ref().display().to_string(),
            source: e,
        })
    }

    /// Create configuration from CLI arguments only (no config file)
    pub fn from_cli(args: &CliArgs) -> Result<Self, ConfigError> {
        let replica_id = args.replica_id.clone().ok_or_else(|| {
            ConfigError::Validation("--replica-id is required when no config file is provided".to_string())
        })?;

        let client_listen_addr = args.client_addr.clone().ok_or_else(|| {
            ConfigError::Validation("--client-addr is required when no config file is provided".to_string())
        })?;

        let replication_listen_addr = args.replication_addr.clone().ok_or_else(|| {
            ConfigError::Validation("--replication-addr is required when no config file is provided".to_string())
        })?;

        Ok(Self {
            server: ServerConfig {
                client_listen_addr,
                replication_listen_addr,
                redis_listen_addr: args.redis_addr.clone(),
            },
            identity: IdentityConfig {
                replica_id,
                identity_file: None,
            },
            replication: ReplicationConfig {
                peers: args.peers.clone().unwrap_or_default(),
                ..Default::default()
            },
            persistence: PersistenceConfig {
                enabled: args.persistence,
                data_dir: args.data_dir.clone().unwrap_or_else(default_data_dir),
                snapshot_interval_s: args.snapshot_interval.unwrap_or_else(default_snapshot_interval),
                ..Default::default()
            },
            expiration: ExpirationConfig::default(),
            logging: LoggingConfig {
                level: args.log_level.clone(),
                format: args.log_format.clone(),
            },
        })
    }

    /// Apply CLI argument overrides to an existing config
    pub fn apply_cli_overrides(&mut self, args: &CliArgs) {
        // Server overrides
        if let Some(ref addr) = args.client_addr {
            self.server.client_listen_addr = addr.clone();
        }
        if let Some(ref addr) = args.replication_addr {
            self.server.replication_listen_addr = addr.clone();
        }
        if let Some(ref addr) = args.redis_addr {
            self.server.redis_listen_addr = Some(addr.clone());
        }

        // Identity overrides
        if let Some(ref id) = args.replica_id {
            self.identity.replica_id = id.clone();
        }

        // Replication overrides
        if let Some(ref peers) = args.peers {
            self.replication.peers = peers.clone();
        }

        // Persistence overrides
        if args.persistence {
            self.persistence.enabled = true;
        }
        if let Some(ref dir) = args.data_dir {
            self.persistence.data_dir = dir.clone();
        }
        if let Some(interval) = args.snapshot_interval {
            self.persistence.snapshot_interval_s = interval;
        }

        // Logging overrides (always applied since they have defaults)
        self.logging.level = args.log_level.clone();
        self.logging.format = args.log_format.clone();
    }

    /// Load config from file if provided, or create from CLI args, then apply overrides
    pub fn load(args: &CliArgs) -> Result<Self, ConfigError> {
        let mut config = if let Some(ref config_path) = args.config {
            // Load from file
            Self::from_file(config_path)?
        } else {
            // Try to create from CLI args only
            Self::from_cli(args)?
        };

        // Apply any CLI overrides on top of file config
        if args.config.is_some() {
            config.apply_cli_overrides(args);
        }

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.identity.replica_id.is_empty() {
            return Err(ConfigError::Validation(
                "replica_id cannot be empty".to_string(),
            ));
        }

        if self.server.client_listen_addr.is_empty() {
            return Err(ConfigError::Validation(
                "client_listen_addr cannot be empty".to_string(),
            ));
        }

        if self.server.replication_listen_addr.is_empty() {
            return Err(ConfigError::Validation(
                "replication_listen_addr cannot be empty".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Read {
        path: String,
        source: std::io::Error,
    },
    Parse {
        path: String,
        source: toml::de::Error,
    },
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Read { path, source } => {
                write!(f, "Failed to read config file '{}': {}", path, source)
            }
            ConfigError::Parse { path, source } => {
                write!(f, "Failed to parse config file '{}': {}", path, source)
            }
            ConfigError::Validation(msg) => {
                write!(f, "Configuration validation failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
            [server]
            client_listen_addr = "0.0.0.0:9000"
            replication_listen_addr = "0.0.0.0:9001"

            [identity]
            replica_id = "replica-1"
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.identity.replica_id, "replica-1");
        assert_eq!(config.server.client_listen_addr, "0.0.0.0:9000");
        assert!(config.replication.peers.is_empty());
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
            [server]
            client_listen_addr = "0.0.0.0:9000"
            replication_listen_addr = "0.0.0.0:9001"

            [identity]
            replica_id = "us-west-2-replica-1"
            identity_file = "/var/lib/a2db/identity"

            [replication]
            peers = ["https://eu-west-1:9001", "https://us-east-1:9001"]
            batch_interval_ms = 100
            max_batch_size = 500
            anti_entropy_interval_s = 300

            [logging]
            level = "debug"
            format = "json"
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.replication.peers.len(), 2);
        assert_eq!(config.replication.batch_interval_ms, 100);
        assert_eq!(config.logging.level, "debug");
    }

    #[test]
    fn test_validate_empty_replica_id() {
        let config = Config {
            server: ServerConfig {
                client_listen_addr: "0.0.0.0:9000".into(),
                replication_listen_addr: "0.0.0.0:9001".into(),
                redis_listen_addr: None,
            },
            identity: IdentityConfig {
                replica_id: "".into(),
                identity_file: None,
            },
            replication: ReplicationConfig::default(),
            persistence: PersistenceConfig::default(),
            expiration: ExpirationConfig::default(),
            logging: LoggingConfig::default(),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_parse_persistence_config() {
        let toml = r#"
            [server]
            client_listen_addr = "0.0.0.0:9000"
            replication_listen_addr = "0.0.0.0:9001"

            [identity]
            replica_id = "replica-1"

            [persistence]
            enabled = true
            data_dir = "/var/lib/a2db/data"
            snapshot_interval_s = 10
            snapshot_retain_count = 5
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.persistence.enabled);
        assert_eq!(config.persistence.data_dir, "/var/lib/a2db/data");
        assert_eq!(config.persistence.snapshot_interval_s, 10);
        assert_eq!(config.persistence.snapshot_retain_count, 5);
    }
}
