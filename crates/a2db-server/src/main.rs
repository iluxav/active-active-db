mod client_service;
mod config;
mod expiration;
mod metrics;
mod persistence;
mod redis_protocol;
mod replication_client;
mod replication_service;

use a2db_core::{CounterStore, Delta};
use a2db_proto::counter::v1::counter_service_server::CounterServiceServer;
use a2db_proto::replication::v1::replication_service_server::ReplicationServiceServer;
use clap::Parser;
use client_service::CounterServiceImpl;
use config::{CliArgs, Config};
use metrics::{Metrics, MetricsServer};
use persistence::PersistenceManager;
use replication_client::ReplicationClient;
use replication_service::ReplicationServiceImpl;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let args = CliArgs::parse();

    // Load configuration (from file if provided, or from CLI args)
    let config = Config::load(&args)?;

    // Initialize logging
    init_logging(&config.logging);

    info!(
        replica_id = %config.identity.replica_id,
        "Starting a2db (Active-Active Database)"
    );

    // Create the counter store
    let store = Arc::new(CounterStore::with_replica_id(&config.identity.replica_id));

    // Create metrics
    let metrics = Arc::new(Metrics::new());

    // Start metrics server if enabled
    if config.metrics.enabled {
        if let Some(ref addr) = config.metrics.listen_addr {
            let metrics_server = MetricsServer::new(Arc::clone(&metrics));
            let metrics_addr = addr.clone();
            tokio::spawn(async move {
                if let Err(e) = metrics_server.serve(&metrics_addr).await {
                    error!("Metrics server error: {}", e);
                }
            });
        }
    }

    // Initialize persistence if enabled
    if config.persistence.enabled {
        let persistence = PersistenceManager::new(Arc::clone(&store), config.persistence.clone());

        // Create data directory if needed
        persistence.init().map_err(|e| {
            error!("Failed to initialize persistence: {}", e);
            e
        })?;

        // Load latest snapshot on startup
        match persistence.load_latest() {
            Ok(true) => info!("Loaded snapshot on startup"),
            Ok(false) => info!("No snapshot found, starting fresh"),
            Err(e) => {
                error!("Failed to load snapshot: {}", e);
                // Continue without snapshot - peers will sync via anti-entropy
                info!("Starting fresh, peers will sync via replication");
            }
        }

        // Spawn background snapshot task
        tokio::spawn(async move {
            persistence.run().await;
        });
    }

    // Start expiration cleanup task if enabled
    if config.expiration.enabled {
        let exp_store = Arc::clone(&store);
        let interval = std::time::Duration::from_secs(config.expiration.cleanup_interval_s);
        let batch_size = config.expiration.cleanup_batch_size;
        let grace_period = std::time::Duration::from_secs(config.expiration.grace_period_s);

        tokio::spawn(async move {
            expiration::run_cleanup_loop(exp_store, interval, batch_size, grace_period).await;
        });
    }

    // Create channels for delta distribution
    // mpsc channel for client service -> replication broadcast
    // 100K capacity reduces backpressure at high concurrency
    let (delta_tx, mut delta_rx) = mpsc::channel::<Delta>(100_000);
    // broadcast channel for replication to all peers
    let (broadcast_tx, _) = broadcast::channel::<Delta>(100_000);

    // Spawn task to forward deltas from client service to broadcast
    let broadcast_tx_clone = broadcast_tx.clone();
    tokio::spawn(async move {
        while let Some(delta) = delta_rx.recv().await {
            // Broadcast to all connected peers
            let _ = broadcast_tx_clone.send(delta);
        }
    });

    // Create delta sender for Redis server (shares the same channel)
    let redis_delta_tx = delta_tx.clone();

    // Create client service
    let client_service = CounterServiceImpl::new(Arc::clone(&store), delta_tx);

    // Create replication service
    let replication_service = ReplicationServiceImpl::new(Arc::clone(&store), broadcast_tx.clone());

    // Start Redis-compatible server if configured
    if let Some(redis_addr) = &config.server.redis_listen_addr {
        let redis_server = redis_protocol::RedisServer::new(
            Arc::clone(&store),
            redis_delta_tx,
            Arc::clone(&metrics),
        );
        let redis_addr = redis_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = redis_server.serve(&redis_addr).await {
                error!("Redis server error: {}", e);
            }
        });
        info!(redis_addr = %config.server.redis_listen_addr.as_ref().unwrap(), "Redis-compatible server started");
    }

    // Start replication clients for each peer
    for peer_addr in &config.replication.peers {
        let client = ReplicationClient::new(
            peer_addr.clone(),
            config.identity.replica_id.clone(),
            Arc::clone(&store),
            broadcast_tx.subscribe(),
        );

        tokio::spawn(async move {
            client.run().await;
        });
    }

    // Parse addresses
    let client_addr = config.server.client_listen_addr.parse()?;
    let replication_addr = config.server.replication_listen_addr.parse()?;

    info!(
        client_addr = %config.server.client_listen_addr,
        replication_addr = %config.server.replication_listen_addr,
        "Starting gRPC servers"
    );

    // Start both servers concurrently
    let client_server = Server::builder()
        .add_service(CounterServiceServer::new(client_service))
        .serve(client_addr);

    let replication_server = Server::builder()
        .add_service(ReplicationServiceServer::new(replication_service))
        .serve(replication_addr);

    // Run both servers
    tokio::select! {
        result = client_server => {
            if let Err(e) = result {
                error!("Client server error: {}", e);
            }
        }
        result = replication_server => {
            if let Err(e) = result {
                error!("Replication server error: {}", e);
            }
        }
    }

    Ok(())
}

fn init_logging(config: &config::LoggingConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    match config.format.as_str() {
        "json" => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().json())
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer())
                .init();
        }
    }
}
