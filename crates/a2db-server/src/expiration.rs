use a2db_core::CounterStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

/// Background task that periodically cleans up expired keys.
///
/// This is important for memory management - without cleanup,
/// expired keys would remain in memory indefinitely.
pub async fn run_cleanup_loop(
    store: Arc<CounterStore>,
    interval: Duration,
    batch_size: usize,
    grace_period: Duration,
) {
    info!(
        "Starting expiration cleanup task (interval={:?}, batch_size={}, grace={:?})",
        interval, batch_size, grace_period
    );

    let grace_ms = grace_period.as_millis() as u64;
    let mut ticker = tokio::time::interval(interval);

    // Don't run immediately on startup
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let removed = store.cleanup_expired(batch_size, grace_ms);
        if removed > 0 {
            debug!("Cleaned up {} expired keys", removed);
        }
    }
}
