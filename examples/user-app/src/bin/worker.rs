// examples/user-app/src/bin/worker.rs - Updated with observability

use user_app::*;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging with JSON output
    concordance::observability::init_tracing();

    tracing::info!(
        service.name = "concordance-worker",
        service.version = env!("CARGO_PKG_VERSION"),
        "Starting Concordance Production Worker"
    );

    tracing::info!(
        aggregates.registered = ?get_registered_aggregates(),
        aggregates.count = get_registered_aggregates().len(),
        "Registered aggregates discovered"
    );

    // Create provider
    let app = match ConcordanceProvider::new().await {
        Ok(provider) => {
            tracing::info!("Concordance provider initialized successfully");
            provider
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "Failed to initialize Concordance provider"
            );
            return Err(e);
        }
    };

    // Health check with structured logging
    match app.health_check().await {
        Ok(health) => {
            tracing::info!(
                health.nats = ?health.nats,
                health.jetstream = ?health.jetstream,
                health.state_store = ?health.state_store,
                health.aggregates = ?health.registered_aggregates,
                "System health check completed"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Health check encountered issues"
            );
        }
    }

    // Start workers
    let worker_handles = app.start_workers().await?;
    
    tracing::info!(
        workers.count = worker_handles.len(),
        nats.subjects.commands = "cc.commands.*",
        nats.subjects.events = "cc.events.*",
        nats.kv.bucket = "CC_STATE",
        "All workers started successfully"
    );

    // Create a shutdown handler
    let shutdown = tokio::signal::ctrl_c();
    
    tokio::select! {
        _ = futures::future::try_join_all(worker_handles) => {
            tracing::warn!("All workers have stopped");
        }
        _ = shutdown => {
            tracing::info!("Received shutdown signal");
        }
    }

    tracing::info!("Worker shutting down gracefully");
    Ok(())
}