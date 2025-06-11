// examples/user-app/src/main.rs - Information Display with OpenTelemetry

use concordance::ConcordanceProvider;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize basic logging for info display
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🎯 Concordance Event Sourcing Framework");
    info!("========================================");
    info!("");
    info!("📋 Available Commands:");
    info!("   Production Worker:    cargo run --bin worker");
    info!("   Integration Tests:    cargo test");
    info!("   Framework Info:       cargo run");
    info!("   Start Infrastructure: ./start-concordance.sh");
    info!("");

    // Show framework information
    show_framework_info().await?;

    info!("");
    info!("🔭 Observability Stack (OpenTelemetry + Jaeger):");
    info!("   Start Stack:    ./start-concordance.sh");
    info!("   Jaeger UI:      http://localhost:16686");
    info!("   NATS Monitor:   http://localhost:8222");
    info!("   OTLP Endpoint:  http://localhost:4317 (gRPC) / 4318 (HTTP)");
    info!("");
    info!("🚀 Quick Start:");
    info!("   1. ./start-concordance.sh                    # Start infrastructure");
    info!("   2. cargo run --bin worker                    # Start worker");
    info!("   3. nats pub cc.commands.order '{{...}}'       # Send command");
    info!("   4. Open http://localhost:16686               # View traces");
    info!("");

    Ok(())
}

async fn show_framework_info() -> Result<(), Box<dyn std::error::Error>> {
    info!("🔧 Framework Information:");
    
    // Show registered aggregates (works without NATS)
    let aggregates = concordance::get_registered_aggregates();
    info!("   Registered Aggregates: {:?}", aggregates);
    
    // Show feature status
    #[cfg(feature = "opentelemetry")]
    info!("   OpenTelemetry: ✅ Enabled");
    
    #[cfg(not(feature = "opentelemetry"))]
    info!("   OpenTelemetry: ❌ Disabled (use --features opentelemetry)");
    
    // Try to connect to NATS and show status
    match ConcordanceProvider::new().await {
        Ok(provider) => {
            info!("   NATS Connection: ✅ Connected");
            
            match provider.health_check().await {
                Ok(health) => {
                    info!("   System Health: ✅ Healthy");
                    info!("   - NATS: {:?}", health.nats);
                    info!("   - JetStream: {:?}", health.jetstream);
                    info!("   - State Store: {:?}", health.state_store);
                }
                Err(e) => {
                    info!("   System Health: ⚠️  Some issues detected");
                    info!("   - Error: {}", e);
                }
            }
        }
        Err(e) => {
            info!("   NATS Connection: ❌ Not Available");
            info!("   - Error: {}", e);
            info!("   - Run './start-concordance.sh' to start infrastructure");
        }
    }
    
    Ok(())
}