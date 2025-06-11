// examples/user-app/src/main.rs - Simple Information Display

use concordance::ConcordanceProvider;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
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
    info!("");

    // Show framework information
    show_framework_info().await?;

    info!("");
    info!("🚀 To get started:");
    info!("   1. Start NATS: docker run -p 4222:4222 nats:latest --jetstream");
    info!("   2. Run worker: cargo run --bin worker");
    info!("   3. Send command: nats pub cc.commands.order '{{\"command_type\":\"create_order\",\"key\":\"order-123\",\"data\":{{\"customer_id\":\"cust-456\",\"total\":99.99,\"items\":[{{\"name\":\"Widget\",\"quantity\":1,\"price\":99.99}}]}}}}'");
    info!("");

    Ok(())
}

async fn show_framework_info() -> Result<(), Box<dyn std::error::Error>> {
    info!("🔧 Framework Information:");
    
    // Show registered aggregates (works without NATS)
    let aggregates = concordance::get_registered_aggregates();
    info!("   Registered Aggregates: {:?}", aggregates);
    
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
            info!("   - This is normal if NATS isn't running");
        }
    }
    
    Ok(())
}