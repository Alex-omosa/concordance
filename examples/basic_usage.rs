// examples/basic_usage.rs
use futures::StreamExt;
use micro_event_sourcing::{CommandConsumer, InterestDeclaration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let nc = async_nats::connect("127.0.0.1:4222").await?;
    let js = async_nats::jetstream::new(nc);

    // Create or get the commands stream
    let stream = js
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "commands".to_string(),
            subjects: vec!["cc.commands.*".to_string()],
            ..Default::default()
        })
        .await?;

    // Create interest declaration for order aggregate
    let interest =
        InterestDeclaration::aggregate_for_commands("order-service", "order", "order_id");

    // Create the command consumer
    let mut consumer = CommandConsumer::try_new(stream, interest).await.unwrap();

    println!("Command consumer started. Waiting for commands...");

    // Process commands from the stream
    while let Some(ackable_cmd_result) = consumer.next().await {
        match ackable_cmd_result {
            Ok(mut ackable_cmd) => {
                // Access the command data via .inner (following inspiration framework pattern)
                println!("Received command: {}", ackable_cmd.inner.command_type);
                println!("For key: {}", ackable_cmd.inner.key);
                println!("Data: {}", ackable_cmd.inner.data);

                // The command has already been sanitized by the Stream implementation
                println!("Sanitized type: {}", ackable_cmd.command_type);

                // Process the command here...
                match ackable_cmd.command_type.as_str() {
                    "create_order" => {
                        println!("Processing order creation...");
                        // Handle order creation logic
                    }
                    "update_order_status" => {
                        println!("Processing order status update...");
                        // Handle order update logic
                    }
                    "test_command" => {
                        println!("Processing test command...");
                        // Handle test command
                    }
                    _ => {
                        println!("Unknown command type: {}", ackable_cmd.command_type);
                    }
                }

                // Acknowledge the message when done processing
                ackable_cmd.ack().await.unwrap();
                println!("Command acknowledged\n");
            }
            Err(e) => {
                println!("Error processing command: {}", e);
                // Could implement retry logic or dead letter queue here
                break; // Exit on error for this example
            }
        }
    }

    Ok(())
}
