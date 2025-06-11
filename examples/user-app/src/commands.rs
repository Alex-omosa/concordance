// examples/user-app/src/commands.rs - Command Handling Logic

use crate::{events::*, OrderAggregate, OrderItem, OrderStatus};
use concordance::{Event, StatefulCommand, WorkError};
use serde::{Deserialize, Serialize};

// ============ COMMAND PAYLOADS ============

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderPayload {
    pub customer_id: String,
    pub total: f64,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfirmOrderPayload {
    // Could add confirmation details here in the future
    // pub confirmed_by: String,
    // pub confirmation_notes: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShipOrderPayload {
    pub tracking_number: String,
    pub carrier: String,
    pub estimated_delivery: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelOrderPayload {
    pub reason: String,
    pub refund_amount: Option<f64>,
}

// ============ COMMAND HANDLERS ============

/// Handle create_order command
pub fn handle_create_order(
    aggregate: &OrderAggregate,
    command: &StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    // Business rule validation
    if aggregate.status != OrderStatus::New {
        return Err(WorkError::Other("Order already exists".to_string()));
    }

    // Parse command payload
    let payload: CreateOrderPayload = serde_json::from_slice(&command.payload)
        .map_err(|e| WorkError::Other(format!("Invalid create_order payload: {}", e)))?;

    // Validate business rules
    validate_create_order(&payload)?;

    // Create event data
    let event_data = OrderCreatedEvent {
        order_id: aggregate.key.clone(),
        customer_id: payload.customer_id,
        total: payload.total,
        items: payload.items,
        version: aggregate.version + 1,
    };

    // Return event
    Ok(vec![Event {
        event_type: "order_created".to_string(),
        payload: serde_json::to_vec(&event_data)
            .map_err(|e| WorkError::Other(format!("Failed to serialize event: {}", e)))?,
        stream: "orders".to_string(),
    }])
}

/// Handle confirm_order command
pub fn handle_confirm_order(
    aggregate: &OrderAggregate,
    command: &StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    // Business rule validation
    if aggregate.status != OrderStatus::Created {
        return Err(WorkError::Other("Order must be created first".to_string()));
    }

    // Parse command payload (even if empty, for future extensibility)
    let _payload: ConfirmOrderPayload =
        serde_json::from_slice(&command.payload).unwrap_or(ConfirmOrderPayload {});

    // Validate business rules
    validate_confirm_order(aggregate)?;

    // Create event data
    let event_data = OrderConfirmedEvent {
        order_id: aggregate.key.clone(),
        version: aggregate.version + 1,
    };

    // Return event
    Ok(vec![Event {
        event_type: "order_confirmed".to_string(),
        payload: serde_json::to_vec(&event_data)
            .map_err(|e| WorkError::Other(format!("Failed to serialize event: {}", e)))?,
        stream: "orders".to_string(),
    }])
}

/// Handle ship_order command
pub fn handle_ship_order(
    aggregate: &OrderAggregate,
    command: &StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    // Business rule validation
    if aggregate.status != OrderStatus::Confirmed {
        return Err(WorkError::Other(
            "Order must be confirmed before shipping".to_string(),
        ));
    }

    // Parse command payload
    let payload: ShipOrderPayload = serde_json::from_slice(&command.payload)
        .map_err(|e| WorkError::Other(format!("Invalid ship_order payload: {}", e)))?;

    // Validate business rules
    validate_ship_order(&payload)?;

    // Create event data
    let event_data = OrderShippedEvent {
        order_id: aggregate.key.clone(),
        tracking_number: payload.tracking_number,
        carrier: payload.carrier,
        estimated_delivery: payload.estimated_delivery,
        version: aggregate.version + 1,
    };

    // Return event
    Ok(vec![Event {
        event_type: "order_shipped".to_string(),
        payload: serde_json::to_vec(&event_data)
            .map_err(|e| WorkError::Other(format!("Failed to serialize event: {}", e)))?,
        stream: "orders".to_string(),
    }])
}

/// Handle cancel_order command
pub fn handle_cancel_order(
    aggregate: &OrderAggregate,
    command: &StatefulCommand,
) -> Result<Vec<Event>, WorkError> {
    // Business rule validation - can cancel if not shipped or delivered
    match aggregate.status {
        OrderStatus::Shipped | OrderStatus::Delivered => {
            return Err(WorkError::Other(
                "Cannot cancel shipped or delivered orders".to_string(),
            ));
        }
        OrderStatus::Cancelled => {
            return Err(WorkError::Other("Order is already cancelled".to_string()));
        }
        _ => {} // OK to cancel
    }

    // Parse command payload
    let payload: CancelOrderPayload = serde_json::from_slice(&command.payload)
        .map_err(|e| WorkError::Other(format!("Invalid cancel_order payload: {}", e)))?;

    // Validate business rules
    validate_cancel_order(&payload)?;

    // Create event data
    let event_data = OrderCancelledEvent {
        order_id: aggregate.key.clone(),
        reason: payload.reason,
        refund_amount: payload.refund_amount,
        version: aggregate.version + 1,
    };

    // Return event
    Ok(vec![Event {
        event_type: "order_cancelled".to_string(),
        payload: serde_json::to_vec(&event_data)
            .map_err(|e| WorkError::Other(format!("Failed to serialize event: {}", e)))?,
        stream: "orders".to_string(),
    }])
}

// ============ VALIDATION HELPERS ============

fn validate_create_order(payload: &CreateOrderPayload) -> Result<(), WorkError> {
    if payload.customer_id.trim().is_empty() {
        return Err(WorkError::Other("Customer ID is required".to_string()));
    }

    if payload.total <= 0.0 {
        return Err(WorkError::Other("Order total must be positive".to_string()));
    }

    if payload.items.is_empty() {
        return Err(WorkError::Other(
            "Order must have at least one item".to_string(),
        ));
    }

    // Validate that total matches sum of items
    let calculated_total: f64 = payload
        .items
        .iter()
        .map(|item| item.price * item.quantity as f64)
        .sum();

    if (payload.total - calculated_total).abs() > 0.01 {
        return Err(WorkError::Other(format!(
            "Total mismatch: expected {:.2}, got {:.2}",
            calculated_total, payload.total
        )));
    }

    Ok(())
}

fn validate_confirm_order(aggregate: &OrderAggregate) -> Result<(), WorkError> {
    if aggregate.total <= 0.0 {
        return Err(WorkError::Other(
            "Cannot confirm order with zero total".to_string(),
        ));
    }

    if aggregate.items.is_empty() {
        return Err(WorkError::Other(
            "Cannot confirm order with no items".to_string(),
        ));
    }

    Ok(())
}

fn validate_ship_order(payload: &ShipOrderPayload) -> Result<(), WorkError> {
    if payload.tracking_number.trim().is_empty() {
        return Err(WorkError::Other("Tracking number is required".to_string()));
    }

    if payload.carrier.trim().is_empty() {
        return Err(WorkError::Other("Carrier is required".to_string()));
    }

    Ok(())
}

fn validate_cancel_order(payload: &CancelOrderPayload) -> Result<(), WorkError> {
    if payload.reason.trim().is_empty() {
        return Err(WorkError::Other(
            "Cancellation reason is required".to_string(),
        ));
    }

    if let Some(refund) = payload.refund_amount {
        if refund < 0.0 {
            return Err(WorkError::Other(
                "Refund amount cannot be negative".to_string(),
            ));
        }
    }

    Ok(())
}

// ============ COMMAND DISPATCHER ============

// /// Main command dispatcher that routes commands to appropriate handlers
// pub fn handle_order_command(
//     aggregate: &OrderAggregate,
//     command: &StatefulCommand,
// ) -> Result<Vec<Event>, WorkError> {
//     match command.command_type.as_str() {
//         "create_order" => OrderCommands::handle_create_order(aggregate, command),
//         "confirm_order" => OrderCommands::handle_confirm_order(aggregate, command),
//         "ship_order" => OrderCommands::handle_ship_order(aggregate, command),
//         "cancel_order" => OrderCommands::handle_cancel_order(aggregate, command),
//         _ => Err(WorkError::Other(format!(
//             "Unknown order command: {}",
//             command.command_type
//         ))),
//     }
// }
