// examples/user-app/src/events.rs - Event Application Logic

use crate::{OrderAggregate, OrderStatus, OrderItem};
use concordance::{Event, WorkError};
use serde::{Deserialize, Serialize};

// ============ EVENT PAYLOADS ============

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderCreatedEvent {
    pub order_id: String,
    pub customer_id: String,
    pub total: f64,
    pub items: Vec<OrderItem>,
    pub version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderConfirmedEvent {
    pub order_id: String,
    pub version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderShippedEvent {
    pub order_id: String,
    pub tracking_number: String,
    pub carrier: String,
    pub estimated_delivery: Option<String>,
    pub version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderDeliveredEvent {
    pub order_id: String,
    pub delivery_date: String,
    pub delivered_to: String,
    pub version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderCancelledEvent {
    pub order_id: String,
    pub reason: String,
    pub refund_amount: Option<f64>,
    pub version: u32,
}

// ============ EVENT HANDLERS ============

pub struct OrderEvents;

    /// Apply order_created event
    pub fn apply_order_created(
        aggregate: &mut OrderAggregate,
        event: &Event,
    ) -> Result<(), WorkError> {
        let event_data: OrderCreatedEvent = serde_json::from_slice(&event.payload)
            .map_err(|e| WorkError::Other(format!("Invalid order_created event payload: {}", e)))?;

        // Validate event data
        validate_order_created(&event_data, aggregate)?;

        // Apply state changes
        aggregate.customer_id = Some(event_data.customer_id);
        aggregate.total = event_data.total;
        aggregate.items = event_data.items;
        aggregate.status = OrderStatus::Created;
        aggregate.version = event_data.version;

        tracing::info!(
            "Order {} created with total ${:.2} for customer {:?}",
            aggregate.key,
            aggregate.total,
            aggregate.customer_id
        );

        Ok(())
    }

    /// Apply order_confirmed event
    pub fn apply_order_confirmed(
        aggregate: &mut OrderAggregate,
        event: &Event,
    ) -> Result<(), WorkError> {
        let event_data: OrderConfirmedEvent = serde_json::from_slice(&event.payload)
            .map_err(|e| WorkError::Other(format!("Invalid order_confirmed event payload: {}", e)))?;

        // Validate event data
       validate_order_confirmed(&event_data, aggregate)?;

        // Apply state changes
        aggregate.status = OrderStatus::Confirmed;
        aggregate.version = event_data.version;

        tracing::info!("Order {} confirmed", aggregate.key);

        Ok(())
    }

    /// Apply order_shipped event
    pub fn apply_order_shipped(
        aggregate: &mut OrderAggregate,
        event: &Event,
    ) -> Result<(), WorkError> {
        let event_data: OrderShippedEvent = serde_json::from_slice(&event.payload)
            .map_err(|e| WorkError::Other(format!("Invalid order_shipped event payload: {}", e)))?;

        // Validate event data
       validate_order_shipped(&event_data, aggregate)?;

        // Apply state changes
        aggregate.status = OrderStatus::Shipped;
        aggregate.version = event_data.version;

        tracing::info!(
            "Order {} shipped via {} with tracking {}",
            aggregate.key,
            event_data.carrier,
            event_data.tracking_number
        );

        Ok(())
    }

    /// Apply order_delivered event
    pub fn apply_order_delivered(
        aggregate: &mut OrderAggregate,
        event: &Event,
    ) -> Result<(), WorkError> {
        let event_data: OrderDeliveredEvent = serde_json::from_slice(&event.payload)
            .map_err(|e| WorkError::Other(format!("Invalid order_delivered event payload: {}", e)))?;

        // Validate event data
       validate_order_delivered(&event_data, aggregate)?;

        // Apply state changes
        aggregate.status = OrderStatus::Delivered;
        aggregate.version = event_data.version;

        tracing::info!(
            "Order {} delivered on {} to {}",
            aggregate.key,
            event_data.delivery_date,
            event_data.delivered_to
        );

        Ok(())
    }

    /// Apply order_cancelled event
    pub fn apply_order_cancelled(
        aggregate: &mut OrderAggregate,
        event: &Event,
    ) -> Result<(), WorkError> {
        let event_data: OrderCancelledEvent = serde_json::from_slice(&event.payload)
            .map_err(|e| WorkError::Other(format!("Invalid order_cancelled event payload: {}", e)))?;

        // Validate event data
       validate_order_cancelled(&event_data, aggregate)?;

        // Apply state changes
        aggregate.status = OrderStatus::Cancelled;
        aggregate.version = event_data.version;

        tracing::info!(
            "Order {} cancelled: {} (refund: ${:.2})",
            aggregate.key,
            event_data.reason,
            event_data.refund_amount.unwrap_or(0.0)
        );

        Ok(())
    }

    // ============ VALIDATION HELPERS ============

    fn validate_order_created(
        event_data: &OrderCreatedEvent,
        aggregate: &OrderAggregate,
    ) -> Result<(), WorkError> {
        if event_data.order_id != aggregate.key {
            return Err(WorkError::Other(format!(
                "Event order_id {} doesn't match aggregate key {}",
                event_data.order_id, aggregate.key
            )));
        }

        if event_data.version != aggregate.version + 1 {
            return Err(WorkError::Other(format!(
                "Version mismatch: expected {}, got {}",
                aggregate.version + 1,
                event_data.version
            )));
        }

        Ok(())
    }

    fn validate_order_confirmed(
        event_data: &OrderConfirmedEvent,
        aggregate: &OrderAggregate,
    ) -> Result<(), WorkError> {
        if event_data.order_id != aggregate.key {
            return Err(WorkError::Other(format!(
                "Event order_id {} doesn't match aggregate key {}",
                event_data.order_id, aggregate.key
            )));
        }

        if event_data.version != aggregate.version + 1 {
            return Err(WorkError::Other(format!(
                "Version mismatch: expected {}, got {}",
                aggregate.version + 1,
                event_data.version
            )));
        }

        Ok(())
    }

    fn validate_order_shipped(
        event_data: &OrderShippedEvent,
        aggregate: &OrderAggregate,
    ) -> Result<(), WorkError> {
        if event_data.order_id != aggregate.key {
            return Err(WorkError::Other(format!(
                "Event order_id {} doesn't match aggregate key {}",
                event_data.order_id, aggregate.key
            )));
        }

        if event_data.version != aggregate.version + 1 {
            return Err(WorkError::Other(format!(
                "Version mismatch: expected {}, got {}",
                aggregate.version + 1,
                event_data.version
            )));
        }

        Ok(())
    }

    fn validate_order_delivered(
        event_data: &OrderDeliveredEvent,
        aggregate: &OrderAggregate,
    ) -> Result<(), WorkError> {
        if event_data.order_id != aggregate.key {
            return Err(WorkError::Other(format!(
                "Event order_id {} doesn't match aggregate key {}",
                event_data.order_id, aggregate.key
            )));
        }

        if event_data.version != aggregate.version + 1 {
            return Err(WorkError::Other(format!(
                "Version mismatch: expected {}, got {}",
                aggregate.version + 1,
                event_data.version
            )));
        }

        Ok(())
    }

    fn validate_order_cancelled(
        event_data: &OrderCancelledEvent,
        aggregate: &OrderAggregate,
    ) -> Result<(), WorkError> {
        if event_data.order_id != aggregate.key {
            return Err(WorkError::Other(format!(
                "Event order_id {} doesn't match aggregate key {}",
                event_data.order_id, aggregate.key
            )));
        }

        if event_data.version != aggregate.version + 1 {
            return Err(WorkError::Other(format!(
                "Version mismatch: expected {}, got {}",
                aggregate.version + 1,
                event_data.version
            )));
        }

        Ok(())
    }

// ============ EVENT DISPATCHER ============

// Main event dispatcher that routes events to appropriate handlers
// pub fn apply_order_event(
//     aggregate: &mut OrderAggregate,
//     event: &Event,
// ) -> Result<(), WorkError> {
//     match event.event_type.as_str() {
//         "order_created" => OrderEvents::apply_order_created(aggregate, event),
//         "order_confirmed" => OrderEvents::apply_order_confirmed(aggregate, event),
//         "order_shipped" => OrderEvents::apply_order_shipped(aggregate, event),
//         "order_delivered" => OrderEvents::apply_order_delivered(aggregate, event),
//         "order_cancelled" => OrderEvents::apply_order_cancelled(aggregate, event),
//         _ => {
//             tracing::warn!("Unknown event type: {}", event.event_type);
//             Err(WorkError::Other(format!(
//                 "Unknown order event: {}",
//                 event.event_type
//             )))
//         }
//     }
// }