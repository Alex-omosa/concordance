# Concordance Core

The foundational crate for the Concordance event sourcing framework, providing automatic aggregate registration and zero-overhead command dispatch.

## Overview

Concordance Core solves one of the fundamental challenges in event sourcing frameworks: **how to route commands to the correct aggregate implementation without runtime overhead or manual configuration**.

Most event sourcing frameworks require either:
- Manual registration of aggregates (error-prone, boilerplate)
- Runtime reflection (performance overhead)
- Framework ownership of domain types (coupling)

Concordance Core provides:
- **Automatic registration** via compile-time macros
- **Zero-overhead dispatch** using function pointers
- **External domain types** with no framework coupling
- **Type-safe routing** with compile-time validation

## Key Technical Innovations

### 1. Compile-Time Aggregate Registration

```rust
// User defines their aggregate
#[derive(Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate {
    pub key: String,
    // ... business fields
}

// Framework automatically discovers it at compile time
// No manual registration needed!
```

**How it works:**
- The `#[derive(Aggregate)]` macro generates registration code
- Uses the `inventory` crate for compile-time collection
- All aggregates are registered in a global static registry
- Zero runtime cost - everything resolved at compile time

### 2. Zero-Overhead Command Dispatch

```rust
// Runtime dispatch that feels dynamic but performs like static
let events = dispatch_command("order", key, state, command)?;
```

**The magic:**
- Each aggregate type gets a unique function pointer at compile time
- Function pointers stored in registry alongside names
- Runtime lookup by name, then direct function call
- No boxing, no dynamic dispatch, no virtual tables

### 3. Type-Erased Function Pointers

```rust
// All aggregates get converted to this signature
pub type AggregateHandler = fn(String, Option<Vec<u8>>, StatefulCommand) -> Result<Vec<Event>, WorkError>;

// But each one is generated specifically for its type
pub const fn create_aggregate_handler<T: AggregateImpl>() -> AggregateHandler {
    |key, state, command| {
        let mut aggregate = T::from_state_direct(key, state)?;
        aggregate.handle_command(command)
    }
}
```

## Core Architecture

### Registry System

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   User Code     │    │  Derive Macro    │    │   Registry      │
│                 │    │                  │    │                 │
│ OrderAggregate  │───▶│ Generates        │───▶│ "order" →       │
│ UserAggregate   │    │ Registration     │    │ handler_fn_ptr  │
│ InventoryAgg... │    │ Code             │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Runtime Dispatch                             │
│                                                                 │
│  dispatch_command("order", key, state, cmd)                   │
│    1. Look up "order" in registry                             │
│    2. Get function pointer                                     │
│    3. Call function directly (zero overhead!)                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Command Input
     │
     ▼
┌─────────────────┐
│ dispatch_command│
│ ("order", ...)  │
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Registry Lookup │
│ name → handler  │
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Function Call   │
│ handler(...)    │
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Aggregate Logic │
│ Business Rules  │
└─────────────────┘
     │
     ▼
Event Output
```

## Key Types and Traits

### AggregateImpl Trait

The core trait that all aggregates must implement:

```rust
pub trait AggregateImpl: Sized + Debug + Send + Sync {
    /// Unique identifier for this aggregate type
    const NAME: &'static str;
    
    /// Create/restore aggregate from serialized state
    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError>;
    
    /// Process a command and return resulting events
    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError>;
    
    /// Serialize current aggregate state
    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError>;
}
```

### Core Data Types

```rust
/// Command to be processed by an aggregate
pub struct StatefulCommand {
    pub aggregate: String,      // Target aggregate type
    pub command_type: String,   // Command name (e.g., "create_order")
    pub key: String,           // Aggregate instance ID
    pub state: Option<Vec<u8>>, // Serialized state (optional)
    pub payload: Vec<u8>,      // Command data
}

/// Event produced by aggregate
pub struct Event {
    pub event_type: String,    // Event name (e.g., "order_created")
    pub payload: Vec<u8>,     // Event data
    pub stream: String,       // Event stream name
}

/// Registry entry for each aggregate type
pub struct AggregateDescriptor {
    pub name: &'static str,           // Aggregate name
    pub handler: AggregateHandler,    // Function pointer
}
```

## How Registration Works

### Step 1: Derive Macro Generation

When you write:
```rust
#[derive(Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate { ... }
```

The macro generates:
```rust
// Generated code (simplified)
concordance_core::inventory::submit! {
    concordance_core::AggregateDescriptor {
        name: "order",
        handler: |key, state, command| {
            let mut aggregate = OrderAggregate::from_state_direct(key, state)?;
            aggregate.handle_command(command)
        }
    }
}
```

### Step 2: Compile-Time Collection

The `inventory` crate collects all submissions:
```rust
// This happens at compile time
static REGISTRY: &[AggregateDescriptor] = &[
    AggregateDescriptor { name: "order", handler: order_handler },
    AggregateDescriptor { name: "user", handler: user_handler },
    AggregateDescriptor { name: "inventory", handler: inventory_handler },
    // ... all registered aggregates
];
```

### Step 3: Runtime Dispatch

```rust
pub fn dispatch_command(entity_name: &str, ...) -> Result<Vec<Event>, WorkError> {
    // O(n) lookup, but n is small and known at compile time
    let handler = inventory::iter::<AggregateDescriptor>()
        .find(|desc| desc.name == entity_name)
        .map(|desc| desc.handler)
        .ok_or_else(|| WorkError::Other("Unknown aggregate".into()))?;
    
    // Direct function call - zero overhead!
    handler(key, state, command)
}
```

## Performance Characteristics

### Compile Time
- **Registration**: O(1) per aggregate (static allocation)
- **Validation**: Full type checking of aggregate implementations
- **Code Generation**: Optimized function pointers per type

### Runtime
- **Lookup**: O(n) where n = number of aggregate types (typically < 20)
- **Dispatch**: Direct function call (same as hand-written match statement)
- **Memory**: Zero heap allocation for dispatch
- **CPU**: No virtual dispatch, no boxing, no reflection

### Comparison with Alternatives

| Approach | Compile Time | Runtime Lookup | Runtime Dispatch | Memory |
|----------|--------------|----------------|------------------|---------|
| Manual match | Fast | O(1) | Direct call | Zero |
| HashMap<String, fn> | Fast | O(1) | Direct call | Heap |
| Dynamic dispatch | Fast | O(1) | Virtual call | Heap |
| **Concordance** | Fast | O(n)* | Direct call | Zero |

*n is small and bounded at compile time

## Integration with Derive Macro

The `concordance-derive` crate works in tandem with `concordance-core`:

```rust
// User code
#[derive(Aggregate)]
#[aggregate(name = "order")]
pub struct OrderAggregate { ... }

impl AggregateImpl for OrderAggregate { ... }
```

**Generated expansion:**
```rust
// What the derive macro produces
concordance_core::inventory::submit! {
    concordance_core::create_aggregate_descriptor::<OrderAggregate>()
}
```

**Registry functions:**
```rust
// Helper functions generated by concordance-core
pub const fn create_aggregate_descriptor<T: AggregateImpl>() -> AggregateDescriptor {
    AggregateDescriptor {
        name: T::NAME,
        handler: create_aggregate_handler::<T>(),
    }
}

pub const fn create_aggregate_handler<T: AggregateImpl>() -> AggregateHandler {
    |key: String, state: Option<Vec<u8>>, command: StatefulCommand| {
        let mut aggregate = T::from_state_direct(key, state)?;
        aggregate.handle_command(command)
    }
}
```

## Usage Examples

### Basic Aggregate Implementation

```rust
use concordance_core::{AggregateImpl, StatefulCommand, Event, WorkError};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderAggregate {
    pub key: String,
    pub status: OrderStatus,
    pub total: f64,
}

impl AggregateImpl for OrderAggregate {
    const NAME: &'static str = "order";
    
    fn from_state_direct(key: String, state: Option<Vec<u8>>) -> Result<Self, WorkError> {
        match state {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| WorkError::Other(e.to_string())),
            None => Ok(OrderAggregate {
                key,
                status: OrderStatus::New,
                total: 0.0,
            }),
        }
    }
    
    fn handle_command(&mut self, command: StatefulCommand) -> Result<Vec<Event>, WorkError> {
        match command.command_type.as_str() {
            "create_order" => {
                // Business logic here
                self.status = OrderStatus::Created;
                
                Ok(vec![Event {
                    event_type: "order_created".to_string(),
                    payload: serde_json::to_vec(&self).unwrap(),
                    stream: "orders".to_string(),
                }])
            }
            _ => Err(WorkError::Other("Unknown command".to_string())),
        }
    }
    
    fn to_state(&self) -> Result<Option<Vec<u8>>, WorkError> {
        Ok(Some(serde_json::to_vec(self).unwrap()))
    }
}

// Manual registration (if not using derive macro)
concordance_core::register_aggregate!(OrderAggregate);
```

### Using the Dispatch System

```rust
use concordance_core::{dispatch_command, StatefulCommand};

fn process_command(aggregate_name: &str, command_data: serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let command = StatefulCommand {
        aggregate: aggregate_name.to_string(),
        command_type: "create_order".to_string(),
        key: "order-123".to_string(),
        state: None,
        payload: serde_json::to_vec(&command_data)?,
    };
    
    // This will automatically route to the correct aggregate
    let events = dispatch_command(aggregate_name, "order-123".to_string(), None, command)?;
    
    println!("Generated {} events", events.len());
    for event in events {
        println!("Event: {}", event.event_type);
    }
    
    Ok(())
}
```

### Registry Introspection

```rust
use concordance_core::{get_registered_aggregates, is_aggregate_registered};

fn main() {
    // Check what aggregates are available
    let aggregates = get_registered_aggregates();
    println!("Available aggregates: {:?}", aggregates);
    
    // Check if specific aggregate is registered
    if is_aggregate_registered("order") {
        println!("Order aggregate is available");
    }
    
    // Validate all required aggregates are present
    let required = ["order", "user", "inventory"];
    for name in required {
        assert!(is_aggregate_registered(name), "Missing aggregate: {}", name);
    }
}
```

## Why This Approach is Revolutionary

### Problem with Traditional Frameworks

1. **Manual Registration**
   ```rust
   // Traditional approach - error prone
   let mut registry = HashMap::new();
   registry.insert("order", Box::new(OrderService::new()) as Box<dyn AggregateService>);
   registry.insert("user", Box::new(UserService::new()) as Box<dyn AggregateService>);
   // Easy to forget, typos, maintenance burden
   ```

2. **Runtime Reflection**
   ```rust
   // Some frameworks use string-based dispatch
   let result = invoke_method(aggregate_name, method_name, args); // Slow, unsafe
   ```

3. **Framework Coupling**
   ```rust
   // Domain types owned by framework
   pub struct OrderAggregate extends FrameworkAggregate { ... } // Coupling
   ```

### Concordance Solution

1. **Zero Configuration**
   ```rust
   #[derive(Aggregate)]  // That's it!
   #[aggregate(name = "order")]
   pub struct OrderAggregate { ... }
   ```

2. **Compile-Time Safety**
   ```rust
   // Impossible to have unregistered aggregates
   // Impossible to have name collisions
   // Type errors caught at compile time
   ```

3. **Zero Framework Coupling**
   ```rust
   // Pure domain code
   pub struct OrderAggregate {
       // Your fields, your types, your logic
   }
   ```

## Testing

The crate includes comprehensive tests demonstrating the registration and dispatch system:

```bash
cd concordance-core
cargo test
```

Key test scenarios:
- Aggregate registration verification
- Command dispatch routing
- Error handling for unknown aggregates
- Type safety validation

## Future Enhancements

Potential areas for expansion:
- **State Management Integration**: Automatic state persistence
- **Event Sourcing**: Built-in event store integration
- **Saga Support**: Long-running process coordination
- **Metrics**: Performance monitoring and aggregate statistics
- **Distributed Dispatch**: Multi-node aggregate routing

## Contributing

When adding features to concordance-core:

1. **Maintain Zero Overhead**: Any addition should not impact runtime performance
2. **Preserve Type Safety**: Compile-time validation is a core principle
3. **Keep External Domain Types**: Don't introduce framework coupling
4. **Document Performance**: Include performance characteristics of changes
5. **Test Registration**: Ensure the registry system works with new features

---

## Technical Deep Dive: The Inventory Magic

For those interested in the low-level details of how compile-time registration works:

### The Inventory Crate

```rust
// The inventory crate provides compile-time collection
inventory::collect!(AggregateDescriptor);

// This creates a static registry that gets populated at compile time
// Each `inventory::submit!` call adds to this collection
```

### Linker Integration

The inventory system works by:
1. Each `submit!` creates a static symbol in a special linker section
2. The linker collects all symbols in that section
3. At runtime, `inventory::iter()` walks that section
4. No heap allocation, no registration code needed

### Function Pointer Generation

```rust
// For each aggregate type T, the compiler generates:
const ORDER_HANDLER: AggregateHandler = |key, state, command| {
    let mut aggregate = OrderAggregate::from_state_direct(key, state)?;
    aggregate.handle_command(command)
};

// This is a regular function pointer, not a closure
// Zero runtime cost, maximum performance
```

This approach gives you the flexibility of dynamic dispatch with the performance of static dispatch!
