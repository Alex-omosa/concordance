#!/bin/bash

echo "🧪 NATS Test Commands for Concordance User-App"
echo "=============================================="
echo ""

# ============ HAPPY PATH SCENARIOS ============

echo "📦 1. HAPPY PATH: Complete Order Lifecycle"
echo "==========================================="

# 1.1 Create a new order
echo "Creating order-001..."
nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-001", 
  "data": {
    "customer_id": "customer-123",
    "total": 299.99,
    "items": [
      {"name": "Gaming Laptop", "quantity": 1, "price": 299.99}
    ]
  }
}'

sleep 2

# 1.2 Confirm the order
echo "Confirming order-001..."
nats pub cc.commands.order '{
  "command_type": "confirm_order",
  "key": "order-001",
  "data": {}
}'

sleep 2

# 1.3 Ship the order
echo "Shipping order-001..."
nats pub cc.commands.order '{
  "command_type": "ship_order", 
  "key": "order-001",
  "data": {
    "tracking_number": "TRK123456789",
    "carrier": "FastShip Express",
    "estimated_delivery": "2025-06-15"
  }
}'

echo ""
echo "📱 2. MOBILE ORDER: Multi-item order"
echo "===================================="

# 2.1 Create multi-item order
nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-mobile-002",
  "data": {
    "customer_id": "mobile-customer-456", 
    "total": 1599.97,
    "items": [
      {"name": "iPhone 15", "quantity": 1, "price": 999.99},
      {"name": "AirPods Pro", "quantity": 1, "price": 249.99},
      {"name": "Phone Case", "quantity": 1, "price": 49.99},
      {"name": "Screen Protector", "quantity": 3, "price": 99.99}
    ]
  }
}'

sleep 2

# 2.2 Confirm mobile order
nats pub cc.commands.order '{
  "command_type": "confirm_order", 
  "key": "order-mobile-002",
  "data": {}
}'

echo ""
echo "🛒 3. BULK ORDER: High-value enterprise order"
echo "============================================"

nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-enterprise-003",
  "data": {
    "customer_id": "enterprise-corp-789",
    "total": 25999.95,
    "items": [
      {"name": "Server Rack", "quantity": 2, "price": 5999.99},
      {"name": "Network Switch", "quantity": 5, "price": 1999.99},
      {"name": "UPS System", "quantity": 3, "price": 1333.33}
    ]
  }
}'

echo ""
echo "❌ 4. ERROR SCENARIOS: Testing business rules"
echo "============================================="

# 4.1 Try to create duplicate order (should fail)
echo "Attempting to create duplicate order-001 (should fail)..."
nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-001",
  "data": {
    "customer_id": "duplicate-customer",
    "total": 99.99,
    "items": [{"name": "Duplicate Item", "quantity": 1, "price": 99.99}]
  }
}'

sleep 2

# 4.2 Try to confirm non-existent order (should fail)
echo "Attempting to confirm non-existent order (should fail)..."
nats pub cc.commands.order '{
  "command_type": "confirm_order",
  "key": "order-nonexistent",
  "data": {}
}'

sleep 2

# 4.3 Try to ship unconfirmed order (should fail)
echo "Attempting to ship unconfirmed order (should fail)..."
nats pub cc.commands.order '{
  "command_type": "ship_order",
  "key": "order-mobile-002", 
  "data": {
    "tracking_number": "INVALID123",
    "carrier": "BadCarrier"
  }
}'

sleep 2

# 4.4 Invalid payload (missing required fields)
echo "Sending invalid create_order (missing customer_id)..."
nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-invalid-004",
  "data": {
    "total": 99.99,
    "items": [{"name": "Item", "quantity": 1, "price": 99.99}]
  }
}'

sleep 2

# 4.5 Invalid payload (wrong total calculation)
echo "Sending create_order with wrong total calculation..."
nats pub cc.commands.order '{
  "command_type": "create_order", 
  "key": "order-wrong-total-005",
  "data": {
    "customer_id": "customer-wrong-math",
    "total": 50.00,
    "items": [
      {"name": "Expensive Item", "quantity": 2, "price": 100.00}
    ]
  }
}'

echo ""
echo "🚫 5. CANCELLATION SCENARIOS"
echo "============================"

# 5.1 Create order to be cancelled
echo "Creating order to be cancelled..."
nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-cancel-006",
  "data": {
    "customer_id": "customer-cancel-test",
    "total": 149.99,
    "items": [{"name": "Cancelable Item", "quantity": 1, "price": 149.99}]
  }
}'

sleep 2

# 5.2 Cancel the order
echo "Cancelling order-cancel-006..."
nats pub cc.commands.order '{
  "command_type": "cancel_order",
  "key": "order-cancel-006",
  "data": {
    "reason": "Customer changed mind",
    "refund_amount": 149.99
  }
}'

sleep 2

# 5.3 Try to cancel already cancelled order (should fail)
echo "Attempting to cancel already cancelled order (should fail)..."
nats pub cc.commands.order '{
  "command_type": "cancel_order",
  "key": "order-cancel-006", 
  "data": {
    "reason": "Double cancellation attempt"
  }
}'

echo ""
echo "🔄 6. RAPID FIRE TEST: Multiple orders quickly"
echo "=============================================="

for i in {10..15}; do
  echo "Creating rapid-fire order-$i..."
  nats pub cc.commands.order "{
    \"command_type\": \"create_order\",
    \"key\": \"order-rapid-$i\",
    \"data\": {
      \"customer_id\": \"rapid-customer-$i\",
      \"total\": $((i * 10)).99,
      \"items\": [
        {\"name\": \"Rapid Item $i\", \"quantity\": 1, \"price\": $((i * 10)).99}
      ]
    }
  }"
  sleep 0.5
done

echo ""
echo "💰 7. SPECIAL CASES: Edge values"
echo "==============================="

# 7.1 Very small order
echo "Creating micro order..."
nats pub cc.commands.order '{
  "command_type": "create_order",
  "key": "order-micro-020",
  "data": {
    "customer_id": "penny-pincher",
    "total": 0.01,
    "items": [{"name": "Penny Item", "quantity": 1, "price": 0.01}]
  }
}'

sleep 1

# 7.2 Large order value
echo "Creating high-value order..."
nats pub cc.commands.order '{
  "command_type": "create_order", 
  "key": "order-luxury-021",
  "data": {
    "customer_id": "high-roller-vip",
    "total": 99999.99,
    "items": [
      {"name": "Diamond Watch", "quantity": 1, "price": 50000.00},
      {"name": "Gold Chain", "quantity": 1, "price": 25000.00},
      {"name": "Platinum Ring", "quantity": 1, "price": 24999.99}
    ]
  }
}'

echo