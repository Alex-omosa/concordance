#!/bin/bash
# Quick setup commands - copy and paste these one by one

# 1. Create directory structure (from concordance root)
mkdir -p observability/{grafana/{provisioning/{datasources,dashboards},dashboards},logs}

# 2. Go to observability directory
cd observability

# 3. Create docker-compose.yml (copy from artifact above)

# 4. Create loki-config.yaml (copy from artifact above)

# 5. Create tempo-config.yaml (copy from artifact above)

# 6. Create vector.toml (copy from artifact above)

# 7. Create Grafana provisioning files
mkdir -p grafana/provisioning/{datasources,dashboards}

# Copy datasources.yaml content from artifact to:
# grafana/provisioning/datasources/datasources.yaml

# Copy dashboards.yaml content from artifact to:
# grafana/provisioning/dashboards/dashboards.yaml

# 8. Create the dashboard
# Copy the dashboard JSON from artifact to:
# grafana/dashboards/concordance.json

# 9. Start everything
docker-compose up -d

# 10. Check status
docker-compose ps

# 11. View logs (optional)
docker-compose logs -f

# 12. In another terminal, run Concordance with logging
cd ../examples/user-app
RUST_LOG=concordance=debug,user_app=debug cargo run --bin worker 2>&1 | tee ../../observability/logs/concordance.log

# 13. Access Grafana
echo "Open http://localhost:3000 in your browser"
echo "Login: admin / concordance"

# 14. Send test commands (in another terminal)
nats pub cc.commands.order '{"command_type":"create_order","key":"order-001","data":{"customer_id":"cust-123","total":99.99,"items":[{"name":"Widget","quantity":1,"price":99.99}]}}'