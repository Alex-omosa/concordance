# test_commands.ps1 - PowerShell script to test NATS command publishing

Write-Host "🧪 Testing Concordance NATS Integration" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Check if NATS CLI is available
if (!(Get-Command "nats" -ErrorAction SilentlyContinue)) {
    Write-Host "❌ NATS CLI not found. Please install it first:" -ForegroundColor Red
    Write-Host "   Download from: https://github.com/nats-io/natscli/releases" -ForegroundColor Yellow
    Write-Host "   OR use Chocolatey: choco install nats" -ForegroundColor Yellow
    exit 1
}

# Check NATS connection
Write-Host "🔌 Checking NATS connection..." -ForegroundColor Blue
$natsCheck = nats server check 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ NATS server not available. Please start it first:" -ForegroundColor Red
    Write-Host "   docker run -p 4222:4222 nats:latest --jetstream" -ForegroundColor Yellow
    Write-Host "   OR download and run nats-server from https://nats.io/download/" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ NATS server is running!" -ForegroundColor Green
Write-Host ""

# Test 1: Create Order Command
Write-Host "📦 Test 1: Publishing create_order command..." -ForegroundColor Blue
$createOrderCommand = @"
{ "command_type": "create_order", "key": "order-ps-001", "data": { "customer_id": "customer-ps-456", "total": 599.99, "items": [{ "name": "PowerShell Test Widget", "quantity": 3, "price": 199.99 }] } }
"@

nats pub cc.commands.order $createOrderCommand
Write-Host "✅ create_order command published!" -ForegroundColor Green
Write-Host ""

# Test 2: Confirm Order Command
Write-Host "✅ Test 2: Publishing confirm_order command..." -ForegroundColor Blue
$confirmOrderCommand = @"
{
  "command_type": "confirm_order",
  "key": "order-ps-001", 
  "data": {}
}
"@

nats pub cc.commands.order $confirmOrderCommand
Write-Host "✅ confirm_order command published!" -ForegroundColor Green
Write-Host ""

# Test 3: Listen for Events
Write-Host "📥 Test 3: Listening for events (5 seconds)..." -ForegroundColor Blue
Write-Host "   You should see order_created and order_confirmed events if the worker is running" -ForegroundColor Gray

# Start event listener in background and kill after 5 seconds
$eventJob = Start-Job -ScriptBlock { nats sub "cc.events.*" }
Start-Sleep -Seconds 5
Stop-Job -Job $eventJob -PassThru | Remove-Job
Write-Host "⏰ Event listening timeout reached" -ForegroundColor Gray
Write-Host ""

# Test 4: Stream Info
Write-Host "📊 Test 4: Stream Information" -ForegroundColor Blue
Write-Host "Commands stream:" -ForegroundColor Gray
try {
    nats stream info CC_COMMANDS 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "   ⚠️  Commands stream not found (worker may not be running)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "   ⚠️  Commands stream not found (worker may not be running)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Events stream:" -ForegroundColor Gray
try {
    nats stream info CC_EVENTS 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "   ⚠️  Events stream not found (worker may not be running)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "   ⚠️  Events stream not found (worker may not be running)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🎉 Testing complete!" -ForegroundColor Green
Write-Host ""
Write-Host "💡 To run the worker:" -ForegroundColor Cyan
Write-Host "   cd examples\user-app" -ForegroundColor Gray
Write-Host "   cargo run --bin worker" -ForegroundColor Gray
Write-Host ""
Write-Host "💡 To run the integration test:" -ForegroundColor Cyan
Write-Host "   cd examples\user-app" -ForegroundColor Gray
Write-Host "   cargo run" -ForegroundColor Gray