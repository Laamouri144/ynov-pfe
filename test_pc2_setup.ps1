# Quick Test Script for PC2
# Run this after cloning the repository to verify setup

Write-Host "`n=== PC2 Environment Verification ===" -ForegroundColor Cyan

# 1. Check Docker
Write-Host "`n[1/10] Checking Docker..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version
    Write-Host "  Docker installed: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "  Docker NOT installed!" -ForegroundColor Red
    exit 1
}

# 2. Check Python
Write-Host "`n[2/10] Checking Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version
    Write-Host "  Python installed: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  Python NOT installed!" -ForegroundColor Red
    exit 1
}

# 3. Check .env file
Write-Host "`n[3/10] Checking environment configuration..." -ForegroundColor Yellow
if (Test-Path .env) {
    Write-Host "  .env file exists" -ForegroundColor Green
    $pc1_ip = (Get-Content .env | Select-String "^PC1_IP=").ToString().Split("=")[1].Trim()
    $pc2_ip = (Get-Content .env | Select-String "^PC2_IP=").ToString().Split("=")[1].Trim()
    Write-Host "  PC1_IP: $pc1_ip" -ForegroundColor White
    Write-Host "  PC2_IP: $pc2_ip" -ForegroundColor White
} else {
    Write-Host "  .env file missing! Copy from .env.example" -ForegroundColor Red
    exit 1
}

# 4. Check required ports
Write-Host "`n[4/10] Checking port availability..." -ForegroundColor Yellow
$ports = @{
    "8123" = "ClickHouse HTTP"
    "9000" = "ClickHouse Native"
    "27017" = "MongoDB"
    "8082" = "Mongo-Express"
}

foreach ($port in $ports.Keys) {
    $connection = Test-NetConnection -ComputerName localhost -Port $port -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
    if ($connection.TcpTestSucceeded) {
        Write-Host "  Port $port ($($ports[$port])): In Use" -ForegroundColor Yellow
    } else {
        Write-Host "  Port $port ($($ports[$port])): Available" -ForegroundColor Green
    }
}

# 5. Start PC2 Docker services
Write-Host "`n[5/10] Starting PC2 Docker services..." -ForegroundColor Yellow
docker-compose --profile pc2 up -d
Start-Sleep -Seconds 10

# 6. Check Docker containers
Write-Host "`n[6/10] Verifying containers..." -ForegroundColor Yellow
$containers = docker-compose ps --format json | ConvertFrom-Json
$expectedContainers = @("clickhouse", "mongodb", "mongo-express")

foreach ($expected in $expectedContainers) {
    $found = $containers | Where-Object { $_.Service -eq $expected }
    if ($found -and $found.State -eq "running") {
        Write-Host "  $expected: Running" -ForegroundColor Green
    } else {
        Write-Host "  $expected: NOT Running" -ForegroundColor Red
    }
}

# 7. Wait for services to be ready
Write-Host "`n[7/10] Waiting for services to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# 8. Test ClickHouse
Write-Host "`n[8/10] Testing ClickHouse connection..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8123/?query=SELECT%201" -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -eq 200) {
        Write-Host "  ClickHouse: OK" -ForegroundColor Green
        Write-Host "  Access: http://localhost:8123" -ForegroundColor White
    }
} catch {
    Write-Host "  ClickHouse: Failed to connect" -ForegroundColor Red
}

# 9. Test MongoDB
Write-Host "`n[9/10] Testing MongoDB connection..." -ForegroundColor Yellow
try {
    $mongoTest = docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')" 2>$null
    if ($mongoTest -match "ok") {
        Write-Host "  MongoDB: OK" -ForegroundColor Green
        Write-Host "  Access: mongodb://localhost:27017" -ForegroundColor White
    }
} catch {
    Write-Host "  MongoDB: Failed to connect" -ForegroundColor Red
}

# 10. Test network connectivity to PC1
Write-Host "`n[10/10] Testing connectivity to PC1..." -ForegroundColor Yellow
if ($pc1_ip -and $pc1_ip -ne "YOUR_PC1_IP") {
    $kafkaTest = Test-NetConnection -ComputerName $pc1_ip -Port 9092 -WarningAction SilentlyContinue
    if ($kafkaTest.TcpTestSucceeded) {
        Write-Host "  PC1 Kafka: Reachable at ${pc1_ip}:9092" -ForegroundColor Green
    } else {
        Write-Host "  PC1 Kafka: Cannot reach ${pc1_ip}:9092" -ForegroundColor Red
        Write-Host "  Check: 1) PC1 is running  2) Firewall rules  3) Network connection" -ForegroundColor Yellow
    }
} else {
    Write-Host "  PC1_IP not configured in .env" -ForegroundColor Yellow
}

# Summary
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
Write-Host "Docker Services: http://localhost:8082 (Mongo-Express: admin/admin)" -ForegroundColor White
Write-Host "ClickHouse HTTP: http://localhost:8123" -ForegroundColor White
Write-Host "MongoDB: mongodb://localhost:27017" -ForegroundColor White
Write-Host "`nNext Steps:" -ForegroundColor Cyan
Write-Host "1. Create Python virtual environment: python -m venv venv" -ForegroundColor White
Write-Host "2. Activate: venv\Scripts\activate" -ForegroundColor White
Write-Host "3. Install dependencies: pip install -r requirements.txt" -ForegroundColor White
Write-Host "4. Run consumer: python scripts\script1_kafka_to_clickhouse.py" -ForegroundColor White
Write-Host ""
