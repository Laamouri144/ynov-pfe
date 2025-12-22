# PC2 Quick Setup Guide

This guide helps you quickly set up PC2 (Analytics PC) after cloning the repository.

## Prerequisites
- Docker Desktop installed
- Python 3.9+ installed
- Git installed
- Network connectivity to PC1

## Quick Setup Steps

### 1. Clone Repository
```bash
git clone <repository-url>
cd ynov-pfe
```

### 2. Configure Environment
```bash
# Copy environment template
cp .env.example .env
```

**Edit .env file and set:**
```env
PC_ROLE=PC2
PC1_IP=<ask-friend-for-ip>  # Example: 192.168.11.129
PC2_IP=<your-local-ip>      # Find with: ipconfig (Windows) or ifconfig (Linux)
```

**Find your IP on Windows:**
```powershell
ipconfig | Select-String "IPv4"
```

### 3. Start PC2 Services
```bash
# Start ClickHouse and MongoDB
docker-compose --profile pc2 up -d

# Verify services are running
docker-compose ps
```

**Expected output:**
```
NAME              STATUS   PORTS
clickhouse        Up       8123, 9000
mongodb           Up       27017
mongo-express     Up       8082
```

### 4. Verify ClickHouse
```bash
# Test ClickHouse connection
docker exec -it clickhouse clickhouse-client --query "SELECT 1"
```

**Expected output:** `1`

### 5. Verify MongoDB
```bash
# Test MongoDB connection
docker exec -it mongodb mongosh --eval "db.version()"
```

### 6. Setup Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 7. Test Network Connectivity to PC1
```bash
# Test Kafka connection (replace with actual PC1 IP)
Test-NetConnection -ComputerName <PC1_IP> -Port 9092
```

**Expected:** `TcpTestSucceeded : True`

### 8. Verify Services Accessibility

**ClickHouse HTTP Interface:**
```bash
curl http://localhost:8123/ping
```
**Expected:** `Ok.`

**MongoDB Web UI:**
Open browser: `http://localhost:8082`
- Username: `admin`
- Password: `admin`

**ClickHouse Query Interface:**
```bash
docker exec -it clickhouse clickhouse-client
```

## Running the Pipeline

### Script 1: Kafka → ClickHouse (Run First)
```bash
# This consumes data from PC1's Kafka and stores in local ClickHouse
python scripts/script1_kafka_to_clickhouse.py
```

**You should see:**
```
INFO: Connected to Kafka at <PC1_IP>:9092
INFO: Connected to ClickHouse at localhost:8123
INFO: Consuming from topic: airline-delays
INFO: Batch 1: Inserted 1000 records into ClickHouse
```

### Script 2: ClickHouse → MongoDB Cache (Run in New Terminal)
```bash
# Activate venv first
venv\Scripts\activate

# Run caching script
python scripts/script2_clickhouse_to_mongodb.py
```

**You should see:**
```
INFO: Reading aggregated data from ClickHouse
INFO: Cached 245 records to MongoDB
INFO: Next update in 300 seconds
```

### Script 3: ML Training (Run Once Data is Collected)
```bash
python scripts/script3_ml_training.py
```

### Streamlit Dashboard (Real-time Visualization)
```bash
streamlit run scripts/streamlit_dashboard.py
```

**Access dashboard:** `http://localhost:8501`

## Firewall Configuration

**Windows Firewall - Open Required Ports:**
```powershell
# ClickHouse HTTP
New-NetFirewallRule -DisplayName "ClickHouse HTTP" -Direction Inbound -LocalPort 8123 -Protocol TCP -Action Allow

# ClickHouse Native
New-NetFirewallRule -DisplayName "ClickHouse Native" -Direction Inbound -LocalPort 9000 -Protocol TCP -Action Allow

# MongoDB
New-NetFirewallRule -DisplayName "MongoDB" -Direction Inbound -LocalPort 27017 -Protocol TCP -Action Allow
```

## Troubleshooting

### Problem: Cannot connect to PC1 Kafka
**Solution:**
1. Verify PC1 IP in `.env` is correct
2. Check PC1 has Kafka running: ask friend to run `docker ps`
3. Test connectivity: `ping <PC1_IP>`
4. Ensure PC1 firewall allows port 9092

### Problem: ClickHouse not starting
**Solution:**
```bash
# Check logs
docker logs clickhouse

# Restart service
docker-compose --profile pc2 restart clickhouse
```

### Problem: Script 1 shows connection errors
**Solution:**
1. Ensure PC1 is running Kafka: `docker ps` on PC1
2. Verify `.env` has correct PC1_IP
3. Check Kafka topic exists on PC1:
   ```bash
   docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

### Problem: MongoDB authentication issues
**Solution:**
```bash
# MongoDB in this setup has no authentication
# Check connection string in .env:
MONGODB_USER=
MONGODB_PASSWORD=
```

### Problem: Port conflicts
**Solution:**
```bash
# Check what's using the port (example for 8123)
netstat -ano | findstr :8123

# Stop conflicting service or change port in docker-compose.yml
```

## Verification Checklist

Before running the full pipeline, verify:

- [ ] ClickHouse accessible: `curl http://localhost:8123/ping` returns `Ok.`
- [ ] MongoDB accessible: `docker exec -it mongodb mongosh --eval "db.version()"`
- [ ] Mongo Express UI loads: `http://localhost:8082`
- [ ] PC1 Kafka reachable: `Test-NetConnection <PC1_IP> -Port 9092`
- [ ] Python environment active: `python --version` shows 3.9+
- [ ] Dependencies installed: `pip list | Select-String "confluent-kafka|clickhouse"`
- [ ] `.env` configured with correct IPs

## Monitoring

### Check Container Status
```bash
docker-compose --profile pc2 ps
```

### View Logs
```bash
# ClickHouse logs
docker logs -f clickhouse

# MongoDB logs
docker logs -f mongodb

# All services
docker-compose --profile pc2 logs -f
```

### Check ClickHouse Data
```bash
docker exec -it clickhouse clickhouse-client

# Query record count
SELECT COUNT(*) FROM airline_data.flights;

# View recent records
SELECT * FROM airline_data.flights ORDER BY ingestion_timestamp DESC LIMIT 10;
```

### Check MongoDB Cache
```bash
docker exec -it mongodb mongosh

use airline_cache
db.aggregated_delays.find().limit(5).pretty()
```

## Performance Tips

1. **ClickHouse Optimization:**
   - Uses MergeTree engine for fast inserts
   - Materialized views for pre-aggregation
   - Columnar storage reduces query time

2. **MongoDB Caching:**
   - Stores pre-aggregated data only
   - Reduces ClickHouse query load
   - 5-minute update interval (configurable)

3. **Resource Allocation:**
   - ClickHouse: 4GB RAM minimum
   - MongoDB: 2GB RAM minimum
   - Docker Desktop: 8GB total recommended

## Next Steps

1. Wait for PC1 to start streaming data
2. Run Script 1 to start consuming
3. Verify data in ClickHouse: `SELECT COUNT(*) FROM airline_data.flights`
4. Run Script 2 for caching
5. Once enough data collected, train ML models with Script 3
6. Launch Streamlit dashboard for real-time monitoring

## Support

- Review [PC2_COMPLETE_GUIDE.md](PC2_COMPLETE_GUIDE.md) for detailed documentation
- Check [README.md](README.md) for architecture overview
- Ask PC1 friend for their IP and confirm Kafka is running

## Quick Commands Reference

```bash
# Start services
docker-compose --profile pc2 up -d

# Stop services
docker-compose --profile pc2 down

# View logs
docker-compose --profile pc2 logs -f

# Restart specific service
docker-compose restart clickhouse

# Check Python scripts
python scripts/script1_kafka_to_clickhouse.py  # Kafka consumer
python scripts/script2_clickhouse_to_mongodb.py  # Cache updater
python scripts/script3_ml_training.py  # ML training
streamlit run scripts/streamlit_dashboard.py  # Dashboard
```
