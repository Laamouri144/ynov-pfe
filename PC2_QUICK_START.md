# PC2 Quick Start Guide

## For Your Friend Setting Up PC2

### Step 1: Get the Code
```bash
# Clone the repository
git clone <repository-url>
cd ynov-pfe
```

### Step 2: Configure Environment
```bash
# Copy the template
cp .env.example .env

# Edit .env with actual IPs
notepad .env
```

**Required changes in .env:**
```ini
# Replace these with actual IPs
PC1_IP=192.168.1.100      # Friend's PC running Kafka
PC2_IP=192.168.1.101      # Your PC running ClickHouse

KAFKA_HOST=${PC1_IP}
CLICKHOUSE_HOST=${PC2_IP}
MONGODB_HOST=${PC2_IP}
```

### Step 3: Run Automated Setup Test
```powershell
# This will:
# - Check prerequisites
# - Start Docker services
# - Test connections
# - Verify everything is ready

powershell -ExecutionPolicy Bypass -File test_pc2_setup.ps1
```

### Step 4: Setup Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate
venv\Scripts\activate

# Install dependencies (takes 5-10 minutes)
pip install -r requirements.txt
```

### Step 5: Access Services

**Mongo Express (MongoDB GUI):**
- URL: http://localhost:8082
- Username: admin
- Password: admin

**ClickHouse:**
- HTTP: http://localhost:8123
- Test query: http://localhost:8123/?query=SELECT%201

### Step 6: Start Data Pipeline

**Terminal 1 - Kafka Consumer (receives data from PC1):**
```bash
python scripts/script1_kafka_to_clickhouse.py
```

**Terminal 2 - Cache Updates (every 5 minutes):**
```bash
python scripts/script2_clickhouse_to_mongodb.py
```

**Terminal 3 - Dashboard (view real-time data):**
```bash
streamlit run scripts/streamlit_dashboard.py
```
Access at: http://localhost:8501

## What Each Script Does

### Script 1: Kafka → ClickHouse
- Connects to PC1's Kafka broker
- Reads airline delay messages
- Inserts data into ClickHouse database
- Runs continuously

### Script 2: ClickHouse → MongoDB
- Aggregates data from ClickHouse every 5 minutes
- Caches in MongoDB for fast dashboard queries
- Updates statistics automatically

### Script 3: ML Training
- Trains delay prediction models
- Compares 3 frameworks: Spark MLlib, Dask-ML, Scikit-learn
- Run manually or schedule daily:
```bash
python scripts/script3_ml_training.py
```

### Streamlit Dashboard
- Real-time visualization
- Shows delays by carrier, airport, time
- Interactive charts and filters

## Troubleshooting

### Can't connect to PC1 Kafka
1. **Check PC1 IP is correct:**
   ```bash
   ping <PC1_IP>
   ```

2. **Test Kafka port:**
   ```bash
   Test-NetConnection -ComputerName <PC1_IP> -Port 9092
   ```

3. **Ask PC1 friend to open firewall:**
   ```powershell
   # On PC1, run:
   New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
   ```

### Docker services won't start
```bash
# Check logs
docker-compose --profile pc2 logs

# Restart
docker-compose --profile pc2 down
docker-compose --profile pc2 up -d
```

### Python package installation fails
```bash
# Update pip first
python -m pip install --upgrade pip

# Install problematic packages individually
pip install pyspark
pip install "dask[complete]"
pip install scikit-learn
```

## Quick Commands Reference

### Docker
```bash
# Start PC2 services
docker-compose --profile pc2 up -d

# Stop PC2 services
docker-compose --profile pc2 down

# View logs
docker-compose --profile pc2 logs -f clickhouse
docker-compose --profile pc2 logs -f mongodb

# Check status
docker-compose ps
```

### Database Access
```bash
# ClickHouse CLI
docker exec -it clickhouse clickhouse-client

# MongoDB CLI
docker exec -it mongodb mongosh

# Query ClickHouse via HTTP
curl "http://localhost:8123/?query=SELECT+count(*)+FROM+airline_data.flights"
```

### Python Scripts
```bash
# Activate environment
venv\Scripts\activate

# Run consumer
python scripts/script1_kafka_to_clickhouse.py

# Run cache updates
python scripts/script2_clickhouse_to_mongodb.py

# Train ML models
python scripts/script3_ml_training.py

# Launch dashboard
streamlit run scripts/streamlit_dashboard.py
```

## Performance Tips

### If ClickHouse is slow:
- Increase batch size in Script 1
- Add more RAM to Docker Desktop
- Use materialized views (already configured)

### If MongoDB is slow:
- Indexes are pre-configured
- Check cache update interval (default: 5 minutes)

### If network is slow:
- Adjust batch sizes in scripts
- Check network latency: `ping <PC1_IP>`

## Support & Documentation

- **Full PC2 Guide:** [PC2_COMPLETE_GUIDE.md](PC2_COMPLETE_GUIDE.md)
- **Setup Checklist:** [PC2_SETUP_CHECKLIST.md](PC2_SETUP_CHECKLIST.md)
- **Project Overview:** [README.md](README.md)

## Verification Checklist

Before starting the pipeline, verify:
- [ ] Docker services running (`docker-compose ps`)
- [ ] ClickHouse accessible (http://localhost:8123)
- [ ] MongoDB accessible (http://localhost:8082)
- [ ] Can ping PC1 (`ping <PC1_IP>`)
- [ ] Python environment activated
- [ ] Dependencies installed (`pip list`)

## You're Ready! 🚀

Once both PCs are set up:
1. PC1 starts: `python scripts/nifi_simulator.py`
2. PC2 starts: `python scripts/script1_kafka_to_clickhouse.py`
3. Watch data flow in real-time!
