# PC2 Setup Checklist

## Prerequisites Verification

### System Requirements
- [ ] Windows 10/11 or Linux
- [ ] Docker Desktop installed
- [ ] Python 3.9+ installed
- [ ] Git installed
- [ ] Network connectivity to PC1

### Ports Available
Check these ports are not in use:
```powershell
# Test ports availability
Test-NetConnection -ComputerName localhost -Port 8123  # ClickHouse HTTP
Test-NetConnection -ComputerName localhost -Port 9000  # ClickHouse Native
Test-NetConnection -ComputerName localhost -Port 27017 # MongoDB
Test-NetConnection -ComputerName localhost -Port 8082  # Mongo-Express
```

## Installation Steps

### 1. Clone Repository
```bash
git clone <repository-url>
cd ynov-pfe
```

### 2. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your IPs
# Example:
# PC1_IP=192.168.1.100  (friend's PC running Kafka)
# PC2_IP=192.168.1.101  (your PC running ClickHouse)
```

### 3. Start Docker Services (PC2 Profile Only)
```bash
# Start ClickHouse and MongoDB
docker-compose --profile pc2 up -d

# Verify containers are running
docker-compose ps
```

Expected output:
```
NAME                COMMAND                  SERVICE             STATUS
clickhouse          "/entrypoint.sh"         clickhouse          Up
mongodb             "docker-entrypoint.s…"   mongodb             Up
mongo-express       "tini -- /docker-ent…"   mongo-express       Up
```

### 4. Verify Database Initialization
```bash
# Check ClickHouse
docker exec clickhouse clickhouse-client --query "SHOW DATABASES"
# Should show: airline_data

# Check MongoDB
docker exec mongodb mongosh --eval "show dbs"
# Should show: airline_cache
```

### 5. Setup Python Environment
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

### 6. Test Network Connectivity to PC1
```bash
# Test Kafka connection (replace with actual PC1 IP)
$pc1_ip = (Get-Content .env | Select-String "PC1_IP").ToString().Split("=")[1].Trim()
Test-NetConnection -ComputerName $pc1_ip -Port 9092

# Should show: TcpTestSucceeded : True
```

## Service Access

### ClickHouse
- **HTTP Interface:** http://localhost:8123
- **Test Query:**
  ```bash
  curl "http://localhost:8123/?query=SELECT%201"
  ```

### MongoDB
- **Connection String:** mongodb://localhost:27017
- **Admin UI:** http://localhost:8082 (user: admin, pass: admin)

### Mongo Express
- **URL:** http://localhost:8082
- **Login:** admin / admin

## Running PC2 Scripts

### Script 1: Kafka to ClickHouse Consumer
**Location:** PC1 or PC2 (flexible - can run on either)
```bash
python scripts/script1_kafka_to_clickhouse.py
```
This reads from Kafka on PC1 and writes to ClickHouse on PC2.

### Script 2: ClickHouse to MongoDB Cache
**Location:** PC2
```bash
python scripts/script2_clickhouse_to_mongodb.py
```
Aggregates data from ClickHouse and caches in MongoDB every 5 minutes.

### Script 3: ML Training
**Location:** PC2
```bash
python scripts/script3_ml_training.py
```
Trains models using Spark MLlib, Dask-ML, and Scikit-learn.

### Streamlit Dashboard
**Location:** PC2
```bash
streamlit run scripts/streamlit_dashboard.py
```
Access at: http://localhost:8501

## Verification Tests

### 1. ClickHouse Connection Test
```bash
python -c "import clickhouse_connect; client = clickhouse_connect.get_client(host='localhost'); print('ClickHouse OK:', client.command('SELECT 1'))"
```

### 2. MongoDB Connection Test
```bash
python -c "from pymongo import MongoClient; client = MongoClient('mongodb://localhost:27017'); print('MongoDB OK:', client.list_database_names())"
```

### 3. Kafka Connection Test (to PC1)
Create test file `test_kafka_connection.py`:
```python
from confluent_kafka import Consumer
from dotenv import load_dotenv
import os

load_dotenv()
pc1_ip = os.getenv('PC1_IP', 'localhost')

consumer = Consumer({
    'bootstrap.servers': f'{pc1_ip}:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
})

topics = consumer.list_topics(timeout=5)
print(f"Connected to Kafka on {pc1_ip}")
print(f"Available topics: {topics.topics.keys()}")
consumer.close()
```

Run:
```bash
python test_kafka_connection.py
```

## Troubleshooting

### Docker Services Won't Start
```bash
# Check logs
docker-compose --profile pc2 logs clickhouse
docker-compose --profile pc2 logs mongodb

# Restart services
docker-compose --profile pc2 down
docker-compose --profile pc2 up -d
```

### Cannot Connect to PC1 Kafka
1. Check firewall on PC1:
   ```bash
   # On PC1 (Windows)
   New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
   ```

2. Verify PC1 IP in .env is correct
3. Test basic connectivity: `ping <PC1_IP>`
4. Test port: `Test-NetConnection -ComputerName <PC1_IP> -Port 9092`

### ClickHouse Permission Denied
```bash
# Reset ClickHouse data
docker-compose --profile pc2 down -v
docker-compose --profile pc2 up -d
```

### Python Dependencies Installation Fails
```bash
# For Spark/Dask issues, install system dependencies first
# Windows: Install Visual C++ Build Tools
# Linux: sudo apt-get install python3-dev build-essential

# Update pip
python -m pip install --upgrade pip

# Install problematic packages individually
pip install pyspark
pip install dask[complete]
pip install scikit-learn
```

## Network Configuration

### Windows Firewall Rules (PC2)
```powershell
# Allow ClickHouse
New-NetFirewallRule -DisplayName "ClickHouse HTTP" -Direction Inbound -LocalPort 8123 -Protocol TCP -Action Allow
New-NetFirewallRule -DisplayName "ClickHouse Native" -Direction Inbound -LocalPort 9000 -Protocol TCP -Action Allow

# Allow MongoDB
New-NetFirewallRule -DisplayName "MongoDB" -Direction Inbound -LocalPort 27017 -Protocol TCP -Action Allow
```

### Verify Connectivity Between PCs
From PC2:
```powershell
# Load PC1 IP from .env
$pc1_ip = (Get-Content .env | Select-String "PC1_IP").ToString().Split("=")[1].Trim()

# Test Kafka
Test-NetConnection -ComputerName $pc1_ip -Port 9092
```

From PC1:
```powershell
# Load PC2 IP from .env
$pc2_ip = (Get-Content .env | Select-String "PC2_IP").ToString().Split("=")[1].Trim()

# Test ClickHouse
Test-NetConnection -ComputerName $pc2_ip -Port 8123
```

## Post-Installation

### 1. Initialize Databases
Wait for Script 1 (running on PC1) to populate ClickHouse with initial data.

### 2. Run Background Services
```bash
# Terminal 1: Cache updates
python scripts/script2_clickhouse_to_mongodb.py

# Terminal 2: Dashboard
streamlit run scripts/streamlit_dashboard.py
```

### 3. Monitor Performance
```bash
# ClickHouse resource usage
docker stats clickhouse

# MongoDB resource usage
docker stats mongodb
```

## Ready Checklist

Before running the pipeline:
- [ ] Docker services running
- [ ] ClickHouse accessible (test with curl)
- [ ] MongoDB accessible (test with mongosh)
- [ ] Network connectivity to PC1 confirmed
- [ ] Python environment activated
- [ ] All dependencies installed
- [ ] Environment variables configured
- [ ] Firewall rules configured

## Next Steps

1. Coordinate with PC1 person to start data flow
2. Run Script 1 to consume from Kafka
3. Wait for data to populate ClickHouse
4. Run Script 2 for caching
5. Access dashboards and monitoring tools
6. Train ML models when sufficient data accumulated

## Support

Refer to:
- [PC2_COMPLETE_GUIDE.md](PC2_COMPLETE_GUIDE.md) - Comprehensive documentation
- [README.md](README.md) - Project overview
- [GITHUB_SETUP.md](GITHUB_SETUP.md) - Git workflow
