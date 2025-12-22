# PC2 Complete Setup Guide - Data Storage & Analytics

**Last Updated:** December 21, 2025
**PC Role:** Data Storage, Processing, and Analytics
**Status:** Awaiting Setup

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Network Configuration](#network-configuration)
4. [Docker Setup](#docker-setup)
5. [ClickHouse Configuration](#clickhouse-configuration)
6. [MongoDB Setup](#mongodb-setup)
7. [Python Environment](#python-environment)
8. [Data Pipeline Scripts](#data-pipeline-scripts)
9. [Machine Learning Pipeline](#machine-learning-pipeline)
10. [Visualization Dashboards](#visualization-dashboards)
11. [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## System Overview

### PC2 Responsibilities
- ClickHouse database for high-performance data storage
- MongoDB for caching aggregated data
- Script 2: ClickHouse to MongoDB caching
- Script 3: Machine Learning model training
- Power BI dashboards for business intelligence
- Streamlit real-time dashboard (optional)

### Architecture
```
PC1 (Kafka) → Script 1 → ClickHouse → Script 2 → MongoDB → Script 3 → ML Models
                            ↓                        ↓
                        Power BI              Streamlit Dashboard
```

### Services on PC2
- ClickHouse (ports 8123, 9000)
- MongoDB (port 27017)
- Mongo Express (port 8082) - optional UI
- Python scripts for data processing and ML
- Power BI Desktop
- Streamlit dashboard server

---

## Prerequisites

### Hardware Requirements
- 16GB RAM minimum (32GB recommended for ML)
- 100GB free disk space
- Stable network connection

### Software Requirements
- Windows 10/11
- Docker Desktop for Windows
- Python 3.9 or higher
- Power BI Desktop
- Microsoft C++ Build Tools (for some Python packages)

### Data Pipeline Understanding
- Understand Kafka consumer concepts
- Basic SQL knowledge for ClickHouse
- MongoDB document structure familiarity
- Machine learning concepts (training, evaluation)

---

## Network Configuration

### Step 1: Find Your IP Address

```powershell
ipconfig
```

Look for "IPv4 Address" under your active network adapter.

### Step 2: Configure Windows Firewall

Run PowerShell as Administrator:

```powershell
# Allow ClickHouse HTTP
New-NetFirewallRule -DisplayName "ClickHouse HTTP" -Direction Inbound -Protocol TCP -LocalPort 8123 -Action Allow

# Allow ClickHouse Native
New-NetFirewallRule -DisplayName "ClickHouse Native" -Direction Inbound -Protocol TCP -LocalPort 9000 -Action Allow

# Allow MongoDB
New-NetFirewallRule -DisplayName "MongoDB" -Direction Inbound -Protocol TCP -LocalPort 27017 -Action Allow

# Allow Streamlit (if hosting dashboard)
New-NetFirewallRule -DisplayName "Streamlit" -Direction Inbound -Protocol TCP -LocalPort 8501 -Action Allow
```

### Step 3: Environment Configuration

Create `.env` file with PC1 connection details:

```env
# PC Configuration
PC_ROLE=PC2
PC1_IP=<PC1_IP_ADDRESS>
PC2_IP=<YOUR_IP_ADDRESS>

# Kafka Configuration (PC1)
KAFKA_HOST=<PC1_IP_ADDRESS>
KAFKA_PORT=9092
KAFKA_TOPIC=airline-delays
CONSUMER_GROUP_ID=airline-consumer-group

# ClickHouse Configuration (Local)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=airline_data

# MongoDB Configuration (Local)
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_DATABASE=airline_cache
MONGODB_COLLECTION=aggregated_delays

# Script Configuration
BATCH_SIZE=1000
CACHE_UPDATE_INTERVAL=300
MODEL_PATH=./models

# Logging
LOG_LEVEL=INFO
LOG_FILE=./logs/pipeline.log
```

### Step 4: Test Connectivity to PC1

```powershell
# Test basic connectivity
ping <PC1_IP>

# Test Kafka port
Test-NetConnection -ComputerName <PC1_IP> -Port 9092

# Test with telnet (if available)
telnet <PC1_IP> 9092
```

---

## Docker Setup

### Docker Compose Configuration

File: `docker-compose.pc2.yml`

Services included:
- **ClickHouse:** High-performance columnar database
- **MongoDB:** Document database for caching
- **Mongo Express:** MongoDB web interface (optional)

### Start Docker Containers

```powershell
# Navigate to project directory
cd <project-path>

# Start all services
docker-compose -f docker-compose.pc2.yml up -d

# Wait 1-2 minutes for services to fully start
```

### Verify Containers

```powershell
# Check running containers
docker ps

# Expected output: clickhouse, mongodb, mongo-express
```

### Check Container Logs

```powershell
# ClickHouse logs
docker logs clickhouse

# MongoDB logs
docker logs mongodb

# Mongo Express logs
docker logs mongo-express
```

### Stop Containers

```powershell
docker-compose -f docker-compose.pc2.yml down
```

### Restart Containers

```powershell
docker-compose -f docker-compose.pc2.yml restart
```

---

## ClickHouse Configuration

### Access ClickHouse Client

```powershell
# Connect to ClickHouse client
docker exec -it clickhouse clickhouse-client
```

### Database Initialization

The database is automatically initialized from `config/clickhouse/init.sql`

**Main Tables:**

1. **flights** - Main fact table with all delay data
2. **carriers** - Carrier lookup table (normalized)
3. **airports** - Airport lookup table (normalized)

**Materialized Views:**

1. **daily_delay_summary** - Daily aggregations
2. **carrier_performance** - Carrier statistics
3. **airport_performance** - Airport statistics
4. **realtime_stats** - Real-time monitoring

### Verify Database Setup

```sql
-- Show databases
SHOW DATABASES;

-- Use airline database
USE airline_data;

-- Show tables
SHOW TABLES;

-- Check table structure
DESCRIBE flights;

-- Count records (should be 0 initially)
SELECT count() FROM flights;
```

### Manual Table Creation (if needed)

If auto-initialization fails, run SQL from `config/clickhouse/init.sql`:

```sql
CREATE DATABASE IF NOT EXISTS airline_data;

CREATE TABLE IF NOT EXISTS airline_data.flights (
    id UInt64,
    year UInt16,
    month UInt8,
    carrier String,
    carrier_name String,
    airport String,
    airport_name String,
    arr_flights UInt32,
    arr_del15 UInt32,
    carrier_ct Float32,
    weather_ct Float32,
    nas_ct Float32,
    security_ct Float32,
    late_aircraft_ct Float32,
    arr_cancelled UInt32,
    arr_diverted UInt32,
    arr_delay UInt32,
    carrier_delay UInt32,
    weather_delay UInt32,
    nas_delay UInt32,
    security_delay UInt32,
    late_aircraft_delay UInt32,
    ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (year, month, carrier, airport)
PARTITION BY toYYYYMM(toDate(concat(toString(year), '-', toString(month), '-01')))
SETTINGS index_granularity = 8192;
```

### ClickHouse Web Interface

Access HTTP interface:

```powershell
# Test connection
curl http://localhost:8123/ping

# Run query via HTTP
curl "http://localhost:8123/?query=SELECT version()"
```

### Performance Optimization

```sql
-- Check table size and compression
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows,
    max(modification_time) as latest_modification
FROM system.parts
WHERE database = 'airline_data' AND active
GROUP BY table;

-- Check query performance
SYSTEM FLUSH LOGS;
SELECT 
    query,
    type,
    event_time,
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 10;
```

---

## MongoDB Setup

### Access MongoDB Shell

```powershell
# Connect to MongoDB
docker exec -it mongodb mongosh
```

### Database Initialization

Database is auto-initialized from `config/mongodb/init-mongo.js`

**Collections:**
- aggregated_delays - Daily/monthly aggregations
- carrier_stats - Carrier performance metrics
- airport_stats - Airport performance metrics
- ml_predictions - ML model predictions

### Verify MongoDB Setup

```javascript
// Show databases
show dbs;

// Use airline cache database
use airline_cache;

// Show collections
show collections;

// Check document count
db.aggregated_delays.countDocuments();

// View sample document
db.aggregated_delays.findOne();
```

### MongoDB Indexes

Indexes are created automatically:

```javascript
// View indexes
db.aggregated_delays.getIndexes();

// Create additional indexes if needed
db.aggregated_delays.createIndex({ "carrier": 1 });
db.aggregated_delays.createIndex({ "airport": 1 });
db.aggregated_delays.createIndex({ "year": 1, "month": 1 });
```

### Mongo Express UI

Access web interface: http://localhost:8082

Default credentials:
- Username: admin
- Password: admin

---

## Python Environment

### Create Virtual Environment

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1
```

### Install Dependencies

```powershell
# Install all required packages
pip install -r requirements.txt
```

**Note:** Some packages require Microsoft C++ Build Tools. If installation fails:

1. Download Visual Studio Build Tools
2. Install "Desktop development with C++"
3. Retry pip install

### Required Packages

- confluent-kafka (Kafka client)
- clickhouse-connect (ClickHouse connector)
- pymongo (MongoDB client)
- pandas (Data manipulation)
- numpy (Numerical computing)
- scikit-learn (Machine learning)
- pyspark (Spark ML)
- dask, dask-ml (Distributed computing)
- streamlit, plotly (Visualization)
- loguru (Logging)

### Verify Installation

```powershell
# Check Python version
python --version

# List installed packages
pip list

# Test imports
python -c "import pandas, numpy, clickhouse_connect, pymongo; print('All imports successful')"
```

---

## Data Pipeline Scripts

### Script 1: Kafka to ClickHouse Consumer

**Location:** `scripts/script1_kafka_to_clickhouse.py`

**Purpose:** Consumes messages from Kafka and inserts into ClickHouse

**Note:** This script runs on PC1 but sends data to PC2 (ClickHouse)

**Configuration:**
- Connects to Kafka on PC1
- Connects to ClickHouse on PC2 (localhost from PC2 perspective)
- Batch size: 1000 records
- Auto-commit enabled

### Script 2: ClickHouse to MongoDB Cache

**Location:** `scripts/script2_clickhouse_to_mongodb.py`

**Purpose:** Extracts aggregated data from ClickHouse and caches in MongoDB

**Run Script:**

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run continuously (updates every 5 minutes)
python scripts/script2_clickhouse_to_mongodb.py

# Run once and exit
python scripts/script2_clickhouse_to_mongodb.py --once
```

**What it does:**
1. Queries ClickHouse for aggregated statistics
2. Computes carrier performance metrics
3. Computes airport performance metrics
4. Generates time series data
5. Updates MongoDB collections via upsert

**Monitoring:**

```powershell
# View logs
Get-Content logs/clickhouse_to_mongodb.log -Tail 50 -Wait
```

**Schedule for Production:**

Use Windows Task Scheduler to run every 5 minutes:

```powershell
# Create task
$action = New-ScheduledTaskAction -Execute 'python' -Argument 'scripts/script2_clickhouse_to_mongodb.py --once' -WorkingDirectory 'C:\path\to\project'
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)
Register-ScheduledTask -TaskName "AirlineDataCache" -Action $action -Trigger $trigger
```

### Script Performance Tuning

**Adjust batch sizes:**

Edit `.env`:
```env
BATCH_SIZE=2000  # Increase for better throughput
CACHE_UPDATE_INTERVAL=600  # Update every 10 minutes instead of 5
```

---

## Machine Learning Pipeline

### Script 3: ML Model Training

**Location:** `scripts/script3_ml_training.py`

**Purpose:** Trains delay prediction models using three frameworks

### Framework Comparison

1. **Scikit-learn (Pandas)** - Traditional single-machine ML
2. **Spark MLlib** - Distributed ML for big data
3. **Dask-ML** - Parallel ML with Dask

### Run ML Training

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run training pipeline
python scripts/script3_ml_training.py
```

**Training Process:**

1. Loads aggregated data from MongoDB
2. Prepares features and labels
3. Splits data (80% train, 20% test)
4. Trains model with all three frameworks
5. Evaluates performance metrics
6. Saves models to `models/` directory
7. Generates comparison report

### Model Outputs

```
models/
├── sklearn_model.joblib        # Scikit-learn model
├── spark_model/                # Spark MLlib model
├── dask_model.joblib           # Dask-ML model
└── framework_comparison.json   # Performance comparison
```

### View Results

```powershell
# View comparison
Get-Content models/framework_comparison.json | ConvertFrom-Json | Format-List
```

### Performance Metrics

Each framework is evaluated on:
- RMSE (Root Mean Square Error)
- MAE (Mean Absolute Error)
- R² Score
- Training time
- Memory usage

### Model Prediction

Use trained models for predictions:

```python
import joblib
import pandas as pd

# Load model
model = joblib.load('models/sklearn_model.joblib')

# Prepare features
features = pd.DataFrame({
    'year': [2023],
    'month': [6],
    'total_flights': [500],
    'total_delayed': [50],
    # ... other features
})

# Predict
prediction = model.predict(features)
print(f"Predicted delay percentage: {prediction[0]:.2f}%")
```

### Retraining Schedule

For production, retrain models daily:

```powershell
# Create scheduled task
$action = New-ScheduledTaskAction -Execute 'python' -Argument 'scripts/script3_ml_training.py' -WorkingDirectory 'C:\path\to\project'
$trigger = New-ScheduledTaskTrigger -Daily -At 2AM
Register-ScheduledTask -TaskName "AirlineMLTraining" -Action $action -Trigger $trigger
```

---

## Visualization Dashboards

### Power BI Setup

#### Install Power BI Desktop

Download from: https://powerbi.microsoft.com/desktop/

#### Connect to ClickHouse

1. Open Power BI Desktop
2. Get Data → More → Database → ClickHouse
3. Enter connection details:
   - Server: localhost:8123
   - Database: airline_data
4. Select tables: flights, carrier_performance, airport_performance
5. Load data

#### Create Data Model

1. Create relationships between tables
2. Define measures:

```dax
Total Flights = SUM(flights[arr_flights])
Total Delayed = SUM(flights[arr_del15])
Delay Rate = DIVIDE([Total Delayed], [Total Flights]) * 100
Average Delay Minutes = DIVIDE(SUM(flights[arr_delay]), SUM(flights[arr_flights]))
```

#### Build Dashboards

**Dashboard 1: Executive Summary**
- KPI cards: Total flights, delay rate, cancellations
- Line chart: Monthly delay trends
- Bar chart: Top delayed carriers
- Map: Delays by airport location
- Pie chart: Delay cause breakdown

**Dashboard 2: Carrier Analysis**
- Table: Carrier performance rankings
- Bar chart: Delays by carrier
- Scatter plot: Flights vs delay rate
- Time series: Carrier trends over time

**Dashboard 3: Airport Analysis**
- Map: Airport locations with delay indicators
- Table: Airport statistics
- Bar chart: Worst performing airports
- Heatmap: Delays by month and airport

#### Refresh Data

Configure scheduled refresh:
1. Publish to Power BI Service (optional)
2. Set up scheduled refresh
3. Or manually refresh: Home → Refresh

### Streamlit Real-Time Dashboard

**Location:** `scripts/streamlit_dashboard.py`

#### Run Dashboard

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run Streamlit app
streamlit run scripts/streamlit_dashboard.py
```

Access: http://localhost:8501

#### Dashboard Features

- Real-time statistics from ClickHouse
- Auto-refresh every 10 seconds
- Interactive charts with Plotly
- Carrier performance rankings
- Airport delay analysis
- Monthly trend visualization
- Delay cause breakdown

#### Customize Dashboard

Edit `scripts/streamlit_dashboard.py` to add:
- Additional charts
- Custom filters
- Different refresh intervals
- Kafka real-time data integration

#### Deploy Dashboard

For production deployment:

```powershell
# Run as background service
Start-Process streamlit run scripts/streamlit_dashboard.py -WindowStyle Hidden

# Or use Windows service wrapper
# Install NSSM: https://nssm.cc/
nssm install StreamlitDashboard "C:\path\to\python.exe" "C:\path\to\streamlit" run scripts/streamlit_dashboard.py
nssm start StreamlitDashboard
```

---

## Monitoring & Troubleshooting

### System Health Checks

```powershell
# Check all containers
docker ps

# Check Docker resource usage
docker stats

# Check disk space
Get-PSDrive C | Select-Object Used,Free
```

### ClickHouse Monitoring

```sql
-- Check active queries
SELECT query_id, user, query, elapsed 
FROM system.processes 
WHERE query NOT LIKE '%system.processes%';

-- Check recent errors
SELECT event_time, message 
FROM system.text_log 
WHERE level <= 'Warning' 
ORDER BY event_time DESC 
LIMIT 10;

-- Check table sizes
SELECT 
    database,
    table,
    formatReadableSize(sum(bytes)) AS size,
    sum(rows) AS rows
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes) DESC;
```

### MongoDB Monitoring

```javascript
// Database stats
db.stats();

// Collection stats
db.aggregated_delays.stats();

// Current operations
db.currentOp();

// Recent slow queries
db.system.profile.find().sort({ts:-1}).limit(10);
```

### Common Issues

#### Issue: Cannot Connect from PC1

**Symptoms:** Script 1 fails to connect to ClickHouse

**Solution:**
```powershell
# Verify ClickHouse is running
docker ps | grep clickhouse

# Test HTTP endpoint
curl http://localhost:8123/ping

# Check firewall
Test-NetConnection -ComputerName <PC2_IP> -Port 8123

# Verify .env configuration on PC1
```

#### Issue: MongoDB Cache Not Updating

**Symptoms:** Empty collections in MongoDB

**Solution:**
```powershell
# Check Script 2 logs
Get-Content logs/clickhouse_to_mongodb.log

# Verify ClickHouse has data
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM airline_data.flights"

# Run script manually with debug output
python scripts/script2_clickhouse_to_mongodb.py --once
```

#### Issue: ML Training Fails

**Symptoms:** Out of memory or import errors

**Solution:**
```powershell
# Check available memory
Get-ComputerInfo | Select-Object CsTotalPhysicalMemory, CsFreePhysicalMemory

# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory

# Install missing build tools
# Download Visual Studio Build Tools

# Use smaller dataset for testing
# Edit script to limit data: .limit(10000)
```

### Log Files

```powershell
# View all logs
Get-ChildItem logs/*.log

# Follow logs in real-time
Get-Content logs/clickhouse_to_mongodb.log -Wait -Tail 50

# Search logs for errors
Select-String -Path logs/*.log -Pattern "ERROR"
```

---

## Performance Optimization

### ClickHouse Optimization

```sql
-- Optimize tables (merge small parts)
OPTIMIZE TABLE flights FINAL;

-- Create additional indexes
ALTER TABLE flights ADD INDEX idx_carrier carrier TYPE bloom_filter GRANULARITY 1;

-- Adjust settings
SET max_threads = 8;
SET max_memory_usage = 10000000000;  -- 10GB
```

### MongoDB Optimization

```javascript
// Create compound indexes
db.aggregated_delays.createIndex({ "year": 1, "month": 1, "carrier": 1 });

// Enable profiling
db.setProfilingLevel(1, { slowms: 100 });

// Compact collections
db.runCommand({ compact: 'aggregated_delays' });
```

### Script Optimization

1. Increase batch sizes for better throughput
2. Use connection pooling
3. Enable compression
4. Parallel processing where applicable

---

## Backup and Recovery

### Backup ClickHouse Data

```powershell
# Backup database
docker exec clickhouse clickhouse-client --query "BACKUP DATABASE airline_data TO Disk('backups', 'airline_data_backup.zip')"

# Or export to CSV
docker exec clickhouse clickhouse-client --query "SELECT * FROM airline_data.flights FORMAT CSV" > backup.csv
```

### Backup MongoDB Data

```powershell
# Backup all databases
docker exec mongodb mongodump --out /backup

# Backup specific database
docker exec mongodb mongodump --db airline_cache --out /backup
```

### Restore Data

```powershell
# Restore ClickHouse
docker exec clickhouse clickhouse-client --query "RESTORE DATABASE airline_data FROM Disk('backups', 'airline_data_backup.zip')"

# Restore MongoDB
docker exec mongodb mongorestore /backup
```

---

## Production Deployment Checklist

- [ ] Static IP configured and tested
- [ ] Firewall rules configured
- [ ] Docker containers running and healthy
- [ ] ClickHouse database initialized
- [ ] MongoDB collections created
- [ ] PC1 connectivity verified
- [ ] Script 2 running and caching data
- [ ] ML models trained successfully
- [ ] Power BI dashboards created
- [ ] Streamlit dashboard accessible
- [ ] Monitoring and alerting configured
- [ ] Backup strategy implemented
- [ ] Documentation reviewed

---

## Appendix

### File Locations

- **Configuration:** `.env`
- **Docker Compose:** `docker-compose.pc2.yml`
- **ClickHouse Init:** `config/clickhouse/init.sql`
- **MongoDB Init:** `config/mongodb/init-mongo.js`
- **Scripts:** `scripts/`
- **Logs:** `logs/`
- **Models:** `models/`

### Port Reference

| Service | Port | Access |
|---------|------|--------|
| ClickHouse HTTP | 8123 | http://localhost:8123 |
| ClickHouse Native | 9000 | TCP connection |
| MongoDB | 27017 | mongodb://localhost:27017 |
| Mongo Express | 8082 | http://localhost:8082 |
| Streamlit | 8501 | http://localhost:8501 |

### Useful SQL Queries

```sql
-- Top delayed carriers
SELECT 
    carrier_name,
    sum(arr_del15) as delayed_flights,
    sum(arr_flights) as total_flights,
    round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_rate
FROM flights
GROUP BY carrier_name
ORDER BY delay_rate DESC
LIMIT 10;

-- Monthly delay trends
SELECT 
    year,
    month,
    round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_rate
FROM flights
GROUP BY year, month
ORDER BY year, month;

-- Delay causes breakdown
SELECT 
    round(sum(carrier_delay) / sum(arr_delay) * 100, 2) as carrier_pct,
    round(sum(weather_delay) / sum(arr_delay) * 100, 2) as weather_pct,
    round(sum(nas_delay) / sum(arr_delay) * 100, 2) as nas_pct,
    round(sum(late_aircraft_delay) / sum(arr_delay) * 100, 2) as aircraft_pct
FROM flights;
```

---

**End of PC2 Complete Guide**
