# Real-Time Data Pipeline - Airline Delay Analysis

## Project Overview
This project implements a complete real-time data pipeline for analyzing airline delays using:
- **Apache NiFi** - Data ingestion and flow generation
- **Apache Kafka** - Real-time data streaming
- **ClickHouse** - High-performance columnar database
- **MongoDB** - Data caching layer
- **Power BI** - Business intelligence dashboards
- **Python** - Data processing, ML, and real-time visualization
- **ML Frameworks** - Spark MLlib, Dask-ML, Scikit-learn

## Dataset
**Airline Delay Cause Dataset** containing:
- Flight delays by carrier, airport, and time period
- Delay categories: carrier, weather, NAS, security, late aircraft
- Arrival statistics and cancellations

## Architecture

```
┌─────────┐ ┌───────┐ ┌──────────┐ ┌────────────┐
│ NiFi │───▶│ Kafka │───▶│ Script 1 │───▶│ ClickHouse │
└─────────┘ └───┬───┘ │ Consumer │ └──────┬─────┘
 │ └──────────┘ │
 │ │
 │ ┌──────────┐ ┌──────▼─────┐
 │ │ Script 2 │◀───│ Power BI │
 │ │ Cache │ └────────────┘
 │ └────┬─────┘
 │ │
 │ ┌────▼─────┐
 └────────▶│ Script 3 │
 │ │ ML Model │
 │ └──────────┘
 │ │
 │ ┌────▼─────┐
 └────────▶│ Streamlit│
 │Dashboard │
 └──────────┘
```

## Distributed Setup (2 PCs)

### PC 1 (Data Ingestion & Streaming) - Your PC
**Responsibilities:**
- Apache NiFi (data flow generation)
- Apache Kafka broker
- Script 1: Kafka → ClickHouse consumer
- Real-time monitoring

**IP Configuration:** Set static IP or use hostname

### PC 2 (Processing & Analytics) - Friend's PC
**Responsibilities:**
- ClickHouse database
- MongoDB cache
- Script 2: ClickHouse → MongoDB caching
- Script 3: ML training (Spark/Dask/Scikit-learn)
- Power BI dashboards
- Streamlit real-time visualization

**IP Configuration:** Set static IP or use hostname

### Network Configuration
Both PCs must be on the same network or have firewall rules configured:

**Ports to open:**
- Kafka: 9092
- ClickHouse: 8123 (HTTP), 9000 (Native)
- MongoDB: 27017
- NiFi: 8080
- Streamlit: 8501

## Quick Start
**For detailed setup instructions:**
- **PC1 Person:** See [PC1_SETUP.md](PC1_SETUP.md) - Quick setup guide
- **PC2 Person:** See [PC2_SETUP.md](PC2_SETUP.md) - Quick setup guide

### PC1 (5 minutes)
```bash
git clone <repo-url> && cd ynov-pfe
cp .env.example .env  # Edit with your IPs
docker-compose --profile pc1 up -d
python -m venv venv && venv\Scripts\activate
pip install -r requirements-pc1.txt
python scripts/nifi_simulator.py
```

### PC2 (5 minutes)
```bash
git clone <repo-url> && cd ynov-pfe
cp .env.example .env  # Edit with both IPs
docker-compose --profile pc2 up -d
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
python scripts/script1_kafka_to_clickhouse.py
```
### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Power BI Desktop
- Network connectivity between PCs

### Setup Instructions

#### On PC 1 (Data Ingestion):
```bash
# 1. Clone and navigate to project
git clone <repository-url>
cd ynov-pfe

# 2. Set your PC1 IP in .env file
# Edit .env and set PC1_IP=<your-ip> and PC2_IP=<friend-ip>

# 3. Start PC1 services only (NiFi, Kafka, Zookeeper)
docker-compose --profile pc1 up -d

# 4. Install Python dependencies
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements-pc1.txt

# 5. Run data simulator or NiFi
python scripts/nifi_simulator.py
```

#### On PC 2 (Processing & Analytics):
```bash
# 1. Clone the same repository
git clone <repository-url>
cd ynov-pfe

# 2. Set IPs in .env file
# Edit .env and set PC1_IP=<friend-ip> and PC2_IP=<your-ip>

# 3. Start PC2 services only (ClickHouse, MongoDB)
docker-compose --profile pc2 up -d

# 4. Install Python dependencies
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt

# 5. Run data pipeline
python scripts/script1_kafka_to_clickhouse.py

# 6. Run caching script
python scripts/script2_clickhouse_to_mongodb.py

# 7. Train ML models (daily)
python scripts/script3_ml_training.py

# 8. Launch Streamlit dashboard
streamlit run scripts/streamlit_dashboard.py
```

## Project Structure
```
ynov-pfe/
├── data/
│ ├── raw/ # Original dataset
│ └── processed/ # Processed data
├── scripts/
│ ├── script1_kafka_to_clickhouse.py
│ ├── script2_clickhouse_to_mongodb.py
│ ├── script3_ml_training.py
│ └── streamlit_dashboard.py
├── nifi/
│ ├── templates/ # NiFi flow templates
│ └── conf/ # NiFi configuration
├── config/
│ ├── clickhouse/ # ClickHouse schemas
│ └── kafka/ # Kafka topics config
├── models/ # Saved ML models
├── notebooks/ # Jupyter notebooks for analysis
├── docker-compose.yml # Unified Docker config (PC1 + PC2)
├── .env.example # Environment template
├── .env # Environment variables (not in git)
├── requirements-pc1.txt # PC1 Python dependencies
├── requirements.txt # PC2 Python dependencies
├── PC1_COMPLETE_GUIDE.md # Detailed PC1 guide
├── PC2_COMPLETE_GUIDE.md # Detailed PC2 guide
└── README.md
```

## Work Division

### Phase 1: Setup (Week 1)
- **PC1 Person:** NiFi flow creation, Kafka setup
- **PC2 Person:** ClickHouse schema design, MongoDB setup

### Phase 2: Data Pipeline (Week 2)
- **PC1 Person:** Script 1 (Kafka consumer)
- **PC2 Person:** Script 2 (Caching layer)

### Phase 3: Visualization (Week 3)
- **PC1 Person:** Streamlit real-time dashboard
- **PC2 Person:** Power BI dashboards

### Phase 4: Machine Learning (Week 4)
- **Both:** ML implementation (divide frameworks)
 - PC1: Spark MLlib, Dask-ML
 - PC2: Scikit-learn, Performance comparison

### Phase 5: Testing & Optimization (Week 5)
- **Both:** Integration testing, performance tuning

## Connection Testing

### Test Network Connectivity
From PC1:
```bash
# Test ClickHouse connection
curl http://<PC2_IP>:8123/ping

# Test MongoDB connection
mongosh mongodb://<PC2_IP>:27017
```

From PC2:
```bash
# Test Kafka connection
docker exec -it kafka kafka-broker-api-versions --bootstrap-server <PC1_IP>:9092
```

## Troubleshooting

### Connection Issues
1. Check firewall settings on both PCs
2. Verify both PCs are on same network
3. Use `ping <PC_IP>` to test basic connectivity
4. Ensure Docker containers can access host network

### Performance Issues
1. Monitor resource usage (CPU, RAM, disk)
2. Adjust batch sizes in Python scripts
3. Scale Kafka partitions if needed
4. Use ClickHouse materialized views for aggregations

## Unified Docker Setup

This project uses a single `docker-compose.yml` with **profiles** to separate PC1 and PC2 services:

### Docker Profiles
- **pc1 profile:** NiFi, Kafka, Zookeeper, Kafka-UI
- **pc2 profile:** ClickHouse, MongoDB, Mongo-Express

### Running Services

**PC1 - Start only ingestion services:**
```bash
docker-compose --profile pc1 up -d
```

**PC2 - Start only storage services:**
```bash
docker-compose --profile pc2 up -d
```

**View running services:**
```bash
docker-compose ps
```

**Stop services:**
```bash
docker-compose --profile pc1 down  # PC1
docker-compose --profile pc2 down  # PC2
```

## Next Steps
1. Review this README
2. ⬜ Configure network settings
3. ⬜ Set up Docker on both PCs
4. ⬜ Configure NiFi flow
5. ⬜ Create ClickHouse schema
6. ⬜ Implement Python scripts
7. ⬜ Build visualizations
8. ⬜ Train ML models
9. ⬜ Prepare presentation

## Team Members
- PC1 (Your Name): Data Ingestion & Streaming
- PC2 (Friend's Name): Processing & Analytics

## Presentation Tips
- Demonstrate live data flow
- Show real-time dashboards updating
- Present ML model comparisons with metrics
- Discuss scalability and improvements
- Explain technical decisions (why ClickHouse vs MySQL)

## License
Academic Project - YNOV 2025
