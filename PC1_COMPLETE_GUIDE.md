# PC1 Complete Setup Guide - Data Ingestion

**Last Updated:** December 21, 2025
**PC Role:** Data Ingestion and Streaming
**Current Status:** Operational

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Prerequisites](#prerequisites)
3. [Network Configuration](#network-configuration)
4. [Docker Setup](#docker-setup)
5. [Python Environment](#python-environment)
6. [NiFi Configuration](#nifi-configuration)
7. [Data Streaming](#data-streaming)
8. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
9. [Integration with PC2](#integration-with-pc2)

---

## System Overview

### PC1 Responsibilities
- Apache NiFi for data flow orchestration
- Apache Kafka broker for message streaming
- Data ingestion from CSV files
- Real-time data generation and publishing
- Kafka consumer script (Script 1) to send data to ClickHouse

### Architecture
```
CSV File → NiFi/Simulator → Kafka → Script 1 → ClickHouse (PC2)
```

### Current Configuration
- **IP Address:** 192.168.11.129
- **Kafka Broker:** 192.168.11.129:9092
- **NiFi UI:** http://localhost:8080/nifi
- **Kafka UI:** http://localhost:8081

---

## Prerequisites

### Hardware Requirements
- 8GB RAM minimum (16GB recommended)
- 50GB free disk space
- Stable network connection

### Software Requirements
- Windows 10/11
- Docker Desktop for Windows
- Python 3.9 or higher
- Git (optional, for version control)

### Data Requirements
- Airline delay dataset: `Airline_Delay_Cause - Airline_Delay_Cause.csv`
- 318,000+ records
- Located in project root directory

---

## Network Configuration

### Step 1: Find Your IP Address

```powershell
ipconfig
```

Look for "IPv4 Address" under your active network adapter. Current PC1 IP: **192.168.11.129**

### Step 2: Configure Windows Firewall

Run PowerShell as Administrator and execute:

```powershell
# Allow Kafka
New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -Protocol TCP -LocalPort 9092 -Action Allow

# Allow NiFi
New-NetFirewallRule -DisplayName "NiFi" -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow

# Allow Zookeeper (if needed for external access)
New-NetFirewallRule -DisplayName "Zookeeper" -Direction Inbound -Protocol TCP -LocalPort 2181 -Action Allow
```

### Step 3: Environment Configuration

Edit `.env` file with your network settings:

```env
# PC Configuration
PC_ROLE=PC1
PC1_IP=192.168.11.129
PC2_IP=<UPDATE_WITH_PC2_IP>

# Kafka Configuration
KAFKA_HOST=192.168.11.129
KAFKA_PORT=9092
KAFKA_TOPIC=airline-delays

# ClickHouse Configuration (PC2)
CLICKHOUSE_HOST=<UPDATE_WITH_PC2_IP>
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_DATABASE=airline_data

# Data Paths
CSV_FILE_PATH=./Airline_Delay_Cause - Airline_Delay_Cause.csv
```

### Step 4: Test Network Connectivity

Once PC2 is ready, test connection:

```powershell
# Test basic connectivity
ping <PC2_IP>

# Test ClickHouse HTTP port
Test-NetConnection -ComputerName <PC2_IP> -Port 8123

# Test ClickHouse Native port
Test-NetConnection -ComputerName <PC2_IP> -Port 9000
```

---

## Docker Setup

### Docker Compose Configuration

File: `docker-compose.pc1.yml`

Services included:
- **Zookeeper:** Coordination service for Kafka
- **Kafka:** Message broker
- **NiFi:** Data flow orchestration
- **Kafka UI:** Web interface for Kafka monitoring

### Start Docker Containers

```powershell
# Navigate to project directory
cd C:\Users\X1\ynov-pfe

# Start all services
docker-compose -f docker-compose.pc1.yml up -d

# Wait 2-3 minutes for services to fully start
```

### Verify Containers

```powershell
# Check running containers
docker ps

# Expected output: zookeeper, kafka, nifi, kafka-ui all running
```

### Check Container Logs

```powershell
# Kafka logs
docker logs kafka

# NiFi logs
docker logs nifi

# Zookeeper logs
docker logs zookeeper
```

### Stop Containers

```powershell
docker-compose -f docker-compose.pc1.yml down
```

### Restart Containers

```powershell
docker-compose -f docker-compose.pc1.yml restart
```

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

For PC1, only essential packages are needed:

```powershell
# Install from requirements-pc1.txt
pip install -r requirements-pc1.txt
```

Required packages:
- python-dotenv (environment variables)
- confluent-kafka (Kafka producer/consumer)
- clickhouse-connect (ClickHouse database connector)
- loguru (logging)
- requests (HTTP requests)
- tqdm (progress bars)

### Verify Installation

```powershell
pip list
```

---

## NiFi Configuration

### Access NiFi UI

1. Open browser: http://localhost:8080/nifi
2. Login credentials:
   - Username: `admin`
   - Password: `adminadminadmin`

### NiFi Flow Architecture

```
CSV File → GetFile → SplitText → ConvertRecord → UpdateAttribute → PublishKafka
```

### Step-by-Step Processor Configuration

#### 1. GetFile Processor

Reads CSV file from disk.

**Properties:**
- Input Directory: `/data/raw`
- File Filter: `Airline_Delay_Cause.*\.csv`
- Keep Source File: `true`
- Minimum File Age: `0 sec`
- Polling Interval: `10 sec`
- Batch Size: `1`

#### 2. SplitText Processor

Splits CSV into individual lines.

**Properties:**
- Line Split Count: `1`
- Header Line Count: `1`
- Remove Trailing Newlines: `true`

#### 3. ConvertRecord Processor

Converts CSV to JSON format.

**Properties:**
- Record Reader: `CSVReader`
- Record Writer: `JsonRecordSetWriter`

**CSVReader Settings:**
- Schema Access Strategy: `Infer Schema`
- Treat First Line as Header: `true`

**JsonRecordSetWriter Settings:**
- Schema Access Strategy: `Inherit Record Schema`
- Pretty Print JSON: `true`

#### 4. UpdateAttribute Processor

Adds timestamp and metadata.

**Custom Properties:**
- `timestamp`: `${now():toNumber()}`
- `ingestion.time`: `${now():format('yyyy-MM-dd HH:mm:ss')}`
- `kafka.key`: `${UUID()}`

#### 5. PublishKafka Processor

Publishes messages to Kafka topic.

**Properties:**
- Kafka Brokers: `kafka:29092`
- Topic Name: `airline-delays`
- Delivery Guarantee: `Best Effort`
- Compression Type: `gzip`
- Batch Size: `100`

### Controller Services

#### CSVReader Configuration

1. Click on canvas (not on processor)
2. Right-click → Configure → Controller Services
3. Add CSVReader
4. Configure:
   - Schema Access Strategy: `Infer Schema`
   - Treat First Line as Header: `true`
   - CSV Format: `RFC 4180`
5. Enable service

#### JsonRecordSetWriter Configuration

1. Add JsonRecordSetWriter
2. Configure:
   - Schema Write Strategy: `full-schema-attribute`
   - Schema Access Strategy: `Inherit Record Schema`
   - Pretty Print JSON: `true`
3. Enable service

### Connect Processors

1. Drag connection from GetFile to SplitText (success)
2. Drag connection from SplitText to ConvertRecord (splits)
3. Drag connection from ConvertRecord to UpdateAttribute (success)
4. Drag connection from UpdateAttribute to PublishKafka (success)
5. Auto-terminate unused relationships

### Start the Flow

1. Select all processors (Ctrl+A)
2. Click Start button (play icon)
3. Monitor data flow through connections

---

## Data Streaming

### Option 1: NiFi Simulator (Recommended)

Use Python script instead of NiFi UI for easier testing.

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run simulator
C:/Users/X1/ynov-pfe/venv/Scripts/python.exe scripts/nifi_simulator.py
```

**Simulator Configuration:**
- Located in: `scripts/nifi_simulator.py`
- Rate: 10 records per second (adjustable)
- Auto-loops dataset when finished
- Includes timestamp injection

**Stop Simulator:**
Press `Ctrl+C`

### Option 2: Apache NiFi

Follow NiFi Configuration section above to set up visual flow.

### Verify Data Streaming

#### Check Kafka Topics

```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output: `airline-delays`

#### View Messages in Kafka

```powershell
# View first 10 messages
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --from-beginning --max-messages 10
```

#### Monitor with Kafka UI

Access: http://localhost:8081

Features:
- View topics and messages
- Monitor consumer groups
- Check broker status
- View message throughput

### Message Format

```json
{
  "year": "2022",
  "month": "5",
  "carrier": "9E",
  "carrier_name": "Endeavor Air Inc.",
  "airport": "ABE",
  "airport_name": "Allentown/Bethlehem/Easton, PA: Lehigh Valley International",
  "arr_flights": "136",
  "arr_del15": "7",
  "carrier_ct": "5.95",
  "weather_ct": "0",
  "nas_ct": "0.05",
  "security_ct": "0",
  "late_aircraft_ct": "1",
  "arr_cancelled": "0",
  "arr_diverted": "0",
  "arr_delay": "255",
  "carrier_delay": "222",
  "weather_delay": "0",
  "nas_delay": "4",
  "security_delay": "0",
  "late_aircraft_delay": "29",
  "ingestion_timestamp": "2025-12-21T10:44:15.961071"
}
```

---

## Monitoring & Troubleshooting

### Check Container Status

```powershell
# View running containers
docker ps

# View all containers (including stopped)
docker ps -a

# Check specific container
docker ps --filter name=kafka
```

### View Container Logs

```powershell
# Last 50 lines
docker logs kafka --tail 50

# Follow logs in real-time
docker logs kafka --follow

# Logs with timestamps
docker logs kafka --timestamps
```

### Common Issues

#### Issue: Kafka Not Starting

**Symptoms:** Container exits immediately after start

**Solution:**
```powershell
# Check logs
docker logs kafka

# Verify Zookeeper is running
docker ps | grep zookeeper

# Restart services
docker-compose -f docker-compose.pc1.yml restart
```

#### Issue: NiFi UI Not Accessible

**Symptoms:** Cannot access http://localhost:8080/nifi

**Solution:**
```powershell
# Wait 2-3 minutes after container start
# Check NiFi logs
docker logs nifi

# Verify container is running
docker ps | grep nifi

# Check health status
docker inspect nifi | grep Health
```

#### Issue: Cannot Connect to Kafka from Scripts

**Symptoms:** Connection timeout or refused

**Solution:**
1. Verify Kafka is running: `docker ps`
2. Check firewall rules
3. Verify IP address in `.env` file
4. Test connection: `telnet localhost 9092`

#### Issue: No Messages in Kafka Topic

**Symptoms:** Topic exists but no messages

**Solution:**
1. Check if NiFi processors are started
2. Verify CSV file path is correct
3. Check NiFi processor logs in UI
4. Run simulator script for testing

### Performance Monitoring

#### Check Kafka Performance

```powershell
# Topic details
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic airline-delays

# Consumer group lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group airline-consumer-group
```

#### Check System Resources

```powershell
# Docker stats
docker stats

# Container resource usage
docker stats kafka nifi zookeeper
```

---

## Integration with PC2

### Prerequisites for Integration

1. PC2 must have ClickHouse running
2. Network connectivity established between PCs
3. Firewall rules configured on both PCs
4. PC2 IP address configured in `.env` file

### Update Configuration

Edit `.env` file with PC2 details:

```env
PC2_IP=<PC2_IP_ADDRESS>
CLICKHOUSE_HOST=<PC2_IP_ADDRESS>
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_DATABASE=airline_data
```

### Test Connection to PC2

```powershell
# Test ping
ping <PC2_IP>

# Test ClickHouse HTTP
Test-NetConnection -ComputerName <PC2_IP> -Port 8123

# Test HTTP endpoint
curl http://<PC2_IP>:8123/ping
```

### Run Kafka Consumer (Script 1)

Once PC2 is ready, start the consumer to send data to ClickHouse:

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run consumer script
C:/Users/X1/ynov-pfe/venv/Scripts/python.exe scripts/script1_kafka_to_clickhouse.py
```

**Script Functionality:**
- Consumes messages from Kafka topic `airline-delays`
- Batches records (default: 1000 records per batch)
- Inserts data into ClickHouse `flights` table
- Logs progress to `logs/kafka_to_clickhouse.log`
- Auto-commits offsets after successful insertion

### Monitor Script Execution

```powershell
# View logs
Get-Content logs/kafka_to_clickhouse.log -Tail 50 -Wait

# Check consumer group status
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group airline-consumer-group
```

### Verify Data in ClickHouse

From PC2 or remotely:

```sql
-- Check record count
SELECT count() FROM airline_data.flights;

-- View sample records
SELECT * FROM airline_data.flights LIMIT 10;

-- Check latest records
SELECT * FROM airline_data.flights ORDER BY ingestion_timestamp DESC LIMIT 10;
```

---

## Operational Commands

### Daily Operations

```powershell
# Start system
docker-compose -f docker-compose.pc1.yml up -d

# Start data streaming
.\venv\Scripts\Activate.ps1
C:/Users/X1/ynov-pfe/venv/Scripts/python.exe scripts/nifi_simulator.py

# In another terminal: Start consumer (when PC2 is ready)
.\venv\Scripts\Activate.ps1
C:/Users/X1/ynov-pfe/venv/Scripts/python.exe scripts/script1_kafka_to_clickhouse.py
```

### Maintenance Commands

```powershell
# View all logs
docker-compose -f docker-compose.pc1.yml logs

# Restart specific service
docker-compose -f docker-compose.pc1.yml restart kafka

# Stop all services
docker-compose -f docker-compose.pc1.yml down

# Clean up (removes volumes)
docker-compose -f docker-compose.pc1.yml down -v
```

### Backup Commands

```powershell
# Backup NiFi flow
# Export template from NiFi UI: Templates menu → Download

# Backup Docker volumes
docker run --rm -v airline-pipeline_kafka-data:/data -v C:\backup:/backup alpine tar czf /backup/kafka-data-backup.tar.gz /data
```

---

## Performance Optimization

### Kafka Optimization

Adjust in `docker-compose.pc1.yml`:

```yaml
KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
KAFKA_LOG_SEGMENT_BYTES: 1073741824  # 1GB
KAFKA_NUM_PARTITIONS: 4  # Increase for parallelism
```

### NiFi Optimization

1. Increase batch sizes for better throughput
2. Adjust scheduling intervals
3. Enable compression for Kafka publishing
4. Use ConnectionPool controller services

### System Resource Allocation

```powershell
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory: 8GB+
```

---

## Security Considerations

### Network Security

- Use firewall rules to restrict access to specific IPs
- Do not expose ports to internet without VPN
- Consider using Kafka SSL/TLS for production

### Access Control

- Change default NiFi password
- Use separate user accounts for each service
- Implement Kafka ACLs for production use

---

## Appendix

### File Locations

- **Configuration:** `.env`
- **Docker Compose:** `docker-compose.pc1.yml`
- **Scripts:** `scripts/`
- **Logs:** `logs/`
- **Data:** `Airline_Delay_Cause - Airline_Delay_Cause.csv`

### Port Reference

| Service | Port | Access |
|---------|------|--------|
| Kafka | 9092 | External |
| Kafka Internal | 29092 | Docker network |
| Zookeeper | 2181 | Docker network |
| NiFi | 8080 | http://localhost:8080/nifi |
| NiFi HTTPS | 8443 | https://localhost:8443/nifi |
| Kafka UI | 8081 | http://localhost:8081 |

### Useful Links

- Kafka Documentation: https://kafka.apache.org/documentation/
- NiFi Documentation: https://nifi.apache.org/docs.html
- Docker Documentation: https://docs.docker.com/

---

**End of PC1 Complete Guide**
