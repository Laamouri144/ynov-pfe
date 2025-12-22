# PC1 Quick Setup Guide

This guide helps you quickly set up PC1 (Data Ingestion PC) after cloning the repository.

## Prerequisites
- Docker Desktop installed
- Python 3.9+ installed
- Git installed
- Airline dataset CSV file

## Quick Setup Steps

### 1. Clone Repository
```bash
git clone <repository-url>
cd ynov-pfe
```

### 2. Add Dataset
Place your airline delay CSV file in the project root:
```
Airline_Delay_Cause - Airline_Delay_Cause.csv
```

Or download from: [provide-download-link]

### 3. Configure Environment
```bash
# Copy environment template
cp .env.example .env
```

**Edit .env file and set:**
```env
PC_ROLE=PC1
PC1_IP=<your-local-ip>      # Find with: ipconfig (Windows)
PC2_IP=<ask-friend-for-ip>  # Get from PC2 person
```

**Find your IP on Windows:**
```powershell
ipconfig | Select-String "IPv4"
```

### 4. Start PC1 Services
```bash
# Start Kafka, Zookeeper, NiFi, Kafka-UI
docker-compose --profile pc1 up -d

# Verify services are running (takes ~2 minutes for NiFi)
docker-compose ps
```

**Expected output:**
```
NAME        STATUS   PORTS
kafka       Up       9092, 29092
zookeeper   Up       2181
nifi        Up       8080, 8443
kafka-ui    Up       8081
```

### 5. Verify Kafka
```bash
# Check Kafka is healthy
docker logs kafka | Select-String "started"

# Create topic (if not auto-created)
docker exec kafka kafka-topics --create --topic airline-delays --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 6. Verify NiFi
```bash
# Check NiFi is ready
docker logs nifi | Select-String "Started"
```

**Access NiFi UI:**
- URL: `http://localhost:8080/nifi`
- Username: `admin`
- Password: `adminadminadmin`

### 7. Setup Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Install minimal PC1 dependencies
pip install -r requirements-pc1.txt
```

**PC1 Dependencies:**
```
python-dotenv
confluent-kafka
clickhouse-connect
loguru
requests
tqdm
```

### 8. Test Kafka Streaming
```bash
# Run the data simulator (alternative to NiFi)
python scripts/nifi_simulator.py
```

**You should see:**
```
2025-12-22 11:35:42 | INFO | Starting Airline Data Simulator
2025-12-22 11:35:42 | INFO | Connected to Kafka broker at localhost:9092
2025-12-22 11:35:42 | INFO | Loaded 318,107 records from CSV
2025-12-22 11:35:42 | INFO | Streaming at 10 records/second
2025-12-22 11:35:42 | SUCCESS | Sent record 1 to Kafka topic: airline-delays
```

### 9. Monitor Kafka Messages

**Option 1: Kafka-UI (Recommended)**
- Open: `http://localhost:8081`
- Navigate to Topics → airline-delays
- View messages in real-time

**Option 2: Command Line**
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --from-beginning --max-messages 5
```

### 10. Share Your IP with PC2
```bash
# Get your IP
ipconfig | Select-String "IPv4"
```

Send this IP to your friend (PC2) so they can update their `.env` file.

## Firewall Configuration

**Windows Firewall - Open Kafka Port:**
```powershell
# Allow Kafka connections from PC2
New-NetFirewallRule -DisplayName "Kafka Broker" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
```

## Troubleshooting

### Problem: Kafka fails to start
**Solution:**
```bash
# Check logs
docker logs kafka

# Common issue: Port already in use
netstat -ano | findstr :9092

# Stop conflicting service or change port in docker-compose.yml
```

### Problem: NiFi takes too long to start
**Solution:**
- NiFi needs 2-3 minutes on first startup
- Check progress: `docker logs -f nifi`
- Look for: "NiFi has started"

### Problem: Data simulator can't connect
**Solution:**
1. Verify Kafka is running: `docker ps | findstr kafka`
2. Check `.env` has `KAFKA_HOST=localhost` (for PC1)
3. Ensure topic exists:
   ```bash
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

### Problem: PC2 can't connect to your Kafka
**Solution:**
1. Verify firewall allows port 9092
2. Test from PC2: `Test-NetConnection <PC1_IP> -Port 9092`
3. Ensure docker-compose.yml has correct KAFKA_ADVERTISED_LISTENERS with your PC1_IP
4. Check `.env` file PC1_IP is correct

### Problem: CSV file not found
**Solution:**
```bash
# Verify file exists
Test-Path "Airline_Delay_Cause - Airline_Delay_Cause.csv"

# Update path in .env if different
CSV_FILE_PATH=./your-file-name.csv
```

## Verification Checklist

Before coordinating with PC2, verify:

- [ ] Kafka running: `docker ps | findstr kafka`
- [ ] Kafka accessible: `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`
- [ ] NiFi UI loads: `http://localhost:8080/nifi`
- [ ] Kafka-UI loads: `http://localhost:8081`
- [ ] Data simulator works: `python scripts/nifi_simulator.py`
- [ ] Topic has messages: Check Kafka-UI or console consumer
- [ ] Firewall allows port 9092
- [ ] `.env` configured with both IPs

## Monitoring

### Check Container Status
```bash
docker-compose --profile pc1 ps
```

### View Logs
```bash
# Kafka logs
docker logs -f kafka

# NiFi logs  
docker logs -f nifi

# All PC1 services
docker-compose --profile pc1 logs -f
```

### Check Kafka Topics
```bash
# List all topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec kafka kafka-topics --describe --topic airline-delays --bootstrap-server localhost:9092

# Check message count (approximate)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic airline-delays
```

### Monitor Data Flow
```bash
# Watch messages in real-time
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays
```

## NiFi vs Simulator

### Option 1: NiFi (Production)
- Visual flow design
- Complex transformations
- Scheduling and monitoring
- Requires NiFi UI configuration

### Option 2: Simulator Script (Testing)
- Quick testing
- No UI needed
- Simple Python script
- Good for development

**Recommendation:** Use simulator for initial testing, migrate to NiFi for production.

## Performance Tips

1. **Kafka Optimization:**
   - Partitions: 3 (configured)
   - Retention: 7 days (168 hours)
   - Auto-create topics: enabled

2. **Network Performance:**
   - Use wired connection if possible
   - Ensure both PCs on same network
   - Consider static IPs for stability

3. **Resource Allocation:**
   - Kafka: 2GB RAM minimum
   - NiFi: 2GB RAM minimum
   - Docker Desktop: 6GB total recommended

## Next Steps

1. Verify all services running
2. Test data streaming with simulator
3. Share your PC1_IP with PC2 person
4. Coordinate with PC2: they need to update their `.env` with your IP
5. PC2 runs Script 1 to start consuming your Kafka messages
6. Monitor Kafka-UI to see consumption

## Working with PC2

Once PC2 is set up:

1. **PC2 updates their .env:**
   ```env
   PC1_IP=<your-ip>
   ```

2. **PC2 starts their services:**
   ```bash
   docker-compose --profile pc2 up -d
   ```

3. **PC2 runs consumer:**
   ```bash
   python scripts/script1_kafka_to_clickhouse.py
   ```

4. **You monitor on Kafka-UI:**
   - Check consumer groups
   - Verify lag is decreasing
   - See messages being consumed

## Support

- Review [PC1_COMPLETE_GUIDE.md](PC1_COMPLETE_GUIDE.md) for detailed documentation
- Check [README.md](README.md) for architecture overview
- Coordinate with PC2 person for IP configuration

## Quick Commands Reference

```bash
# Start services
docker-compose --profile pc1 up -d

# Stop services
docker-compose --profile pc1 down

# View logs
docker-compose --profile pc1 logs -f

# Restart specific service
docker-compose restart kafka

# Run simulator
python scripts/nifi_simulator.py

# Monitor Kafka
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays

# Check consumer groups (after PC2 connects)
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group airline-consumer-group
```
