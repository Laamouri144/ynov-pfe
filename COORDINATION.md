# Project Coordination - Both PCs

## Overview
This document helps coordinate the setup and operation between PC1 and PC2.

## Before You Start

### PC1 (Data Ingestion) - Current PC
**Status:** ✓ READY
- Docker services tested and working
- Kafka streaming verified
- 279 test messages sent successfully

### PC2 (Data Storage & Analytics) - Friend's PC
**Status:** Waiting for setup
- Clone repository from GitHub
- Follow [PC2_QUICK_START.md](PC2_QUICK_START.md)
- Run `test_pc2_setup.ps1` for automated verification

## Network Coordination

### Step 1: Exchange IP Addresses
**PC1 provides to PC2:**
```
PC1_IP=192.168.11.129  (already configured)
```

**PC2 provides to PC1:**
```
PC2_IP=<friend-will-provide>
```

### Step 2: Both Update .env Files
**PC1 updates:**
```ini
PC2_IP=<friend-provided-ip>
CLICKHOUSE_HOST=<friend-provided-ip>
```

**PC2 updates:**
```ini
PC1_IP=192.168.11.129
PC2_IP=<their-own-ip>
KAFKA_HOST=192.168.11.129
```

### Step 3: Firewall Configuration

**PC1 (Your PC):**
```powershell
# Allow Kafka access
New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow

# Allow NiFi (optional)
New-NetFirewallRule -DisplayName "NiFi" -Direction Inbound -LocalPort 8080 -Protocol TCP -Action Allow
```

**PC2 (Friend's PC):**
```powershell
# Allow ClickHouse
New-NetFirewallRule -DisplayName "ClickHouse HTTP" -Direction Inbound -LocalPort 8123 -Protocol TCP -Action Allow
New-NetFirewallRule -DisplayName "ClickHouse Native" -Direction Inbound -LocalPort 9000 -Protocol TCP -Action Allow

# Allow MongoDB
New-NetFirewallRule -DisplayName "MongoDB" -Direction Inbound -LocalPort 27017 -Protocol TCP -Action Allow
```

## Startup Sequence

### Phase 1: PC1 Startup (Already Done)
```bash
# Services running:
docker-compose --profile pc1 up -d

# Status check:
docker-compose ps
# ✓ zookeeper - Up
# ✓ kafka - Up
# ✓ nifi - Up
# ✓ kafka-ui - Up
```

### Phase 2: PC2 Startup (Friend Does This)
```bash
# 1. Clone repository
git clone <repository-url>
cd ynov-pfe

# 2. Configure .env
cp .env.example .env
# Edit with correct IPs

# 3. Start services
docker-compose --profile pc2 up -d

# 4. Verify
powershell -ExecutionPolicy Bypass -File test_pc2_setup.ps1
```

### Phase 3: Test Connectivity

**From PC1 to PC2:**
```powershell
# Load PC2 IP
$pc2_ip = (Get-Content .env | Select-String "^PC2_IP=").ToString().Split("=")[1].Trim()

# Test ClickHouse
Test-NetConnection -ComputerName $pc2_ip -Port 8123

# Should show: TcpTestSucceeded : True
```

**From PC2 to PC1:**
```powershell
# Test Kafka
Test-NetConnection -ComputerName 192.168.11.129 -Port 9092

# Should show: TcpTestSucceeded : True
```

### Phase 4: Start Data Flow

**PC1 starts sending data:**
```bash
# Option 1: Using simulator (easier)
python scripts/nifi_simulator.py

# Option 2: Using NiFi UI
# Access http://localhost:8080/nifi
```

**PC2 starts consuming data:**
```bash
# Terminal 1: Consume from Kafka → ClickHouse
python scripts/script1_kafka_to_clickhouse.py

# Terminal 2: Cache updates
python scripts/script2_clickhouse_to_mongodb.py

# Terminal 3: Dashboard
streamlit run scripts/streamlit_dashboard.py
```

## Verification Checklist

### After Both PCs Are Running

**PC1 Checks:**
- [ ] Kafka has messages: `docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --from-beginning --max-messages 1`
- [ ] Can reach PC2 ClickHouse: `Test-NetConnection -ComputerName <PC2_IP> -Port 8123`
- [ ] Simulator is sending data successfully

**PC2 Checks:**
- [ ] Can reach PC1 Kafka: `Test-NetConnection -ComputerName 192.168.11.129 -Port 9092`
- [ ] ClickHouse is receiving data: `docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM airline_data.flights"`
- [ ] MongoDB is updating: Check Mongo-Express at http://localhost:8082
- [ ] Dashboard shows data: http://localhost:8501

## Monitoring

### PC1 Monitoring
```bash
# Kafka messages count
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic airline-delays

# Kafka UI
http://localhost:8081

# NiFi UI
http://localhost:8080/nifi
```

### PC2 Monitoring
```bash
# ClickHouse row count
docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM airline_data.flights"

# MongoDB GUI
http://localhost:8082

# Streamlit Dashboard
http://localhost:8501
```

## Common Issues & Solutions

### Issue: PC2 can't connect to PC1 Kafka
**Symptoms:** Script 1 shows connection errors

**Solutions:**
1. **Verify PC1 IP is correct in PC2's .env**
2. **Check firewall on PC1:**
   ```powershell
   Get-NetFirewallRule -DisplayName "Kafka" | Select-Object DisplayName, Enabled, Action
   ```
3. **Test basic connectivity:**
   ```powershell
   ping 192.168.11.129
   Test-NetConnection -ComputerName 192.168.11.129 -Port 9092
   ```
4. **Check Kafka is listening on external IP:**
   ```bash
   docker exec kafka kafka-broker-api-versions --bootstrap-server 192.168.11.129:9092
   ```

### Issue: PC1 can't verify PC2 ClickHouse
**Symptoms:** Connection timeout to PC2

**Solutions:**
1. **Verify PC2 IP is correct**
2. **Check firewall on PC2**
3. **Verify ClickHouse is running:**
   ```bash
   docker exec clickhouse clickhouse-client --query "SELECT 1"
   ```

### Issue: No data flowing
**Symptoms:** ClickHouse table is empty

**Troubleshooting:**
1. **PC1: Check Kafka has messages:**
   ```bash
   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --max-messages 5
   ```

2. **PC2: Check Script 1 logs:**
   ```bash
   # If script is running, check logs/kafka_to_clickhouse.log
   Get-Content logs/kafka_to_clickhouse.log -Tail 50
   ```

3. **PC2: Manually test ClickHouse insert:**
   ```bash
   docker exec clickhouse clickhouse-client --query "INSERT INTO airline_data.flights (id, year, month, carrier) VALUES (999, 2024, 1, 'TEST')"
   docker exec clickhouse clickhouse-client --query "SELECT * FROM airline_data.flights WHERE id=999"
   ```

## Performance Optimization

### If Network is Slow
**PC1 adjustments:**
- Reduce simulator rate: Edit `nifi_simulator.py`, change `RECORDS_PER_SECOND`

**PC2 adjustments:**
- Increase batch size: Edit `script1_kafka_to_clickhouse.py`, increase `BATCH_SIZE`

### If ClickHouse is Slow
```sql
-- PC2: Create materialized views (already in init.sql)
-- Verify they exist:
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM airline_data"
```

### If MongoDB is Slow
```bash
# PC2: Verify indexes exist
docker exec mongodb mongosh --eval "db.getSiblingDB('airline_cache').aggregated_delays.getIndexes()"
```

## Work Division

### Daily Operations

**PC1 Person:**
- Keep Kafka running
- Monitor data flow (Kafka-UI)
- Restart simulator if needed
- Check logs periodically

**PC2 Person:**
- Monitor ClickHouse storage
- Run Script 2 for cache updates
- Build dashboards in Power BI
- Train ML models weekly

### Development Tasks

**Week 1-2: Setup & Testing**
- Both: Complete setup and verify connectivity
- PC1: Fine-tune data generation rate
- PC2: Verify database schemas

**Week 3: Visualization**
- PC1: Build Streamlit dashboard features
- PC2: Create Power BI reports

**Week 4: Machine Learning**
- PC2: Train and compare ML models
- Both: Evaluate performance

**Week 5: Optimization**
- Both: Performance tuning
- Both: Integration testing
- Both: Documentation updates

## Emergency Procedures

### If PC1 Goes Down
**Impact:** No new data flowing to Kafka

**Recovery:**
1. Restart Docker services: `docker-compose --profile pc1 up -d`
2. Verify Kafka: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
3. Resume simulator: `python scripts/nifi_simulator.py`

### If PC2 Goes Down
**Impact:** Data accumulates in Kafka, not persisted

**Recovery:**
1. Restart Docker services: `docker-compose --profile pc2 up -d`
2. Verify ClickHouse: `docker exec clickhouse clickhouse-client --query "SELECT 1"`
3. Resume Script 1: `python scripts/script1_kafka_to_clickhouse.py`
4. **Note:** Script 1 will catch up automatically from Kafka

### If Network Connection Drops
**Impact:** Data accumulates on PC1 Kafka

**Recovery:**
1. Fix network connection
2. PC2 Script 1 will automatically resume consuming from last offset
3. Check Kafka retention (168 hours configured) - no data loss

## Communication Template

**Setup Coordination Message:**
```
Hi! I've pushed the project to GitHub at <repository-url>

My PC (PC1) details:
- IP Address: 192.168.11.129
- Kafka Port: 9092
- Status: Ready and running

Your PC (PC2) setup:
1. Clone the repo: git clone <repository-url>
2. Follow PC2_QUICK_START.md
3. Run test_pc2_setup.ps1
4. Send me your PC2 IP address
5. I'll update my .env and we can test connectivity

Let me know when you're ready!
```

**Daily Status Update:**
```
PC1 Status:
- Kafka messages today: <count>
- Simulator status: Running
- Issues: None / <describe>

PC2 Status:
- ClickHouse rows: <count>
- Last cache update: <time>
- Dashboard: http://localhost:8501
- Issues: None / <describe>
```

## Success Criteria

### Phase 1: Setup Complete ✓
- [ ] Both PCs have Docker running
- [ ] Both can access their respective services
- [ ] Network connectivity verified

### Phase 2: Data Flowing
- [ ] Kafka shows messages
- [ ] ClickHouse shows growing row count
- [ ] MongoDB cache updates
- [ ] No errors in logs

### Phase 3: Visualization
- [ ] Streamlit dashboard displays real-time data
- [ ] Power BI connected to ClickHouse
- [ ] Interactive charts working

### Phase 4: ML Pipeline
- [ ] Models trained successfully
- [ ] Performance comparison complete
- [ ] Predictions stored in MongoDB

## Next Steps

1. **PC1:** Push to GitHub
2. **PC2:** Clone and setup
3. **Both:** Exchange IPs and test connectivity
4. **PC1:** Start data flow
5. **PC2:** Start consumers and monitoring
6. **Both:** Verify end-to-end pipeline
7. **Both:** Build visualizations and train models

Good luck! 🚀
