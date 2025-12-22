# GitHub Setup Guide

## Overview
This repository uses a **unified Docker Compose** with profiles allowing each person to run only their required services on their respective PC.

## Before Pushing to GitHub

### 1. Verify .gitignore
```bash
# Check that sensitive files are excluded
cat .gitignore
```

Ensure these are excluded:
- `.env` (environment variables)
- `venv/` (Python virtual environment)
- `*.csv` (large dataset files)
- Docker volume data

### 2. Clean Project
```bash
# Remove any temporary files
Get-ChildItem -Recurse -Include __pycache__,*.pyc,*.log | Remove-Item -Force -Recurse
```

### 3. Test Docker Configuration
```bash
# Validate Docker Compose syntax
docker-compose config

# Test PC1 profile
docker-compose --profile pc1 config

# Test PC2 profile  
docker-compose --profile pc2 config
```

## Initialize Git Repository

```bash
# Initialize repository
git init

# Add all files
git add .

# First commit
git commit -m "Initial commit: Real-time airline delay data pipeline

- Unified Docker Compose with PC1/PC2 profiles
- Data ingestion: NiFi, Kafka, Zookeeper (PC1)
- Data storage: ClickHouse, MongoDB (PC2)
- Python scripts for ETL and ML
- Complete documentation guides"

# View what will be committed
git status
```

## Create GitHub Repository

### Option 1: Via GitHub Web Interface
1. Go to github.com and login
2. Click "New Repository"
3. Name: `airline-delay-pipeline` (or your choice)
4. Description: "Real-time data pipeline for airline delay analysis"
5. Visibility: Private or Public
6. **Do NOT** initialize with README (we already have one)
7. Create repository

### Option 2: Via GitHub CLI (if installed)
```bash
gh repo create airline-delay-pipeline --private --source=. --remote=origin
```

## Push to GitHub

```bash
# Add remote repository
git remote add origin https://github.com/YOUR_USERNAME/airline-delay-pipeline.git

# Push to main branch
git branch -M main
git push -u origin main
```

## Clone on Friend's PC (PC2)

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/airline-delay-pipeline.git
cd airline-delay-pipeline

# Copy and configure environment
cp .env.example .env
# Edit .env with both PC IPs:
#   PC_ROLE=PC2
#   PC1_IP=<friend-ip>
#   PC2_IP=<your-ip>

# Start only PC2 services
docker-compose --profile pc2 up -d

# Setup Python environment
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Start consuming from PC1's Kafka
python scripts/script1_kafka_to_clickhouse.py
```

**Quick Setup Guides:**
- PC1: See [PC1_SETUP.md](PC1_SETUP.md)
- PC2: See [PC2_SETUP.md](PC2_SETUP.md)

## How It Works

### Unified Docker Architecture
The project uses **Docker Compose profiles** to separate services:

```yaml
services:
  zookeeper:
    profiles:
      - pc1  # Only runs on PC1
  
  kafka:
    profiles:
      - pc1  # Only runs on PC1
  
  clickhouse:
    profiles:
      - pc2  # Only runs on PC2
```

### Running Services by Profile

**PC1 (Your PC):**
```bash
docker-compose --profile pc1 up -d
```
Starts: Zookeeper, Kafka, NiFi, Kafka-UI

**PC2 (Friend's PC):**
```bash
docker-compose --profile pc2 up -d
```
Starts: ClickHouse, MongoDB, Mongo-Express

### Benefits
- ✓ Single repository for both PCs
- ✓ Unified version control
- ✓ Easy collaboration
- ✓ Each PC runs only what it needs
- ✓ No confusion about which services go where

## Dataset Handling

The dataset (41MB CSV) is excluded via `.gitignore`:
```
*.csv
```

### Option 1: Share Dataset Separately
- Upload to Google Drive / Dropbox
- Share link in README
- Each person downloads manually

### Option 2: Use Git LFS (Large File Storage)
```bash
# Install Git LFS
git lfs install

# Track CSV files
git lfs track "*.csv"
git add .gitattributes
git commit -m "Track CSV files with Git LFS"
```

### Option 3: Sample Dataset in Repo
Create a small sample for testing:
```bash
# Create sample (first 1000 rows)
head -n 1000 "Airline_Delay_Cause - Airline_Delay_Cause.csv" > data/raw/sample.csv
git add data/raw/sample.csv
```

## Collaboration Workflow

### PC1 Person
1. Clone repo
2. Set PC1_IP and PC2_IP in `.env`
3. Run: `docker-compose --profile pc1 up -d`
4. Start data streaming: `python scripts/nifi_simulator.py`

### PC2 Person
1. Clone repo
2. Set PC1_IP and PC2_IP in `.env`
3. Run: `docker-compose --profile pc2 up -d`
4. Start consumer: `python scripts/script1_kafka_to_clickhouse.py`

### Making Changes
```bash
# Pull latest changes
git pull

# Make your changes
# ...

# Commit and push
git add .
git commit -m "Description of changes"
git push
```

## Important Notes

1. **Never commit `.env`** - Contains IP addresses and secrets
2. **Always use `.env.example`** - Template without sensitive data
3. **Document IP changes** - Update guides if IPs change
4. **Test before pushing** - Run `docker-compose config` to validate
5. **Communicate with friend** - Coordinate who works on what

## Troubleshooting

### Problem: Git shows too many files
**Solution:** Check `.gitignore` includes `venv/` and `__pycache__/`

### Problem: Docker Compose fails after clone
**Solution:** Ensure `.env` is created from `.env.example`

### Problem: Services conflict
**Solution:** Each PC should only run their profile:
- PC1: `--profile pc1`
- PC2: `--profile pc2`

## Next Steps

After both clone the repository:
1. Configure `.env` on both PCs with correct IPs
2. PC1: Test Kafka connection
3. PC2: Test ClickHouse and MongoDB
4. Verify network connectivity between PCs
5. Run end-to-end pipeline test

## Support Documentation

- [PC1_COMPLETE_GUIDE.md](PC1_COMPLETE_GUIDE.md) - Comprehensive PC1 setup
- [PC2_COMPLETE_GUIDE.md](PC2_COMPLETE_GUIDE.md) - Comprehensive PC2 setup
- [README.md](README.md) - Project overview
