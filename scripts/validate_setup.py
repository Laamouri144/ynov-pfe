"""
System Validation Script
Run this on PC1 or PC2 to verify your setup is correct
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def print_success(msg):
    print(f"{GREEN}✓ {msg}{RESET}")

def print_error(msg):
    print(f"{RED}✗ {msg}{RESET}")

def print_warning(msg):
    print(f"{YELLOW}⚠ {msg}{RESET}")

def print_info(msg):
    print(f"{BLUE}ℹ {msg}{RESET}")

def check_env_file():
    """Check if .env file exists and is configured"""
    print("\n" + "="*60)
    print("1. Checking Environment Configuration")
    print("="*60)
    
    if not Path('.env').exists():
        print_error(".env file not found!")
        print_info("Run: cp .env.example .env")
        return False
    
    print_success(".env file exists")
    
    load_dotenv()
    pc_role = os.getenv('PC_ROLE')
    pc1_ip = os.getenv('PC1_IP')
    pc2_ip = os.getenv('PC2_IP')
    
    if not pc_role:
        print_error("PC_ROLE not set in .env")
        return False
    
    print_success(f"PC_ROLE = {pc_role}")
    print_info(f"PC1_IP = {pc1_ip}")
    print_info(f"PC2_IP = {pc2_ip}")
    
    if pc1_ip == "192.168.1.100" or pc2_ip == "192.168.1.101":
        print_warning("Using default IPs - make sure to update with actual IPs!")
    
    return True, pc_role

def check_docker():
    """Check if Docker is installed and running"""
    print("\n" + "="*60)
    print("2. Checking Docker")
    print("="*60)
    
    try:
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, check=True)
        print_success(f"Docker installed: {result.stdout.strip()}")
    except Exception as e:
        print_error(f"Docker not found: {e}")
        return False
    
    try:
        result = subprocess.run(['docker', 'ps'], 
                              capture_output=True, text=True, check=True)
        print_success("Docker daemon is running")
    except Exception as e:
        print_error(f"Docker daemon not running: {e}")
        return False
    
    return True

def check_docker_compose():
    """Check if docker-compose exists"""
    print("\n" + "="*60)
    print("3. Checking Docker Compose")
    print("="*60)
    
    try:
        result = subprocess.run(['docker-compose', '--version'], 
                              capture_output=True, text=True, check=True)
        print_success(f"Docker Compose: {result.stdout.strip()}")
        return True
    except Exception:
        print_error("docker-compose command not found")
        print_info("Try: docker compose --version (newer Docker versions)")
        return False

def check_pc1_services():
    """Check PC1 specific services"""
    print("\n" + "="*60)
    print("4. Checking PC1 Services")
    print("="*60)
    
    services = ['kafka', 'zookeeper', 'nifi', 'kafka-ui']
    all_running = True
    
    for service in services:
        try:
            result = subprocess.run(['docker', 'ps', '--filter', f'name={service}', '--format', '{{.Names}}'], 
                                  capture_output=True, text=True, check=True)
            if service in result.stdout:
                print_success(f"{service} container is running")
            else:
                print_error(f"{service} container not running")
                all_running = False
        except Exception as e:
            print_error(f"Error checking {service}: {e}")
            all_running = False
    
    if not all_running:
        print_info("Start services with: docker-compose --profile pc1 up -d")
    
    return all_running

def check_pc2_services():
    """Check PC2 specific services"""
    print("\n" + "="*60)
    print("4. Checking PC2 Services")
    print("="*60)
    
    services = ['clickhouse', 'mongodb', 'mongo-express']
    all_running = True
    
    for service in services:
        try:
            result = subprocess.run(['docker', 'ps', '--filter', f'name={service}', '--format', '{{.Names}}'], 
                                  capture_output=True, text=True, check=True)
            if service in result.stdout:
                print_success(f"{service} container is running")
            else:
                print_error(f"{service} container not running")
                all_running = False
        except Exception as e:
            print_error(f"Error checking {service}: {e}")
            all_running = False
    
    if not all_running:
        print_info("Start services with: docker-compose --profile pc2 up -d")
    
    return all_running

def check_python_env():
    """Check Python environment"""
    print("\n" + "="*60)
    print("5. Checking Python Environment")
    print("="*60)
    
    python_version = sys.version_info
    print_success(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version < (3, 9):
        print_error("Python 3.9+ required!")
        return False
    
    # Check if in virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print_success("Running in virtual environment")
    else:
        print_warning("Not in virtual environment")
        print_info("Recommended: python -m venv venv && venv\\Scripts\\activate")
    
    return True

def check_python_packages(pc_role):
    """Check required Python packages"""
    print("\n" + "="*60)
    print("6. Checking Python Packages")
    print("="*60)
    
    if pc_role == "PC1":
        packages = ['dotenv', 'confluent_kafka', 'clickhouse_connect', 'loguru']
        requirements_file = "requirements-pc1.txt"
    else:
        packages = ['dotenv', 'confluent_kafka', 'clickhouse_connect', 'pymongo', 
                   'loguru', 'streamlit', 'pandas', 'numpy']
        requirements_file = "requirements.txt"
    
    all_installed = True
    for package in packages:
        try:
            __import__(package.replace('_', '-'))
            print_success(f"{package} installed")
        except ImportError:
            print_error(f"{package} not installed")
            all_installed = False
    
    if not all_installed:
        print_info(f"Install packages: pip install -r {requirements_file}")
    
    return all_installed

def check_dataset():
    """Check if dataset exists (PC1 only)"""
    print("\n" + "="*60)
    print("7. Checking Dataset")
    print("="*60)
    
    csv_file = os.getenv('CSV_FILE_PATH', './Airline_Delay_Cause - Airline_Delay_Cause.csv')
    
    if Path(csv_file).exists():
        size_mb = Path(csv_file).stat().st_size / (1024 * 1024)
        print_success(f"Dataset found: {csv_file} ({size_mb:.2f} MB)")
        return True
    else:
        print_error(f"Dataset not found: {csv_file}")
        print_info("Make sure to add the airline delay CSV file")
        return False

def check_network_connectivity(pc_role):
    """Check network connectivity"""
    print("\n" + "="*60)
    print("8. Checking Network Connectivity")
    print("="*60)
    
    pc1_ip = os.getenv('PC1_IP')
    pc2_ip = os.getenv('PC2_IP')
    
    if pc_role == "PC1":
        # PC1 checks if Kafka port is accessible
        print_info(f"Checking if Kafka port 9092 is accessible...")
        try:
            result = subprocess.run(['Test-NetConnection', '-ComputerName', 'localhost', 
                                   '-Port', '9092', '-InformationLevel', 'Quiet'], 
                                  capture_output=True, text=True, shell=True)
            if 'True' in result.stdout:
                print_success("Kafka port 9092 is accessible")
            else:
                print_warning("Kafka port 9092 not accessible - make sure Kafka is running")
        except Exception as e:
            print_warning(f"Could not test Kafka port: {e}")
    
    else:  # PC2
        # PC2 checks if can reach PC1 Kafka
        print_info(f"Checking connection to PC1 Kafka at {pc1_ip}:9092...")
        try:
            result = subprocess.run(['Test-NetConnection', '-ComputerName', pc1_ip, 
                                   '-Port', '9092', '-InformationLevel', 'Quiet'], 
                                  capture_output=True, text=True, shell=True)
            if 'True' in result.stdout:
                print_success(f"Can reach PC1 Kafka at {pc1_ip}:9092")
            else:
                print_error(f"Cannot reach PC1 Kafka at {pc1_ip}:9092")
                print_info("Make sure PC1 has Kafka running and firewall allows port 9092")
        except Exception as e:
            print_warning(f"Could not test PC1 connection: {e}")
    
    return True

def main():
    print(f"\n{BLUE}{'='*60}")
    print("  AIRLINE DELAY PIPELINE - SYSTEM VALIDATION")
    print(f"{'='*60}{RESET}\n")
    
    # Check environment
    env_ok = check_env_file()
    if not env_ok:
        print_error("\nEnvironment configuration failed!")
        return
    
    env_ok, pc_role = env_ok
    print_info(f"\nValidating setup for: {pc_role}")
    
    # Check Docker
    if not check_docker():
        print_error("\nDocker check failed!")
        return
    
    check_docker_compose()
    
    # Check services based on PC role
    if pc_role == "PC1":
        check_pc1_services()
        check_dataset()
    else:
        check_pc2_services()
    
    # Check Python
    check_python_env()
    check_python_packages(pc_role)
    
    # Check network
    check_network_connectivity(pc_role)
    
    print(f"\n{BLUE}{'='*60}")
    print("  VALIDATION COMPLETE")
    print(f"{'='*60}{RESET}\n")
    
    print_info("Next steps:")
    if pc_role == "PC1":
        print("  1. Start streaming: python scripts/nifi_simulator.py")
        print("  2. Monitor Kafka-UI: http://localhost:8081")
        print("  3. Share your PC1_IP with PC2 person")
    else:
        print("  1. Start consumer: python scripts/script1_kafka_to_clickhouse.py")
        print("  2. Check ClickHouse: http://localhost:8123/ping")
        print("  3. Monitor MongoDB: http://localhost:8082")

if __name__ == "__main__":
    main()
