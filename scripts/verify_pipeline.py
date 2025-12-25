"""
Script de Vérification du Pipeline
Vérifie que tous les services et le flux de données fonctionnent correctement
"""

import os
import sys
import time
from datetime import datetime
import clickhouse_connect
from confluent_kafka import Consumer, Producer, KafkaError
from pymongo import MongoClient
from loguru import logger

# Configuration
KAFKA_HOST = 'localhost'
KAFKA_PORT = '9092'
KAFKA_TOPIC = 'airline-delays'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")

def check_kafka():
    """Vérifier que Kafka fonctionne"""
    try:
        # Tester avec un producer
        producer = Producer({'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}'})
        test_msg = f"test-{datetime.now().isoformat()}"
        producer.produce('test-topic', test_msg.encode('utf-8'))
        producer.flush(timeout=5)
        logger.success("✓ Kafka est accessible et fonctionnel")
        return True
    except Exception as e:
        logger.error(f"✗ Kafka ERROR: {e}")
        return False

def check_kafka_topic():
    """Vérifier que le topic airline-delays existe et contient des messages"""
    try:
        consumer = Consumer({
            'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}',
            'group.id': 'verification-group',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([KAFKA_TOPIC])
        
        # Essayer de lire un message
        msg_count = 0
        start_time = time.time()
        while time.time() - start_time < 5:  # Timeout 5 secondes
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                msg_count += 1
                if msg_count >= 1:
                    break
        
        consumer.close()
        
        if msg_count > 0:
            logger.success(f"✓ Topic '{KAFKA_TOPIC}' contient des messages")
            return True
        else:
            logger.warning(f"⚠ Topic '{KAFKA_TOPIC}' existe mais est vide")
            return False
    except Exception as e:
        logger.error(f"✗ Kafka Topic ERROR: {e}")
        return False

def check_clickhouse():
    """Vérifier que ClickHouse fonctionne et contient des données"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username='default',
            password=''
        )
        
        # Vérifier la connexion
        result = client.query("SELECT 1")
        logger.success("✓ ClickHouse est accessible")
        
        # Vérifier la base de données
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows]
        if 'airline_data' in databases:
            logger.success("✓ Base de données 'airline_data' existe")
        else:
            logger.error("✗ Base de données 'airline_data' n'existe pas")
            return False
        
        # Vérifier les tables
        result = client.query("SHOW TABLES FROM airline_data")
        tables = [row[0] for row in result.result_rows]
        if 'flights' in tables:
            logger.success("✓ Table 'flights' existe")
        else:
            logger.error("✗ Table 'flights' n'existe pas")
            return False
        
        # Compter les enregistrements
        result = client.query("SELECT count(*) FROM airline_data.flights")
        count = result.result_rows[0][0]
        logger.success(f"✓ Table 'flights' contient {count:,} enregistrements")
        
        if count > 0:
            # Afficher quelques statistiques
            result = client.query("""
                SELECT 
                    carrier, 
                    carrier_name, 
                    count(*) as flights,
                    sum(arr_del15) as delayed
                FROM airline_data.flights 
                GROUP BY carrier, carrier_name 
                ORDER BY flights DESC 
                LIMIT 3
            """)
            logger.info("  Top 3 compagnies:")
            for row in result.result_rows:
                logger.info(f"    - {row[1]} ({row[0]}): {row[2]:,} vols, {row[3]:,} retards")
        
        return count > 0
    except Exception as e:
        logger.error(f"✗ ClickHouse ERROR: {e}")
        return False

def check_mongodb():
    """Vérifier que MongoDB fonctionne"""
    try:
        client = MongoClient(f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/', serverSelectionTimeoutMS=5000)
        
        # Vérifier la connexion
        client.admin.command('ping')
        logger.success("✓ MongoDB est accessible")
        
        # Vérifier la base de données
        db = client['airline_cache']
        collections = db.list_collection_names()
        
        if collections:
            logger.success(f"✓ Base 'airline_cache' contient {len(collections)} collection(s)")
            for coll_name in collections:
                count = db[coll_name].count_documents({})
                logger.info(f"    - {coll_name}: {count:,} documents")
        else:
            logger.warning("⚠ Base 'airline_cache' est vide (normal si script2 n'a pas encore tourné)")
        
        client.close()
        return True
    except Exception as e:
        logger.error(f"✗ MongoDB ERROR: {e}")
        return False

def check_docker_containers():
    """Vérifier que tous les conteneurs Docker sont actifs"""
    import subprocess
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'],
            capture_output=True,
            text=True,
            check=True
        )
        
        containers = {}
        for line in result.stdout.strip().split('\n'):
            if '\t' in line:
                name, status = line.split('\t', 1)
                containers[name] = status
        
        required_containers = ['kafka', 'zookeeper', 'clickhouse', 'mongodb', 'nifi']
        all_running = True
        
        for container in required_containers:
            if container in containers:
                if 'Up' in containers[container]:
                    logger.success(f"✓ Container '{container}' est actif: {containers[container]}")
                else:
                    logger.error(f"✗ Container '{container}' n'est pas actif: {containers[container]}")
                    all_running = False
            else:
                logger.error(f"✗ Container '{container}' n'existe pas")
                all_running = False
        
        return all_running
    except Exception as e:
        logger.error(f"✗ Docker ERROR: {e}")
        return False

def check_csv_file():
    """Vérifier que le fichier CSV existe"""
    csv_path = './data/Airline_Delay_Cause - Airline_Delay_Cause.csv'
    if os.path.exists(csv_path):
        size = os.path.getsize(csv_path)
        logger.success(f"✓ Fichier CSV existe ({size:,} bytes)")
        return True
    else:
        logger.error(f"✗ Fichier CSV introuvable: {csv_path}")
        return False

def main():
    """Exécuter toutes les vérifications"""
    logger.info("=" * 60)
    logger.info("VÉRIFICATION DU PIPELINE - Airline Delay Data")
    logger.info("=" * 60)
    
    results = {}
    
    # Vérifications
    logger.info("\n[1/7] Vérification des conteneurs Docker...")
    results['docker'] = check_docker_containers()
    
    logger.info("\n[2/7] Vérification du fichier CSV...")
    results['csv'] = check_csv_file()
    
    logger.info("\n[3/7] Vérification de Kafka...")
    results['kafka'] = check_kafka()
    
    logger.info("\n[4/7] Vérification du topic Kafka...")
    results['kafka_topic'] = check_kafka_topic()
    
    logger.info("\n[5/7] Vérification de ClickHouse...")
    results['clickhouse'] = check_clickhouse()
    
    logger.info("\n[6/7] Vérification de MongoDB...")
    results['mongodb'] = check_mongodb()
    
    # Résumé
    logger.info("\n" + "=" * 60)
    logger.info("RÉSUMÉ")
    logger.info("=" * 60)
    
    passed = sum(results.values())
    total = len(results)
    
    for name, status in results.items():
        icon = "✓" if status else "✗"
        level = "success" if status else "error"
        getattr(logger, level)(f"{icon} {name.upper()}: {'OK' if status else 'ÉCHEC'}")
    
    logger.info(f"\nScore: {passed}/{total} tests réussis")
    
    if passed == total:
        logger.success("\n🎉 Tous les composants fonctionnent correctement!")
        logger.info("\n📊 Pipeline actuel:")
        logger.info("   CSV → Simulateur/NiFi → Kafka → Script1 → ClickHouse → Script2 → MongoDB")
    else:
        logger.warning(f"\n⚠️  {total - passed} composant(s) en échec")
        logger.info("\nActions recommandées:")
        if not results.get('docker'):
            logger.info("  - Démarrer les conteneurs: docker-compose up -d")
        if not results.get('kafka_topic'):
            logger.info("  - Démarrer le simulateur: python scripts/nifi_simulator.py")
        if not results.get('clickhouse') or not results['kafka_topic']:
            logger.info("  - Démarrer script1: python scripts/script1_kafka_to_clickhouse.py")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
