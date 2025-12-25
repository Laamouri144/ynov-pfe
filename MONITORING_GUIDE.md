# 🔍 Guide de Surveillance et Vérification du Pipeline

## ✅ Vérification Rapide (1 commande)

```bash
python scripts/verify_pipeline.py
```

Ce script vérifie automatiquement :
- ✓ Tous les conteneurs Docker
- ✓ Fichier CSV source
- ✓ Kafka et le topic airline-delays
- ✓ ClickHouse et les données
- ✓ MongoDB

## 📊 Vérifications Détaillées

### 1. État des Services Docker

```bash
# Voir tous les conteneurs
docker ps

# Voir les logs d'un service
docker logs kafka --tail 50
docker logs clickhouse --tail 50
docker logs mongodb --tail 50
docker logs nifi --tail 50
```

### 2. Monitoring Kafka

```bash
# Voir les messages dans Kafka (derniers 10)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --from-beginning --max-messages 10

# Voir les informations du topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic airline-delays

# Interface Web Kafka UI
# http://localhost:8081
```

### 3. Monitoring ClickHouse

```bash
# Nombre total d'enregistrements
docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM airline_data.flights"

# Statistiques en temps réel
docker exec clickhouse clickhouse-client --query "SELECT carrier_name, count(*) as flights, sum(arr_del15) as delays FROM airline_data.flights GROUP BY carrier_name ORDER BY flights DESC LIMIT 10" --format Pretty

# Derniers enregistrements insérés
docker exec clickhouse clickhouse-client --query "SELECT * FROM airline_data.flights ORDER BY ingestion_timestamp DESC LIMIT 5" --format Pretty

# Interface Web ClickHouse
# http://localhost:8123/play
```

### 4. Monitoring MongoDB

```bash
# Connexion au shell MongoDB
docker exec -it mongodb mongosh

# Dans mongosh:
use airline_cache
show collections
db.aggregated_data.countDocuments()
db.aggregated_data.find().limit(5).pretty()

# Interface Web Mongo Express
# http://localhost:8082 (admin/admin)
```

### 5. Monitoring NiFi

```bash
# Accès à l'interface NiFi
# http://localhost:8080/nifi
# User: admin / Password: adminadminadmin

# Logs NiFi
docker logs nifi --tail 100 -f
```

## 🚀 Commandes de Gestion

### Démarrer le Pipeline Complet

```bash
# 1. Démarrer tous les services
docker-compose up -d

# 2. Attendre que tout soit prêt (environ 2 minutes)
Start-Sleep -Seconds 120

# 3. Vérifier l'état
python scripts/verify_pipeline.py

# 4. Démarrer le simulateur (ou utiliser NiFi)
python scripts/nifi_simulator.py &

# 5. Démarrer le consumer Kafka -> ClickHouse
python scripts/script1_kafka_to_clickhouse.py &

# 6. (Optionnel) Démarrer ClickHouse -> MongoDB
python scripts/script2_clickhouse_to_mongodb.py &
```

### Arrêter le Pipeline

```bash
# Arrêter les scripts Python (Ctrl+C dans chaque terminal)

# Arrêter les conteneurs
docker-compose down

# Arrêter et supprimer les volumes (⚠️ EFFACE LES DONNÉES)
docker-compose down -v
```

### Redémarrer un Service

```bash
docker-compose restart kafka
docker-compose restart clickhouse
docker-compose restart mongodb
docker-compose restart nifi
```

## 📈 Surveillance en Temps Réel

### Voir les logs des scripts Python

```bash
# Script 1 (Kafka -> ClickHouse)
Get-Content logs/kafka_to_clickhouse.log -Wait -Tail 20

# Simulateur NiFi
Get-Content logs/nifi_simulator.log -Wait -Tail 20

# Script 2 (ClickHouse -> MongoDB)
Get-Content logs/clickhouse_to_mongodb.log -Wait -Tail 20
```

### Dashboard ClickHouse en Temps Réel

```bash
# Exécuter cette commande pour voir les stats toutes les 5 secondes
while ($true) {
    Clear-Host
    Write-Host "=== STATISTIQUES PIPELINE ===" -ForegroundColor Cyan
    Write-Host ""
    docker exec clickhouse clickhouse-client --query "
        SELECT 
            count(*) as total_records,
            count(DISTINCT carrier) as carriers,
            sum(arr_flights) as total_flights,
            sum(arr_del15) as delayed_flights,
            round(sum(arr_del15)*100.0/sum(arr_flights), 2) as delay_percentage
        FROM airline_data.flights
    " --format Pretty
    Start-Sleep -Seconds 5
}
```

## 🔧 Résolution de Problèmes

### Kafka : Topic vide ou pas de messages

```bash
# Vérifier que le simulateur/NiFi tourne
# Redémarrer le simulateur
python scripts/nifi_simulator.py
```

### ClickHouse : Pas de données

```bash
# Vérifier que script1 tourne
python scripts/script1_kafka_to_clickhouse.py

# Vérifier les logs
Get-Content logs/kafka_to_clickhouse.log -Tail 50
```

### MongoDB : Base vide

```bash
# C'est normal si script2 n'a pas encore été lancé
python scripts/script2_clickhouse_to_mongodb.py
```

### Conteneur ne démarre pas

```bash
# Voir les logs d'erreur
docker logs <container_name>

# Redémarrer le conteneur
docker-compose restart <service_name>

# En dernier recours : tout recréer
docker-compose down
docker-compose up -d
```

## 📊 Métriques Clés à Surveiller

### Performance
- **Débit Kafka** : Messages/seconde
- **Latence ClickHouse** : Temps d'insertion par batch
- **Utilisation CPU/RAM** : `docker stats`

### Données
- **Nombre d'enregistrements** : ClickHouse
- **Taux de retard moyen** : Calculs d'agrégation
- **Complétude des données** : Vérifier les valeurs NULL

### Santé du Système
- **Tous les conteneurs Up** : `docker ps`
- **Pas d'erreurs dans les logs**
- **Scripts Python actifs**

## 🎯 Checklist Quotidienne

- [ ] Vérifier que tous les conteneurs sont actifs
- [ ] Exécuter `python scripts/verify_pipeline.py`
- [ ] Vérifier le nombre d'enregistrements dans ClickHouse
- [ ] Consulter les logs pour détecter les erreurs
- [ ] Vérifier l'espace disque : `docker system df`

## 💡 Commandes Utiles Rapides

```bash
# Tout en un : statut complet
docker ps && echo "" && docker exec clickhouse clickhouse-client --query "SELECT count(*) as records FROM airline_data.flights"

# Nettoyer Docker (libérer espace)
docker system prune -f

# Backup des données ClickHouse
docker exec clickhouse clickhouse-client --query "SELECT * FROM airline_data.flights FORMAT CSVWithNames" > backup_$(date +%Y%m%d).csv
```
