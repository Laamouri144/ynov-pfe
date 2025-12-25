# 📥 Guide d'Import du Flow NiFi

## 🎯 Vue d'ensemble

Vous avez un flow NiFi pré-configuré qui :
- ✅ Lit le fichier CSV `Airline_Delay_Cause`
- ✅ Sépare les lignes
- ✅ Convertit CSV → JSON
- ✅ Ajoute un timestamp
- ✅ Envoie à Kafka (topic: airline-delays)

## 📋 Méthode 1 : Import via l'Interface Web (RECOMMANDÉ)

### Étape 1 : Accéder à NiFi
1. Ouvrir votre navigateur
2. Aller à : **http://localhost:8080/nifi**
3. Login avec :
   - **Username** : `admin`
   - **Password** : `adminadminadmin`

### Étape 2 : Importer le Flow
1. **Clic droit** sur le canvas (la zone de travail blanche)
2. Sélectionner **"Upload Process Group"**
3. Cliquer sur **"Select Process Group"**
4. Naviguer vers : `C:\Users\hp\ynov-pfe\scripts\airlineflow (1).json`
5. Cliquer sur **"Upload"**
6. Glisser et déposer le Process Group sur le canvas

### Étape 3 : Configurer le GetFile Processor
1. **Double-cliquer** sur le Process Group "airlineflow"
2. Localiser le processeur **"GetFile"** (rectangle bleu)
3. **Clic droit** sur GetFile → **Configure**
4. Onglet **"Properties"**
5. Vérifier/Modifier :
   - **Input Directory** : `/data`
   - **File Filter** : `Airline_Delay_Cause.*\.csv`
   - **Keep Source File** : `true`
6. Cliquer **Apply**

### Étape 4 : Activer les Controller Services
1. **Clic droit** sur le canvas → **Configure**
2. Aller à l'onglet **"Controller Services"**
3. Vous verrez 2 services :
   - **CSVReader** (lit le CSV)
   - **JsonRecordSetWriter** (écrit en JSON)
4. Pour chaque service :
   - Cliquer sur l'icône **éclair** (⚡) à droite
   - Sélectionner **"Enable"**
   - Attendre que le statut devienne **"Enabled"** (vert)
5. Cliquer **Close**

### Étape 5 : Démarrer le Flow
1. **Clic droit** sur le Process Group "airlineflow"
2. Sélectionner **"Start"**

OU

1. Ouvrir le Process Group (double-clic)
2. Sélectionner tous les processeurs : **Ctrl + A**
3. **Clic droit** → **Start**

### Étape 6 : Surveiller l'Exécution
- Les chiffres sur les connexions indiquent le flux de données
- Les processeurs actifs ont un symbole ▶️ vert
- Vérifier les bulletins en haut à droite (icône 📋)

## 📋 Méthode 2 : Import via Script PowerShell

```powershell
.\import_nifi_flow.ps1
```

Puis suivre les instructions affichées.

## 🔧 Configuration du Flow

### Structure du Flow
```
GetFile
   ↓
SplitText (sépare les lignes)
   ↓
ConvertRecord (CSV → JSON)
   ↓
UpdateAttribute (ajoute timestamp)
   ↓
PublishKafka (envoie à Kafka)
   ↓
Success/Failure Funnels
```

### Configuration Kafka
- **Kafka Brokers** : `kafka:29092` (réseau Docker interne)
- **Topic** : `airline-delays`
- **Compression** : gzip
- **Delivery Guarantee** : Best Effort (acks=0)

### Configuration CSV
- **Format** : RFC-4180
- **Separator** : `,`
- **Header** : Première ligne traitée comme header
- **Encoding** : UTF-8

## ✅ Vérification

### 1. Vérifier que NiFi envoie des données
```bash
# Dans NiFi, regarder les compteurs sur les connexions
# Le processeur PublishKafka doit montrer des messages envoyés
```

### 2. Vérifier les messages dans Kafka
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --max-messages 5
```

### 3. Vérifier l'insertion dans ClickHouse
```bash
# S'assurer que script1 est en cours d'exécution
python scripts/script1_kafka_to_clickhouse.py

# Vérifier les données
docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM airline_data.flights"
```

## 🔍 Dépannage

### Problème : Le fichier n'est pas trouvé
**Solution** :
```bash
# Vérifier que le fichier existe dans le conteneur
docker exec nifi ls -la /data/

# Le fichier doit être : /data/Airline_Delay_Cause - Airline_Delay_Cause.csv
```

### Problème : Les Controller Services ne s'activent pas
**Solution** :
1. Vérifier qu'il n'y a pas d'erreurs dans les propriétés
2. Dans l'onglet Controller Services, cliquer sur l'icône "⚠️" pour voir l'erreur
3. Corriger les propriétés manquantes

### Problème : Kafka n'est pas accessible
**Solution** :
1. Vérifier la configuration de PublishKafka
2. S'assurer d'utiliser `kafka:29092` (pas `localhost:9092`)
3. Vérifier que Kafka est actif : `docker ps | grep kafka`

### Problème : Aucune donnée ne circule
**Solution** :
1. Vérifier les bulletins (icône 📋 en haut à droite)
2. Consulter les logs : `docker logs nifi --tail 100`
3. Vérifier le processeur GetFile :
   - Est-il en cours d'exécution ?
   - Le répertoire est-il correct ?
   - Le fichier est-il présent ?

## 🎯 Après l'Import

### Arrêter le Simulateur
Si vous utilisez NiFi, arrêtez le simulateur Python :
```bash
# Appuyer sur Ctrl+C dans le terminal où tourne le simulateur
```

### Garder Script1 Actif
Le script `script1_kafka_to_clickhouse.py` doit continuer à tourner :
```bash
python scripts/script1_kafka_to_clickhouse.py
```

### Architecture Finale
```
CSV → NiFi → Kafka → Script1 → ClickHouse → Script2 → MongoDB
```

## 📊 Interfaces de Monitoring

- **NiFi** : http://localhost:8080/nifi
- **Kafka UI** : http://localhost:8081
- **ClickHouse** : http://localhost:8123/play
- **Mongo Express** : http://localhost:8082

## 💡 Conseils

1. **Débit** : Ajustez le "Run Schedule" de GetFile (par défaut 60 sec)
2. **Logs** : Surveillez les bulletins NiFi pour les erreurs
3. **Backpressure** : Si les connexions se remplissent, augmentez les seuils
4. **Performance** : NiFi consomme ~2-4 GB RAM, surveillez les ressources

## 🔄 Comparaison Simulateur vs NiFi

| Aspect | Simulateur | NiFi |
|--------|-----------|------|
| Ressources | Léger (~100 MB) | Lourd (~2-4 GB) |
| Configuration | Code Python | Interface graphique |
| Démarrage | Immédiat | 2-3 minutes |
| Monitoring | Logs | Interface visuelle |
| Flexibilité | Code modifiable | Processeurs configurables |
| Production | Tests/Dev | Production |

## 📝 Résumé

1. ✅ Accéder à http://localhost:8080/nifi
2. ✅ Importer `airlineflow (1).json`
3. ✅ Activer les Controller Services
4. ✅ Démarrer le flow
5. ✅ Vérifier dans Kafka UI et ClickHouse

**Le flow est prêt à l'emploi !** 🎉
