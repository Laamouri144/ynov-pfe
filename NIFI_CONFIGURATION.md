# Configuration NiFi pour le Pipeline de Données

## 📋 Prérequis
- NiFi est démarré : http://localhost:8080/nifi
- Credentials : admin / adminadminadmin
- Kafka est accessible sur localhost:9092

## 🔧 Configuration du Flow NiFi

### Étape 1 : Accéder à NiFi
1. Ouvrir http://localhost:8080/nifi dans votre navigateur
2. Se connecter avec :
   - Username: `admin`
   - Password: `adminadminadmin`

### Étape 2 : Créer le Flow de Lecture CSV → Kafka

#### A. Ajouter un Processeur GetFile
1. Glissez l'icône **Processor** sur le canvas
2. Cherchez et sélectionnez **GetFile**
3. Configurez :
   - **Input Directory** : `/data`
   - **File Filter** : `Airline_Delay_Cause.*\.csv`
   - **Keep Source File** : `true` (pour ne pas supprimer le fichier)
   - **Polling Interval** : `10 sec`
   - **Batch Size** : `10`

#### B. Ajouter SplitText (pour lire ligne par ligne)
1. Ajoutez un processeur **SplitText**
2. Configurez :
   - **Line Split Count** : `1`
   - **Header Line Count** : `1`
   - **Remove Trailing Newlines** : `true`

#### C. Ajouter ConvertRecord (CSV vers JSON)
1. Ajoutez un processeur **ConvertRecord**
2. Créez un **CSVReader** Controller Service :
   - Type : `CSVReader`
   - Schema Access Strategy : `Use String Fields From Header`
   - Treat First Line as Header : `true`
3. Créez un **JsonRecordSetWriter** Controller Service :
   - Type : `JsonRecordSetWriter`
   - Schema Write Strategy : `Do Not Write Schema`
   - Output Grouping : `One Line Per Object`

#### D. Ajouter UpdateAttribute (ajouter timestamp)
1. Ajoutez un processeur **UpdateAttribute**
2. Ajoutez une propriété :
   - **ingestion_timestamp** : `${now():format('yyyy-MM-dd HH:mm:ss')}`

#### E. Ajouter PublishKafka_2_6
1. Ajoutez un processeur **PublishKafka_2_6**
2. Configurez :
   - **Kafka Brokers** : `kafka:29092`
   - **Topic Name** : `airline-delays`
   - **Delivery Guarantee** : `Best Effort`
   - **Message Key Field** : (laisser vide)
   - **Use Transactions** : `false`

### Étape 3 : Connecter les Processeurs
1. GetFile → SplitText (relation: success)
2. SplitText → ConvertRecord (relation: splits)
3. ConvertRecord → UpdateAttribute (relation: success)
4. UpdateAttribute → PublishKafka (relation: success)
5. Pour chaque processeur, connecter les relations d'erreur (failure) vers un processeur LogAttribute ou auto-terminate

### Étape 4 : Activer les Controller Services
1. Clic droit sur le canvas → Configure
2. Onglet **Controller Services**
3. Activer tous les services (CSVReader, JsonRecordSetWriter)

### Étape 5 : Démarrer le Flow
1. Sélectionnez tous les processeurs (Ctrl+A)
2. Clic droit → Start
3. Vérifiez que les données circulent en regardant les compteurs sur les connexions

## 🔍 Vérification

### Vérifier que les données arrivent dans Kafka
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic airline-delays --from-beginning --max-messages 5
```

### Vérifier que script1 consomme les données
Le script `script1_kafka_to_clickhouse.py` doit être en cours d'exécution :
```bash
python scripts/script1_kafka_to_clickhouse.py
```

### Vérifier dans ClickHouse
```bash
docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM airline_data.flights"
```

## 📊 Schéma du CSV

Les colonnes du fichier CSV sont :
- year, month, carrier, carrier_name
- airport, airport_name
- arr_flights, arr_del15
- carrier_ct, weather_ct, nas_ct, security_ct, late_aircraft_ct
- arr_cancelled, arr_diverted, arr_delay
- carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay

## ⚠️ Résolution de Problèmes

### Le fichier n'est pas trouvé
- Vérifiez que le CSV est bien copié : `docker exec nifi ls -la /data/`
- Le chemin complet est : `/data/Airline_Delay_Cause - Airline_Delay_Cause.csv`

### Kafka n'est pas accessible
- Utilisez `kafka:29092` depuis NiFi (réseau Docker interne)
- NE PAS utiliser `localhost:9092` depuis NiFi

### Les données ne passent pas
- Vérifiez les bulletins (icône liste) en haut à droite de NiFi
- Consultez les logs : `docker logs nifi --tail 100`

## 🎯 Avantages de NiFi vs Simulateur

**NiFi :**
- ✅ Interface graphique professionnelle
- ✅ Monitoring en temps réel
- ✅ Gestion des erreurs sophistiquée
- ✅ Backpressure automatique
- ❌ Plus lourd (2-4 GB RAM)

**Simulateur Python :**
- ✅ Léger et rapide
- ✅ Code facile à comprendre/modifier
- ✅ Parfait pour développement/tests
- ❌ Moins "professionnel" visuellement

## 💡 Recommandation

Pour un PFE, utilisez :
1. **Simulateur** pendant le développement et les tests
2. **NiFi** pour la démonstration finale (impact visuel)
3. Documentez les deux approches dans votre rapport
