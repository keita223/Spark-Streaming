# Compte Rendu - TP4 Spark Streaming

## Auteur
Réalisé dans le cadre du cours de Data Streaming - M2 IASD

Date: 18 Décembre 2025

---

## Objectifs du TP

✅ Disposer d'un pipeline Kafka → Spark Streaming fonctionnel
✅ Comprendre Spark Streaming dans ses grandes lignes
✅ Être à l'aise avec la lecture et la transformation des données en flux continu

---

## Architecture du Projet

```
Producteur Python (Windows)
    ↓
    → localhost:9092 (EXTERNAL)
    ↓
KAFKA (Docker)
    → kafka:29092 (INTERNAL)
    ↓
SPARK STREAMING (Docker)
    ↓
CONSOLE OUTPUT (filtré)
```

### Configuration Kafka - Double Listener

Pour permettre la communication entre:
- **Producteur Python (Windows)** → `localhost:9092`
- **Spark (Docker)** → `kafka:29092`

```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

---

## Commandes Importantes

### 1. Démarrage de l'Infrastructure

```bash
# Démarrer les conteneurs Docker (Zookeeper, Kafka, Spark)
docker-compose up -d

# Vérifier que les conteneurs sont en cours d'exécution
docker ps

# Arrêter les conteneurs
docker-compose down
```

### 2. Gestion de Kafka

```bash
# Lister les topics Kafka
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Créer un topic
docker exec kafka kafka-topics --create --topic test-topic --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

# Supprimer un topic
docker exec kafka kafka-topics --delete --topic test-topic --bootstrap-server kafka:29092

# Voir les détails d'un topic
docker exec kafka kafka-topics --describe --topic test-topic --bootstrap-server kafka:29092
```

### 3. Lancement du Pipeline

**Terminal 1 - Producteur Kafka:**
```bash
python kafka/producer.py
```

**Terminal 2 - Spark Streaming:**
```bash
docker exec spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
  /app/spark_kafka_stream.py
```

**Alternative - Entrer dans le conteneur Spark d'abord:**
```bash
# Étape 1: Entrer dans le conteneur
docker exec -it spark bash

# Étape 2: Lancer Spark
/opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
  /app/spark_kafka_stream.py
```

### 4. Commandes de Debugging

```bash
# Voir les logs d'un conteneur
docker logs kafka
docker logs spark
docker logs zookeeper

# Voir les logs en temps réel
docker logs -f kafka

# Vérifier l'état du réseau Docker
docker network ls
docker network inspect spark-streaming_default

# Tester la connectivité entre conteneurs
docker exec spark ping kafka
```

---

## Erreurs Courantes et Solutions

### ❌ Erreur 1: PySpark ne fonctionne pas sur Windows

**Erreur:**
```
AttributeError: module 'socketserver' has no attribute 'UnixStreamServer'
```

**Cause:**
PySpark utilise des dépendances Unix qui ne sont pas disponibles sur Windows.

**Solution:**
Exécuter Spark **dans le conteneur Docker** au lieu de Windows:
```bash
docker exec spark /opt/spark/bin/spark-submit ...
```

---

### ❌ Erreur 2: Version incompatible du package Kafka

**Erreur:**
```
NoClassDefFoundError: org/apache/spark/internal/LogKeys$NUM_RETRY
```

**Cause:**
Utilisation de `spark-sql-kafka-0-10_2.12:3.5.0` avec Spark 4.1.0

**Solution:**
Utiliser la version compatible:
```python
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0")
```

**Règle:**
- Spark 4.1.0 utilise **Scala 2.13** → package `_2.13:4.1.0`
- Format: `spark-sql-kafka-0-10_<scala-version>:<spark-version>`

---

### ❌ Erreur 3: Connection to node -1 (localhost/127.0.0.1:9092) could not be established

**Erreur:**
```
Connection to node 1 (localhost/127.0.0.1:9092) could not be established
```

**Cause:**
Kafka annonce `localhost:9092` mais Spark (dans Docker) cherche sur son propre conteneur.

**Solution 1 - Double Listener (RECOMMANDÉ):**
```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
```
- Producteur (Windows) → `localhost:9092`
- Spark (Docker) → `kafka:29092`

**Solution 2 - Tout dans Docker:**
```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
```
Mais le producteur doit aussi tourner dans Docker.

---

### ❌ Erreur 4: Permission denied - Ivy cache

**Erreur:**
```
FileNotFoundException: /nonexistent/.ivy2.5.2/cache/...
```

**Cause:**
Spark essaie d'écrire dans un répertoire non accessible.

**Solution:**
Spécifier un répertoire accessible:
```bash
--conf spark.jars.ivy=/tmp/.ivy2
```

---

### ❌ Erreur 5: Kafka timeout - Failed to update metadata

**Erreur:**
```
KafkaTimeoutError: Failed to update metadata after 60.0 secs
```

**Cause:**
Le producteur/consommateur ne peut pas se connecter à Kafka.

**Solutions:**
1. Vérifier que Kafka est démarré: `docker ps`
2. Vérifier que le topic existe: `docker exec kafka kafka-topics --list --bootstrap-server kafka:29092`
3. Vérifier la configuration des listeners dans docker-compose.yml
4. Attendre 10-15 secondes après `docker-compose up -d` avant de lancer les applications

---

## Structure du Code

### spark_kafka_stream.py

```python
# Configuration essentielle
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
    .master("local[*]") \
    .getOrCreate()

# Connexion à Kafka (depuis Docker)
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "test-topic"

# Lecture du stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Transformation (filtrage)
df_filtered = df_string.select("value", "timestamp") \
    .filter(col("value").contains("important"))

# Écriture du résultat
query = df_filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Attendre la fin
query.awaitTermination()
```

### kafka/producer.py

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # EXTERNAL listener
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

messages = [
    "This is an important message",
    "This is a regular message",
    "Another important update",
    "Just a normal event",
    "Critical important alert",
    "Random data here",
    "Important notification received"
]

# Envoi continu de messages
while True:
    message = random.choice(messages)
    producer.send('test-topic', value=message)
    time.sleep(2)
```

---

## Questions de Réflexion

### 1. En quoi Spark Streaming diffère-t-il d'un consommateur Kafka ?

**Consommateur Kafka classique:**
- Lit les messages un par un ou par petits lots
- Traitement simple et direct
- Logique de traitement écrite manuellement
- Gère manuellement les offsets et la parallélisation

**Spark Streaming:**
- Traite les données par **micro-batches** (petits lots toutes les quelques secondes)
- Fournit un **DataFrame API** pour des transformations complexes (filter, select, groupBy, etc.)
- **Parallélise automatiquement** le traitement sur plusieurs cœurs/machines
- Offre des garanties de tolérance aux pannes (peut reprendre après un crash)
- Permet des opérations avancées: agrégations, fenêtres temporelles, jointures

**En résumé:** Un consommateur Kafka lit des messages, Spark Streaming traite des flux de données avec des opérations complexes.

---

### 2. Quel rôle joue Kafka dans ce processus ?

Kafka agit comme un **tampon de messages distribué** (message buffer):

- **Stockage temporaire:** Garde les messages pendant une durée configurée (retention)
- **Découplage:** Les producteurs et consommateurs n'ont pas besoin d'être synchronisés
- **Résilience:** Si Spark tombe en panne, les messages restent dans Kafka
- **Rejouabilité:** Spark peut relire les messages depuis le début (`startingOffsets: "earliest"`)
- **Scalabilité:** Kafka peut gérer des millions de messages/seconde

**Analogie:** Kafka = la file d'attente dans un restaurant, Spark = le chef qui prend les commandes et les traite.

---

### 3. Quel rôle joue Spark ?

Spark est le **moteur de traitement et d'analyse** des données:

- **Transformation:** Applique des filtres, agrégations, calculs sur les données
- **Parallélisation:** Distribue le calcul sur plusieurs machines si nécessaire
- **État:** Peut maintenir un état entre les batches (compteurs, agrégations)
- **Sortie:** Écrit les résultats vers différentes destinations (console, fichiers, bases de données)

**Exemple concret du TP:**
```python
# Spark lit depuis Kafka
df = spark.readStream.format("kafka").load()

# Spark transforme les données
df_filtered = df.filter(col("value").contains("important"))

# Spark écrit le résultat
query = df_filtered.writeStream.format("console").start()
```

**Analogie:** Spark = usine de traitement qui transforme les matières premières (messages Kafka) en produits finis (résultats).

---

### 4. Pourquoi séparer le stockage des données (Kafka) du calcul (Spark) ?

Cette séparation suit le principe **"Separation of Concerns"** et apporte plusieurs avantages:

**a) Scalabilité indépendante:**
- Vous pouvez ajouter plus de brokers Kafka sans toucher à Spark
- Vous pouvez ajouter plus de workers Spark sans toucher à Kafka

**b) Flexibilité:**
- Plusieurs applications peuvent lire le même topic Kafka (Spark, Python consumer, autre système)
- Vous pouvez changer votre logique Spark sans perdre les données Kafka

**c) Résilience:**
- Si Spark tombe, Kafka continue à recevoir des messages
- Si Kafka redémarre, Spark peut reprendre là où il s'était arrêté

**d) Spécialisation:**
- Kafka est optimisé pour le stockage et la distribution de messages
- Spark est optimisé pour le calcul et les transformations complexes

**Exemple concret dans le TP:**
- Le `producer.py` continue d'envoyer des messages même si Spark n'est pas lancé
- Quand on démarre Spark avec `startingOffsets: "earliest"`, il peut lire tous les messages depuis le début
- On peut arrêter/redémarrer Spark sans perdre de données

---

## Concepts Clés Appris

### 1. Streaming DataFrame
Un DataFrame qui représente un flux continu de données. Il a le même API qu'un DataFrame batch, mais les données arrivent en continu.

### 2. Micro-batch
Spark Streaming traite les données par **petits lots** (par défaut toutes les quelques secondes), pas message par message.

### 3. Schema Kafka
Les messages Kafka apparaissent dans Spark avec cette structure:
```
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

### 4. Output Modes
- **append:** Ajoute seulement les nouvelles lignes (utilisé dans le TP)
- **complete:** Réémet tout le résultat à chaque batch
- **update:** Émet seulement les lignes modifiées

### 5. Checkpointing
Spark peut sauvegarder son état pour reprendre après un crash (non utilisé dans ce TP mais important en production).

---

## Bonnes Pratiques

### Configuration Docker

✅ **Utiliser des listeners séparés pour Kafka:**
```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
```

✅ **Monter le code en volume:**
```yaml
volumes:
  - ./:/app
```
Permet de modifier le code sans reconstruire l'image.

✅ **Attendre que Kafka soit prêt:**
```bash
sleep 15 && docker exec kafka kafka-topics --list ...
```

### Configuration Spark

✅ **Spécifier le cache Ivy:**
```bash
--conf spark.jars.ivy=/tmp/.ivy2
```

✅ **Utiliser la bonne version du package:**
```
spark-sql-kafka-0-10_2.13:4.1.0  # Pour Spark 4.1.0
```

✅ **Définir le log level:**
```python
spark.sparkContext.setLogLevel("WARN")
```

### Développement

✅ **Tester d'abord la connexion Kafka:**
```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

✅ **Utiliser startingOffsets: "earliest" pour le développement:**
```python
.option("startingOffsets", "earliest")
```
Permet de rejouer tous les messages.

✅ **Vérifier les logs en cas d'erreur:**
```bash
docker logs kafka
docker logs spark
```

---

## Résultats Obtenus

### Output du Producteur
```
📤 Starting to send messages to topic: test-topic
✅ Sent message 1: This is an important message
✅ Sent message 2: Just a normal event
✅ Sent message 3: Another important update
✅ Sent message 4: This is a regular message
✅ Sent message 5: Critical important alert
```

### Output de Spark (filtré)
```
-------------------------------------------
Batch: 0
-------------------------------------------
+---------------------------+-------------------+
|value                      |timestamp          |
+---------------------------+-------------------+
|This is an important message|2025-12-18 21:30:15|
|Another important update    |2025-12-18 21:30:19|
|Critical important alert    |2025-12-18 21:30:23|
+---------------------------+-------------------+
```

Seuls les messages contenant "important" sont affichés! ✅

---

## Améliorations Possibles

### 1. Agrégations
Compter le nombre de messages "important" par fenêtre de temps:
```python
from pyspark.sql.functions import window

df_windowed = df_filtered.groupBy(
    window("timestamp", "1 minute")
).count()
```

### 2. Parsing JSON
Si les messages sont en JSON:
```python
from pyspark.sql.functions import from_json, schema_of_json

schema = schema_of_json('{"user": "alice", "action": "click"}')
df_json = df.select(from_json(col("value"), schema).alias("data"))
```

### 3. Écriture vers fichiers
```python
query = df_filtered.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/app/output") \
    .option("checkpointLocation", "/app/checkpoint") \
    .start()
```

### 4. Multiple topics
```python
.option("subscribe", "topic1,topic2,topic3")
```

---

## Conclusion

Ce TP a permis de:

✅ Construire un pipeline Kafka → Spark Streaming complet
✅ Comprendre les différences entre stockage (Kafka) et traitement (Spark)
✅ Maîtriser la configuration Docker pour la communication inter-conteneurs
✅ Résoudre les problèmes courants de compatibilité et de connectivité
✅ Appliquer des transformations sur des flux de données en temps réel

**Points clés à retenir:**
- Spark Streaming utilise le même API que Spark batch
- Kafka et Spark sont deux systèmes complémentaires
- La configuration réseau est cruciale pour Docker
- Les micro-batches permettent un bon compromis entre latence et débit

---

## Références

- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Confluent Kafka Docker](https://docs.confluent.io/platform/current/installation/docker/config-reference.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

---

**Auteur:** Réalisé dans le cadre du TP4 - M2 IASD
**Date:** 18 Décembre 2025
