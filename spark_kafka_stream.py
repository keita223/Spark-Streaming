# Spark Streaming — Kafka to Spark Pipeline (Introduction Lab)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from utils.schema import get_transaction_schema


def main():
    """
    Pipeline Kafka → Spark Streaming.
    Lit les transactions JSON depuis Kafka, parse le schéma,
    et filtre les transactions avec un montant > 300.
    """

    # Création de la SparkSession
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session créée.")

    # Paramètres Kafka (réseau interne Docker)
    kafka_bootstrap_servers = "kafka:29092"
    kafka_topic = "test-topic"

    # Lecture du stream Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Connecté au topic Kafka.")
    print("Schéma brut Kafka :")
    df.printSchema()

    # Conversion bytes → string
    df_string = df.selectExpr("CAST(value AS STRING) AS json_value", "timestamp")

    # Parsing JSON avec le schéma des transactions
    schema = get_transaction_schema()
    df_parsed = df_string.select(
        from_json(col("json_value"), schema).alias("data"),
        col("timestamp")
    ).select("data.*", "timestamp")

    print("Schéma après parsing JSON :")
    df_parsed.printSchema()

    # Filtrage : transactions avec montant > 300
    df_filtered = df_parsed.filter(col("amount") > 300)
    print("Filtre appliqué : amount > 300")

    # Écriture vers la console
    query = df_filtered.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    print("Stream démarré — écriture vers la console...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
