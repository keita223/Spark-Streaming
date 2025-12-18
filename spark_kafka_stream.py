## Spark Streaming — Kafka to Spark Pipeline (Introduction Lab)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    """
    Main entry point of the Spark Streaming application.
    """

    # TODO 2: Create a SparkSession
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark Session created successfully!")

    # TODO 3: Define Kafka connection parameters
    kafka_bootstrap_servers = "kafka:29092"
    kafka_topic = "test-topic"
    


    # TODO 4: Read data from Kafka as a streaming DataFrame
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    print(" Connected to Kafka topic successfully!")
   


    # TODO 5: Inspect the schema of the streaming DataFrame
    print(" Schema of Kafka DataFrame:")
    df.printSchema()



    # TODO 6: Convert the Kafka message value from bytes to string
    from pyspark.sql.functions import col

    df_string = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

    print(" Kafka messages converted to string format!")
   


    # TODO 7: Apply a simple transformation
    # Filter: keep only messages containing "important" (case insensitive)
    df_filtered = df_string.select("value", "timestamp") \
        .filter(col("value").contains("important"))

    print("✅ Filter applied: keeping only messages containing 'important'")

  


    # TODO 8: Write the streaming output
    query = df_filtered.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    print(" Streaming query started! Writing to console...")
   


    # TODO 9: Keep the streaming query running
    query.awaitTermination()
   


    # TODO 10: Gracefully stop the Spark session
    spark.stop()
    print(" Spark session stopped.")



if __name__ == "__main__":
    main()