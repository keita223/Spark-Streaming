import sys
import os
import time
from kafka import KafkaProducer

# Ajoute la racine du projet au path pour importer utils.schema
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema import csv_to_json_records


def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: v.encode('utf-8')
    )

    topic = 'test-topic'
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "transactions.csv")

    print("=" * 60)
    print(f"Chargement des transactions depuis : {csv_path}")
    records = csv_to_json_records(csv_path)
    print(f"{len(records)} transactions chargées.")
    print(f"Envoi vers le topic Kafka : {topic}")
    print("=" * 60)

    try:
        counter = 0
        while True:
            # Parcourt les transactions en boucle
            record = records[counter % len(records)]
            producer.send(topic, value=record)
            counter += 1
            print(f"[#{counter}] Envoyé : {record}")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nArrêt du producer...")
    finally:
        producer.close()
        print("Producer fermé.")


if __name__ == "__main__":
    main()
