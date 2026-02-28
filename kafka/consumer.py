from kafka import KafkaConsumer
import json
from datetime import datetime


def main():
    """
    Kafka Consumer de debug.
    Lit les transactions JSON depuis 'test-topic' et affiche leur contenu
    avec les métadonnées et un flag pour les montants élevés (> 300).
    """
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='debug-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )

    print("=" * 60)
    print("Kafka Consumer Debugger - Transactions JSON")
    print("=" * 60)
    print("Topic    : test-topic")
    print("Serveur  : localhost:9092")
    print("Mode     : earliest (rejoue tous les messages)")
    print("=" * 60)
    print("En attente de messages... (Ctrl+C pour arrêter)\n")

    try:
        for message in consumer:
            data = message.value
            if not data:
                continue

            ts = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            flag = "HIGH (>300)" if data.get("amount", 0) > 300 else "normal"

            print(f"[{ts}] Partition={message.partition} | Offset={message.offset} | {flag}")
            print(f"  transaction_id={data['transaction_id']} | user_id={data['user_id']} | amount={data['amount']} | timestamp={data['timestamp']}")
            print("-" * 60)

    except KeyboardInterrupt:
        print("\nArrêt du consumer.")
    finally:
        consumer.close()
        print("Consumer fermé.")


if __name__ == "__main__":
    main()
