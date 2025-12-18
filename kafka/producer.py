from kafka import KafkaProducer
import json
import time
import random

def main():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    topic = 'test-topic'
    
    print(f"📤 Starting to send messages to topic: {topic}")
    
    messages = [
        "This is an important message",
        "This is a regular message",
        "Another important update",
        "Just a normal event",
        "Critical important alert",
        "Random data here",
        "Important notification received"
    ]
    
    try:
        counter = 0
        while True:
            # Send a random message
            message = random.choice(messages)
            producer.send(topic, value=message)
            
            counter += 1
            print(f"✅ Sent message {counter}: {message}")
            
            # Wait 2 seconds before sending next message
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n⏹️  Stopping producer...")
    finally:
        producer.close()
        print("✅ Producer closed.")

if __name__ == "__main__":
    main()
