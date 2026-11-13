import json
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "wikipedia-edits"

# Create Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Connected to Kafka!")
print("Waiting for Wikipedia events...\n")

try:
    for message in consumer:
        edit = message.value

        print(
            f"Page: {edit.get('title', 'Unknown')} | "
            f"Type: {edit.get('type', 'Unknown')} | "
            f"User: {edit.get('user', 'Unknown')}"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
