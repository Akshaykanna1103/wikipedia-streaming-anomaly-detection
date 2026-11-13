import json
import time
import requests

from kafka import KafkaProducer
from sseclient import SSEClient


# =====================================================
# 1. CONFIGURATION
# =====================================================

WIKIPEDIA_STREAM_URL = (
    "https://stream.wikimedia.org/v2/stream/recentchange"
)

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "wikipedia-edits"


# =====================================================
# 2. CREATE KAFKA PRODUCER
# =====================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)

print("Connected to Kafka successfully!")
print("Listening for Wikipedia live events...\n")


# =====================================================
# 3. CONNECT TO WIKIPEDIA EVENTSTREAM
# =====================================================

try:
    while True:

        response = None
        client = None

        try:
            response = requests.get(
                WIKIPEDIA_STREAM_URL,
                stream=True,
                headers={
                    "User-Agent": "WikipediaActivityMonitor/1.0"
                },
                timeout=(10, None)
            )

            response.raise_for_status()

            client = SSEClient(response)

            print("Connected to Wikipedia EventStream!")

            # =================================================
            # 4. RECEIVE AND PUBLISH EVENTS
            # =================================================

            for event in client.events():

                if not event.data:
                    continue

                try:
                    edit_data = json.loads(event.data)

                    # Timestamp immediately before publishing to Kafka.
                    # The dashboard uses this to calculate pipeline delay.
                    edit_data["pipeline_sent_at"] = time.time()

                    future = producer.send(
                        KAFKA_TOPIC,
                        value=edit_data
                    )

                    # Wait for Kafka acknowledgement.
                    future.get(timeout=10)

                    print(
                        f"Event sent successfully | "
                        f"Title: {edit_data.get('title', 'Unknown')} | "
                        f"Type: {edit_data.get('type', 'Unknown')}"
                    )

                except json.JSONDecodeError:
                    print("Skipping invalid JSON event.")

                except Exception as error:
                    print(f"Error publishing event: {error}")

        except KeyboardInterrupt:
            print("\nStopping Wikipedia producer...")
            break

        except Exception as error:
            print(f"Connection or streaming error: {error}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)

        finally:
            if client is not None:
                client.close()

            if response is not None:
                response.close()

finally:
    # =================================================
    # 5. CLOSE PRODUCER
    # =================================================

    producer.flush()
    producer.close()

    print("Wikipedia producer stopped.")
