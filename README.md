# Real-Time Wikipedia Activity Monitoring and Anomaly Detection Using Apache Kafka

## 1. Project Overview

This project implements a real-time data streaming pipeline that monitors live Wikipedia editing activity and detects unusual activity patterns using Apache Kafka.

The system collects live Wikipedia recent-change events, streams them through Kafka, processes them using a Python analytics consumer, and displays real-time monitoring information through a Streamlit dashboard.

The primary objective is to identify unusual spikes in Wikipedia editing activity and provide insights that can help identify events requiring further investigation.

## 2. Project Objectives

- Collect genuine real-time Wikipedia editing events.
- Implement a Kafka-based producer-consumer architecture.
- Process and analyze incoming streaming events.
- Detect unusual activity spikes using statistical techniques.
- Prevent duplicate events from being counted multiple times.
- Monitor throughput, processing latency, and consumer lag.
- Visualize live activity and analytics through a dashboard.
- Test consumer recovery when processing is interrupted.

## 3. System Architecture

```text
       Wikimedia Recent Changes Stream
                    |
                    v
          Python Kafka Producer
                    |
                    v
             Apache Kafka
             Topic: wikipedia-edits
             3 Partitions
                    |
          +---------+---------+
          |                   |
          v                   v
   Analytics Consumer   Dashboard Consumer
          |                   |
          v                   v
   Duplicate Detection   Live Event Monitoring
          |
          v
   SQLite Database
          |
          v
   Business Analytics
          |
          v
   Anomaly Detection
          |
          v
      Streamlit Dashboard
```

## 4. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Application development |
| Apache Kafka | Real-time event streaming |
| Docker | Kafka deployment |
| SQLite | Analytics storage and duplicate tracking |
| Streamlit | Interactive dashboard |
| Pandas | Dashboard data processing |
| Wikimedia SSE API | Live Wikipedia event source |

## 5. Data Source

The project uses the official Wikimedia Recent Changes EventStream:

https://stream.wikimedia.org/v2/stream/recentchange

The stream provides live Wikipedia activity events, including:
- Edit events
- New page events
- Categorization events
- Log events

The producer reads these events and publishes them to Kafka in JSON format.

## 6. Project Structure

```text
wikipedia-streaming-anomaly-detection/
│
├── producer/
│   └── wikipedia_producer.py
│
├── consumer/
│   └── wikipedia_consumer.py
│
├── analytics/
│   └── wikipedia_analytics.py
│
├── dashboard/
│   └── app.py
│
├── config/
│
├── docker-compose.yml
├── requirements.txt
├── README.md
├── AI_PROMPTS.txt
└── .gitignore
```

## 7. Installation and Setup

### Prerequisites

- Python 3.11 or later
- Docker Desktop
- Git

### Step 1: Clone the repository

```bash
git clone https://github.com/Akshaykanna1103/wikipedia-streaming-anomaly-detection.git

cd wikipedia-streaming-anomaly-detection
```

### Step 2: Create a virtual environment

```bash
python3 -m venv .venv

source .venv/bin/activate
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start Kafka using Docker Compose

```bash
docker compose up -d
```

### Step 5: Create the Kafka topic

```bash
docker exec -it wikipedia-kafka /opt/kafka/bin/kafka-topics.sh \
--create \
--topic wikipedia-edits \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

If the topic already exists, Kafka will report that it already exists.

## 8. Running the Project

Run each component in a separate terminal from the project directory.

### Terminal 1: Kafka

Check that Kafka is running:

```bash
docker ps
```

### Terminal 2: Wikipedia Producer

```bash
source .venv/bin/activate
python producer/wikipedia_producer.py
```

The producer connects to Wikimedia's live event stream and publishes events to Kafka.

### Terminal 3: Streamlit Dashboard

```bash
source .venv/bin/activate
streamlit run dashboard/app.py
```

Open the local URL displayed by Streamlit in your browser.

### Terminal 4: Analytics Consumer

```bash
source .venv/bin/activate
python analytics/wikipedia_analytics.py
```

The analytics consumer reads Kafka events, performs duplicate detection, updates SQLite analytics, and evaluates activity windows.

## 9. Anomaly Detection

The project uses a statistical threshold-based approach to identify unusual activity spikes.

The analytics consumer processes events in non-overlapping 10-second windows.

The anomaly detection mechanism uses:
- Six previous completed windows as the initial baseline.
- Historical average activity.
- Standard deviation.
- A spike multiplier of 1.8.
- A standard deviation limit of 3.

The detection threshold is:

```text
Threshold = max(
    Average × 1.8,
    Average + 3 × Standard Deviation
)
```

When the current window's event count exceeds the calculated threshold, the system generates an anomaly alert.

These alerts indicate unusual activity patterns and require further investigation. They do not independently establish malicious activity.

## 10. Duplicate Event Detection

To prevent duplicate events from being counted multiple times, the analytics consumer uses Wikimedia's unique event identifier.

A SQLite table named `processed_events` stores previously processed event IDs.

The system uses:

```sql
INSERT OR IGNORE INTO processed_events
(event_id, processed_at)
VALUES (?, ?);
```

If an event ID already exists, the duplicate is ignored.

The event ID insertion and analytics updates are performed within the same SQLite transaction.

Kafka automatic offset commits are disabled. The consumer commits the processed message offset after successful database processing.

This provides effectively-once analytics processing for events with valid unique IDs, even when Kafka redelivers messages.

## 11. Business Analytics and Dashboard

The Streamlit dashboard provides:

- Total Wikipedia events.
- Actual edit event counts.
- Anomaly alerts.
- Anomaly alert rate.
- Most edited Wikipedia articles.
- Event type distribution.
- Daily activity history.
- Live event throughput.
- Average and maximum pipeline processing delay.
- Recent streaming events.
- Kafka consumer performance information.

## 12. Reliability and Recovery

The project uses Kafka to buffer incoming events while consumers are unavailable.

During consumer interruption:
1. The producer continues publishing events.
2. Kafka retains messages according to its topic retention configuration.
3. The analytics consumer can restart using its committed offsets.
4. Previously unprocessed messages can be consumed after recovery.
5. SQLite duplicate detection prevents repeated analytics updates.

The project also includes a consumer lag monitoring test to observe backlog recovery.

## 13. Performance Considerations

The project monitors:

- **Throughput:** Number of events processed per second.
- **Latency:** Time between producer timestamp and dashboard receipt.
- **Consumer lag:** Difference between Kafka log-end offsets and consumer offsets.
- **Reliability:** Consumer recovery and message processing behavior.
- **Scalability:** Kafka topic partitioning and consumer architecture.

The current Kafka deployment uses one broker, three topic partitions, and replication factor one. It demonstrates partition-based distribution but does not provide broker-level high availability.

## 14. Limitations

- Anomaly detection currently uses processing-time windows rather than event-time windows.
- The anomaly baseline is maintained in memory and resets when the analytics consumer restarts.
- The current deployment uses a single Kafka broker.
- Duplicate detection depends on the availability of a unique Wikimedia event ID.
- The system does not have ground-truth labels to calculate supervised anomaly detection accuracy.
- Anomaly alerts indicate unusual activity and are not proof of malicious behavior.

## 15. Future Enhancements

- Implement event-time window processing.
- Persist anomaly detection baseline information.
- Improve anomaly detection with machine learning techniques.
- Introduce multi-broker Kafka deployment.
- Add automated alert notifications.
- Improve monitoring and performance evaluation.

## 16. Author

**Akshay Kanna**

GitHub: https://github.com/Akshaykanna1103

## 17. License

This project is developed for academic and educational purposes.
