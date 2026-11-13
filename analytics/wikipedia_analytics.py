
import json
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from statistics import mean, stdev

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

# ---------------- CONFIGURATION ----------------

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "wikipedia-edits"
DATABASE = "analytics/wikipedia_analytics.db"

WINDOW_SECONDS = 10
BASELINE_WINDOWS = 6
SPIKE_MULTIPLIER = 1.8
STANDARD_DEVIATION_LIMIT = 3
REPORT_INTERVAL = 30

# ---------------- DATABASE ----------------

connection = sqlite3.connect(DATABASE)

connection.execute("""
CREATE TABLE IF NOT EXISTS daily_summary (
    date TEXT PRIMARY KEY,
    total_events INTEGER DEFAULT 0,
    total_edits INTEGER DEFAULT 0,
    anomaly_count INTEGER DEFAULT 0,
    windows_checked INTEGER DEFAULT 0
)
""")

# Preserve the old total_edits value, which actually counted
# all event types, by renaming it to total_events.
columns = {
    row[1]
    for row in connection.execute(
        "PRAGMA table_info(daily_summary)"
    )
}

if "total_events" not in columns and "total_edits" in columns:
    connection.execute("""
    ALTER TABLE daily_summary
    RENAME COLUMN total_edits TO total_events
    """)

    connection.execute("""
    ALTER TABLE daily_summary
    ADD COLUMN total_edits INTEGER DEFAULT 0
    """)

connection.execute("""
CREATE TABLE IF NOT EXISTS event_type_daily (
    date TEXT,
    event_type TEXT,
    event_count INTEGER DEFAULT 0,
    PRIMARY KEY (date, event_type)
)
""")

connection.execute("""
CREATE TABLE IF NOT EXISTS page_daily_edits (
    date TEXT,
    page TEXT,
    edit_count INTEGER DEFAULT 0,
    PRIMARY KEY (date, page)
)
""")

connection.execute("""
CREATE TABLE IF NOT EXISTS wikipedia_article_edits (
    date TEXT,
    page TEXT,
    edit_count INTEGER DEFAULT 0,
    PRIMARY KEY (date, page)
)
""")
connection.commit()


connection.execute("""
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT
)
""")
connection.execute("""
CREATE TABLE IF NOT EXISTS anomaly_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at TEXT,
    event_count INTEGER,
    baseline_average REAL,
    threshold REAL
)
""")

connection.commit()

# ---------------- KAFKA CONSUMER ----------------

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=False,
    group_id="wikipedia-business-analytics",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
    consumer_timeout_ms=1000
)

print("Connected to Kafka successfully!")
print("Wikipedia business analytics started.\n")

# ---------------- RUNTIME VARIABLES ----------------

historical_counts = deque(maxlen=12)

window_start = time.time()
window_event_count = 0

last_report = time.time()

# ---------------- DAILY EVENT STORAGE ----------------


def record_event(edit):

    meta = edit.get("meta") or {}
    event_id = meta.get("id")

    if event_id is None:
        print("WARNING: Event has no Wikimedia ID. Skipping.")
        return False

    event_id = str(event_id)

    try:
        today = datetime.now(timezone.utc).date().isoformat()
        event_type = edit.get("type", "unknown")
        page = edit.get("title", "Unknown")

        is_actual_edit = event_type == "edit"

        is_wikipedia_article = (
            is_actual_edit
            and edit.get("namespace") == 0
            and str(edit.get("server_name", "")).endswith(
                ".wikipedia.org"
            )
        )

        cursor = connection.execute("""
            INSERT OR IGNORE INTO processed_events
            (event_id, processed_at)
            VALUES (?, ?)
        """, (event_id, datetime.now(timezone.utc).isoformat()))

        if cursor.rowcount == 0:
            connection.commit()
            return False

        connection.execute("""
            INSERT INTO daily_summary
            (date, total_events, total_edits)
            VALUES (?, 1, ?)
            ON CONFLICT(date)
            DO UPDATE SET
                total_events = total_events + 1,
                total_edits = total_edits + excluded.total_edits
        """, (today, int(is_actual_edit)))

        connection.execute("""
            INSERT INTO event_type_daily
            (date, event_type, event_count)
            VALUES (?, ?, 1)
            ON CONFLICT(date, event_type)
            DO UPDATE SET event_count = event_count + 1
        """, (today, event_type))

        if is_wikipedia_article:
            connection.execute("""
                INSERT INTO wikipedia_article_edits
                (date, page, edit_count)
                VALUES (?, ?, 1)
                ON CONFLICT(date, page)
                DO UPDATE SET edit_count = edit_count + 1
            """, (today, page))

        connection.commit()
        return True

    except Exception:
        connection.rollback()
        raise

# ---------------- ANOMALY DETECTION ----------------

def evaluate_window(event_count):

    if len(historical_counts) < BASELINE_WINDOWS:

        print(
            f"Collecting baseline: "
            f"{len(historical_counts)}/{BASELINE_WINDOWS} windows"
        )

        historical_counts.append(event_count)
        return

    baseline = list(historical_counts)

    average_count = mean(baseline)
    deviation = stdev(baseline)

    threshold = max(
        average_count * SPIKE_MULTIPLIER,
        average_count + STANDARD_DEVIATION_LIMIT * deviation
    )

    today = datetime.now(timezone.utc).date().isoformat()

    connection.execute("""
    INSERT INTO daily_summary (date, windows_checked)
    VALUES (?, 1)
    ON CONFLICT(date)
    DO UPDATE SET windows_checked = windows_checked + 1
    """, (today,))

    if event_count > threshold:

        detected_at = datetime.now(timezone.utc).isoformat()

        connection.execute("""
        UPDATE daily_summary
        SET anomaly_count = anomaly_count + 1
        WHERE date = ?
        """, (today,))

        connection.execute("""
        INSERT INTO anomaly_events
        (detected_at, event_count, baseline_average, threshold)
        VALUES (?, ?, ?, ?)
        """, (
            detected_at,
            event_count,
            average_count,
            threshold
        ))

        print("\n!!! ACTIVITY SPIKE DETECTED !!!")
        print(f"Current events: {event_count}")
        print(f"Historical average: {average_count:.2f}")
        print(f"Detection threshold: {threshold:.2f}")
        print("Alert requires further investigation.\n")

    else:
        print(
            f"Normal activity | Current: {event_count} | "
            f"Threshold: {threshold:.2f}"
        )

    connection.commit()
    historical_counts.append(event_count)

# ---------------- BUSINESS REPORT ----------------

def generate_report():

    today = datetime.now(timezone.utc).date().isoformat()

    summary = connection.execute("""
    SELECT total_events, total_edits, anomaly_count, windows_checked
    FROM daily_summary
    WHERE date = ?
    """, (today,)).fetchone()

    if summary is None:
        return

    total_events, total_edits, anomalies, windows = summary

    anomaly_rate = (
        anomalies / windows * 100 if windows else 0
    )

    top_pages = connection.execute("""
    SELECT page, edit_count
    FROM wikipedia_article_edits
    WHERE date = ?
    ORDER BY edit_count DESC
    LIMIT 10
    """, (today,)).fetchall()

    event_types = connection.execute("""
    SELECT event_type, event_count
    FROM event_type_daily
    WHERE date = ?
    ORDER BY event_count DESC
    """, (today,)).fetchall()

    print("\n" + "=" * 55)
    print("WIKIPEDIA DAILY BUSINESS ANALYTICS (UTC)")
    print("=" * 55)

    print(f"Date: {today}")
    print(f"Total Wikipedia events: {total_events}")
    print(f"Actual edit events: {total_edits}")
    print(f"Anomalies detected: {anomalies}")
    print(f"Evaluated windows: {windows}")
    print(f"Anomaly rate: {anomaly_rate:.2f}%")

    print("\nEvent type distribution:")

    for event_type, count in event_types:
        print(f"{event_type:<20} {count}")

    print("\nTop 10 actually edited pages:")

    for page, count in top_pages:
        print(f"{page[:45]:<45} {count}")

    print("=" * 55)


# ---------------- MAIN PROCESSING LOOP ----------------

try:

    while True:

        records = consumer.poll(timeout_ms=1000)

        for partition, messages in records.items():

            for message in messages:

                edit = message.value
                is_new_event = record_event(edit)

                # Commit after successful database processing.
                topic_partition = TopicPartition(
                    message.topic,
                    message.partition
                )

                offset = OffsetAndMetadata(
                    message.offset + 1,
                    ""
                )

                consumer.commit({
                    topic_partition: offset
                })

                if is_new_event:

                    window_event_count += 1

                    print(
                        f"Received | Page: {edit.get('title', 'Unknown')} | "
                        f"Type: {edit.get('type', 'unknown')}"
                    )

                else:

                    print(
                        f"Duplicate or missing-ID event ignored | "
                        f"Page: {edit.get('title', 'Unknown')}"
                    )

        # Evaluate completed windows.
        now = time.time()

        while now - window_start >= WINDOW_SECONDS:

            evaluate_window(window_event_count)

            window_event_count = 0
            window_start += WINDOW_SECONDS

        # Generate business report every 30 seconds.
        if now - last_report >= REPORT_INTERVAL:

            generate_report()
            last_report = now

except KeyboardInterrupt:

    print("\nStopping analytics...")

finally:

    consumer.close()
    connection.close()

    print("Analytics stopped.")