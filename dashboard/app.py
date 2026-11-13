
import json
import sqlite3
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

# =====================================================
# 1. CONFIGURATION
# =====================================================

st.set_page_config(
    page_title="Wikipedia Business Activity Monitor",
    page_icon="📊",
    layout="wide"
)

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "wikipedia-edits"

DATABASE = (
    Path(__file__).resolve().parent.parent
    / "analytics"
    / "wikipedia_analytics.db"
)

REFRESH_SECONDS = 3
MAX_EVENT_HISTORY = 2000
MAX_METRIC_HISTORY = 120

# =====================================================
# 2. SESSION STATE
# =====================================================

defaults = {
    "consumer": None,
    "consumer_error": None,
    "session_events": 0,
    "recent_events": deque(maxlen=MAX_EVENT_HISTORY),
    "latency_history": deque(maxlen=MAX_METRIC_HISTORY),
    "throughput_history": deque(maxlen=MAX_METRIC_HISTORY),
    "event_times": deque(maxlen=MAX_EVENT_HISTORY),
}

for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# =====================================================
# 3. DATABASE READING
# =====================================================

def query_database(query, params=()):
    try:
        with sqlite3.connect(
            f"file:{DATABASE}?mode=ro",
            uri=True,
            timeout=10
        ) as connection:
            connection.execute("PRAGMA busy_timeout=10000")
            return pd.read_sql_query(
                query,
                connection,
                params=params
            )
    except (sqlite3.Error, pd.errors.DatabaseError) as error:
        st.warning(f"Database read issue: {error}")
        return pd.DataFrame()


today = datetime.now(timezone.utc).date().isoformat()

daily_df = query_database(
    """
    SELECT
        date,
        total_events,
        total_edits,
        anomaly_count,
        windows_checked
    FROM daily_summary
    WHERE date = ?
    """,
    (today,)
)

event_types_df = query_database(
    """
    SELECT event_type, event_count
    FROM event_type_daily
    WHERE date = ?
    ORDER BY event_count DESC
    """,
    (today,)
)

top_pages_df = query_database(
    """
    SELECT page, edit_count
    FROM wikipedia_article_edits
    WHERE date = ?
    ORDER BY edit_count DESC
    LIMIT 10
    """,
    (today,)
)

anomalies_df = query_database(
    """
    SELECT
        detected_at,
        event_count,
        baseline_average,
        threshold
    FROM anomaly_events
    ORDER BY detected_at DESC
    LIMIT 20
    """
)

daily_history_df = query_database(
    """
    SELECT
        date,
        total_events,
        total_edits,
        anomaly_count,
        windows_checked
    FROM daily_summary
    ORDER BY date DESC
    LIMIT 30
    """
)

# =====================================================
# 4. KAFKA CONSUMER
# =====================================================

def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="wikipedia-dashboard-v2",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=1000,
    )


def read_kafka_events():
    if st.session_state.consumer is None:
        try:
            st.session_state.consumer = create_consumer()
            st.session_state.consumer_error = None
        except Exception as error:
            st.session_state.consumer_error = str(error)
            return

    try:
        for message in st.session_state.consumer:
            edit = message.value
            received_at = time.time()

            st.session_state.session_events += 1
            st.session_state.event_times.append(received_at)

            st.session_state.recent_events.append({
                "Page": edit.get("title", "Unknown"),
                "Type": edit.get("type", "unknown"),
                "User": edit.get("user", "Unknown"),
                "Received At": datetime.fromtimestamp(
                    received_at
                ).strftime("%H:%M:%S")
            })

            sent_at = edit.get("pipeline_sent_at")

            if isinstance(sent_at, (int, float)):
                delay_ms = max(
                    0,
                    (received_at - sent_at) * 1000
                )
                st.session_state.latency_history.append(
                    delay_ms
                )

        st.session_state.consumer_error = None

    except Exception as error:
        st.session_state.consumer_error = str(error)

        try:
            st.session_state.consumer.close()
        except Exception:
            pass

        st.session_state.consumer = None


# =====================================================
# 5. READ LIVE EVENTS
# =====================================================

read_kafka_events()

now = time.time()

while (
    st.session_state.event_times
    and now - st.session_state.event_times[0] > 10
):
    st.session_state.event_times.popleft()

current_throughput = (
    len(st.session_state.event_times) / 10
)

st.session_state.throughput_history.append({
    "Time": datetime.now().strftime("%H:%M:%S"),
    "Events/sec": current_throughput
})

# =====================================================
# 6. DASHBOARD HEADER
# =====================================================

st.title("📊 Wikipedia Real-Time Business Activity Monitor")

st.caption(
    "Apache Kafka | Wikimedia EventStream | "
    "Daily Business Analytics | Anomaly Detection"
)

st.write(
    "Business Objective 1: Monitor daily Wikipedia activity "
    "and identify unusual activity spikes."
)

st.write(
    "Business Objective 2: Identify the most frequently "
    "edited Wikipedia pages for prioritized review."
)

# =====================================================
# 7. BUSINESS KPI SECTION
# =====================================================

st.header("Daily Business KPIs")

if not daily_df.empty:
    row = daily_df.iloc[0]

    total_events = int(row["total_events"])
    total_edits = int(row["total_edits"])
    anomaly_count = int(row["anomaly_count"])
    windows_checked = int(row["windows_checked"])
else:
    total_events = 0
    total_edits = 0
    anomaly_count = 0
    windows_checked = 0

anomaly_rate = (
    anomaly_count / windows_checked * 100
    if windows_checked > 0
    else 0
)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Wikipedia Events Today", f"{total_events:,}")
col2.metric("Actual Edit Events Today", f"{total_edits:,}")
col3.metric("Anomaly Alerts Today", f"{anomaly_count:,}")
col4.metric("Anomaly Alert Rate", f"{anomaly_rate:.2f}%")

st.caption(
    f"Date: {today} (UTC). "
    f"Anomaly rate = {anomaly_count} flagged windows / "
    f"{windows_checked} evaluated windows. "
    "Historical events collected before event-type tracking "
    "are included in total events but not actual edit counts."
)

# =====================================================
# 8. BUSINESS OBJECTIVE 1
# =====================================================

st.header("Business Objective 1: Daily Anomaly Monitoring")

col1, col2 = st.columns(2)

with col1:
    st.metric(
        "10-Second Windows Evaluated",
        f"{windows_checked:,}"
    )

with col2:
    st.metric(
        "Activity Spike Alerts",
        f"{anomaly_count:,}"
    )

if anomaly_count > 0:
    st.warning(
        f"{anomaly_count} unusual activity windows have "
        "been flagged today for further investigation."
    )
else:
    st.info(
        "No activity spike alerts recorded today."
    )

st.caption(
    "Anomaly detection compares 10-second event counts "
    "against historical windows. Alerts indicate unusual "
    "activity, not confirmed malicious editing."
)

st.subheader("Recent Anomaly Alerts")

if not anomalies_df.empty:
    st.dataframe(
        anomalies_df.rename(columns={
            "detected_at": "Detected At (UTC)",
            "event_count": "Events in Window",
            "baseline_average": "Baseline Average",
            "threshold": "Detection Threshold"
        }),
        hide_index=True,
        width="stretch"
    )
else:
    st.info("No anomaly records available yet.")

# =====================================================
# 9. BUSINESS OBJECTIVE 2
# =====================================================

st.header("Business Objective 2: Most Edited Pages")

if not top_pages_df.empty:
    top_pages_df = top_pages_df.rename(columns={
        "page": "Wikipedia Page",
        "edit_count": "Actual Edit Events"
    })

    st.dataframe(
        top_pages_df,
        hide_index=True,
        width="stretch"
    )

    chart_df = top_pages_df.set_index(
        "Wikipedia Page"
    )

    st.bar_chart(chart_df)

    st.caption(
        "These counts include only events whose Wikimedia "
        "event type is 'edit'."
    )
else:
    st.info("Waiting for actual page-edit events.")

# =====================================================
# 10. EVENT TYPE DISTRIBUTION
# =====================================================

st.header("Wikipedia Event Type Distribution")

if not event_types_df.empty:
    event_types_df = event_types_df.rename(columns={
        "event_type": "Event Type",
        "event_count": "Event Count"
    })

    st.bar_chart(
        event_types_df.set_index("Event Type")
    )

    st.dataframe(
        event_types_df,
        hide_index=True,
        width="stretch"
    )
else:
    st.info("Waiting for event-type statistics.")

# =====================================================
# 11. DAILY HISTORY
# =====================================================

st.header("Daily Activity History")

if not daily_history_df.empty:
    history = daily_history_df.sort_values("date")

    st.line_chart(
        history.set_index("date")[
            ["total_events", "total_edits"]
        ]
    )

    st.dataframe(
        daily_history_df,
        hide_index=True,
        width="stretch"
    )

# =====================================================
# 12. LIVE KAFKA PERFORMANCE
# =====================================================

st.header("Live Kafka System Performance")

latencies = list(st.session_state.latency_history)

average_delay = mean(latencies) if latencies else None
maximum_delay = max(latencies) if latencies else None

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Dashboard Session Events",
    f"{st.session_state.session_events:,}"
)

col2.metric(
    "Consumer Throughput",
    f"{current_throughput:.2f} events/sec"
)

col3.metric(
    "Average Pipeline Delay",
    f"{average_delay:.0f} ms"
    if average_delay is not None
    else "Waiting for data"
)

col4.metric(
    "Maximum Pipeline Delay",
    f"{maximum_delay:.0f} ms"
    if maximum_delay is not None
    else "Waiting for data"
)

st.caption(
    "Throughput uses the last 10 seconds of events retained "
    "by the dashboard. Delay measures producer timestamp "
    "to dashboard receipt and can include Kafka backlog."
)

if st.session_state.consumer_error:
    st.error(
        "Kafka consumer error: "
        + st.session_state.consumer_error
    )
else:
    st.success("Dashboard Kafka consumer connected")

if st.session_state.throughput_history:
    trend_df = pd.DataFrame(
        list(st.session_state.throughput_history)
    )
    st.line_chart(trend_df.set_index("Time"))

# =====================================================
# 13. SCALABILITY AND RELIABILITY
# =====================================================

st.header("Scalability and Reliability")

col1, col2, col3 = st.columns(3)

col1.metric("Kafka Topic", KAFKA_TOPIC)
col2.metric("Configured Partitions", "3")
col3.metric("Configured Brokers", "1")

st.info(
    "The current configuration uses one Kafka broker "
    "and three partitions. Replication factor 1 does "
    "not provide broker-level failover."
)

st.write(
    "The producer includes reconnection handling for "
    "Wikimedia stream interruptions. SQLite stores "
    "daily business statistics across dashboard restarts."
)

st.subheader("Anomaly Detection Accuracy")

st.write(
    "The system uses rule-based activity spike detection. "
    "Because ground-truth anomaly labels are unavailable, "
    "precision, recall, F1-score and classification accuracy "
    "are not reported."
)

# =====================================================
# 14. RECENT LIVE EVENTS
# =====================================================

st.header("Recent Wikipedia Events")

if st.session_state.recent_events:
    recent_df = pd.DataFrame(
        list(st.session_state.recent_events)[-20:]
    )

    st.dataframe(
        recent_df,
        hide_index=True,
        width="stretch"
    )
else:
    st.info("Waiting for live Wikipedia events.")

# =====================================================
# 15. AUTOMATIC REFRESH
# =====================================================

time.sleep(REFRESH_SECONDS)
st.rerun()

