"""
kafka_consumer.py
-----------------
Consumes heart beat messages from a Kafka topic, validates them, flags
anomalies, and writes clean records to PostgreSQL.

Requirements:
    pip install kafka-python psycopg2-binary

Usage:
    python kafka_consumer.py [--topic heartbeat] [--group heartbeat-group]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("kafka_consumer")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BOOTSTRAP  = "localhost:9092"
DEFAULT_TOPIC      = "heartbeat"
DEFAULT_GROUP      = "heartbeat-group"

# Heart rate thresholds for anomaly classification
HR_CRITICAL_LOW  = 40    # < 40 bpm  → bradycardia alert
HR_WARNING_LOW   = 50    # < 50 bpm  → low warning
HR_WARNING_HIGH  = 130   # > 130 bpm → elevated warning
HR_CRITICAL_HIGH = 150   # > 150 bpm → tachycardia alert

# DB connection (reads from env with sensible local defaults)
DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "heartbeat_db"),
    "user":     os.getenv("PG_USER",     "heartbeat_user"),
    "password": os.getenv("PG_PASSWORD", "heartbeat_pass"),
}


# ---------------------------------------------------------------------------
# Validation & classification
# ---------------------------------------------------------------------------
def classify_heart_rate(bpm: int) -> str:
    """Return a status label for a given heart rate."""
    if bpm < HR_CRITICAL_LOW or bpm > HR_CRITICAL_HIGH:
        return "CRITICAL"
    if bpm < HR_WARNING_LOW or bpm > HR_WARNING_HIGH:
        return "WARNING"
    return "NORMAL"


def validate_message(data: dict) -> tuple[bool, str]:
    """
    Basic schema & range validation.
    Returns (is_valid, reason).
    """
    required = {"customer_id", "timestamp", "heart_rate"}
    missing = required - data.keys()
    if missing:
        return False, f"Missing fields: {missing}"

    bpm = data.get("heart_rate")
    if not isinstance(bpm, int) or not (0 < bpm < 300):
        return False, f"Implausible heart_rate value: {bpm}"

    try:
        datetime.fromisoformat(data["timestamp"])
    except (ValueError, TypeError):
        return False, f"Bad timestamp: {data['timestamp']}"

    return True, "OK"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def connect_db() -> psycopg2.extensions.connection:
    """Open and return a PostgreSQL connection."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


INSERT_SQL = """
    INSERT INTO heart_rate_readings
        (customer_id, customer_name, reading_timestamp, heart_rate,
         status, age, ingested_at)
    VALUES
        (%(customer_id)s, %(customer_name)s, %(reading_timestamp)s,
         %(heart_rate)s, %(status)s, %(age)s, %(ingested_at)s)
    ON CONFLICT DO NOTHING;
"""


def insert_reading(cursor, record: dict):
    cursor.execute(INSERT_SQL, record)


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------
def run(bootstrap_servers: str, topic: str, group_id: str):
    log.info("Connecting to Kafka at %s …", bootstrap_servers)
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",    # replay from start if no offset
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=-1,          # block forever
        )
    except NoBrokersAvailable:
        log.error("No Kafka brokers available at %s.", bootstrap_servers)
        sys.exit(1)

    log.info("Connecting to PostgreSQL at %s/%s …", DB_CONFIG["host"], DB_CONFIG["dbname"])
    try:
        conn = connect_db()
        cursor = conn.cursor()
    except psycopg2.OperationalError as exc:
        log.error("DB connection failed: %s", exc)
        sys.exit(1)

    log.info("Consumer ready. Listening on topic '%s' …", topic)
    processed = errors = anomalies = 0

    try:
        for message in consumer:
            data = message.value

            # ── Validate ──────────────────────────────────────────────────
            valid, reason = validate_message(data)
            if not valid:
                log.warning("Invalid message (offset=%d): %s", message.offset, reason)
                errors += 1
                continue

            # ── Classify ──────────────────────────────────────────────────
            bpm    = data["heart_rate"]
            status = classify_heart_rate(bpm)

            if status != "NORMAL":
                anomalies += 1
                log.warning(
                    "⚠  ANOMALY [%s] %s – BPM=%d  status=%s",
                    data["customer_id"], data.get("customer_name", "?"),
                    bpm, status,
                )
            else:
                log.info(
                    "✓  [%s] %-20s  BPM=%-3d  status=%s",
                    data["customer_id"], data.get("customer_name", "?"),
                    bpm, status,
                )

            # ── Persist ───────────────────────────────────────────────────
            record = {
                "customer_id":        data["customer_id"],
                "customer_name":      data.get("customer_name"),
                "reading_timestamp":  data["timestamp"],
                "heart_rate":         bpm,
                "status":             status,
                "age":                data.get("age"),
                "ingested_at":        datetime.now(timezone.utc).isoformat(),
            }
            try:
                insert_reading(cursor, record)
                conn.commit()
                processed += 1
            except psycopg2.Error as exc:
                conn.rollback()
                log.error("DB insert failed: %s", exc)
                errors += 1

    except KeyboardInterrupt:
        log.info("Shutdown requested.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        log.info(
            "Consumer closed. processed=%d  anomalies=%d  errors=%d",
            processed, anomalies, errors,
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Heart Beat Kafka Consumer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic",     default=DEFAULT_TOPIC)
    parser.add_argument("--group",     default=DEFAULT_GROUP)
    args = parser.parse_args()

    run(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        group_id=args.group,
    )
