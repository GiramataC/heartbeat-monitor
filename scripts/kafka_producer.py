"""
kafka_producer.py
-----------------
Reads synthetic heart beat data from the generator and publishes each
record to a Kafka topic as a JSON message.

Requirements:
    pip install kafka-python

Usage:
    python kafka_producer.py [--interval 1.0] [--topic heartbeat]
"""

import argparse
import json
import logging
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from data_generator import stream_readings, CUSTOMER_PROFILES

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("kafka_producer")

# ---------------------------------------------------------------------------
# Defaults (override via CLI or environment variables)
# ---------------------------------------------------------------------------
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC     = "heartbeat"
DEFAULT_INTERVAL  = 1.0    # seconds between full rounds of readings


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Reliability settings
        acks="all",              # wait for all in-sync replicas
        retries=5,
        retry_backoff_ms=300,
        # Throughput / latency balance
        linger_ms=10,            # batch small messages for 10 ms
        compression_type="gzip",
    )


def on_send_success(record_metadata):
    log.debug(
        "Delivered → topic=%s  partition=%d  offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc):
    log.error("Delivery failed: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(bootstrap_servers: str, topic: str, interval: float):
    log.info("Connecting to Kafka at %s …", bootstrap_servers)
    try:
        producer = create_producer(bootstrap_servers)
    except NoBrokersAvailable:
        log.error(
            "No Kafka brokers available at %s. "
            "Make sure Kafka is running (docker-compose up -d).",
            bootstrap_servers,
        )
        sys.exit(1)

    log.info(
        "Producer ready. Publishing to topic '%s' every %.1f s …",
        topic, interval,
    )
    log.info("Monitoring %d customers: %s", len(CUSTOMER_PROFILES),
             ", ".join(CUSTOMER_PROFILES.keys()))

    sent_total = 0
    try:
        for reading in stream_readings(interval_seconds=interval):
            future = producer.send(
                topic,
                key=reading["customer_id"],
                value=reading,
            )
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            sent_total += 1
            log.info(
                "[%s] %-20s  BPM=%-3d  ts=%s",
                reading["customer_id"],
                reading["customer_name"],
                reading["heart_rate"],
                reading["timestamp"],
            )

            # Flush every 50 messages so the buffer doesn't grow unbounded
            if sent_total % 50 == 0:
                producer.flush()
                log.info("--- Flushed %d messages so far ---", sent_total)

    except KeyboardInterrupt:
        log.info("Shutdown requested.")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed. Total messages sent: %d", sent_total)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Heart Beat Kafka Producer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP,
                        help="Kafka bootstrap server(s), comma-separated")
    parser.add_argument("--topic", default=DEFAULT_TOPIC,
                        help="Kafka topic to publish to")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                        help="Seconds between full rounds of readings")
    args = parser.parse_args()

    run(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        interval=args.interval,
    )
