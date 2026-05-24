"""
test_pipeline.py
----------------
Unit and integration tests for the heart beat monitoring pipeline.

Run with:
    python -m pytest tests/test_pipeline.py -v

No external services are required for the unit tests.
Integration tests (marked with @pytest.mark.integration) need the
full docker-compose stack running.
"""

import json
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

# Make the scripts directory importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from data_generator import (
    generate_heart_rate,
    generate_reading,
    stream_readings,
    CUSTOMER_PROFILES,
)
from kafka_consumer import (
    classify_heart_rate,
    validate_message,
)


# =============================================================================
# Data Generator Tests
# =============================================================================

class TestGenerateHeartRate:
    """Heart rate generation stays within physiological limits."""

    def test_returns_integer(self):
        profile = CUSTOMER_PROFILES["C001"]
        bpm = generate_heart_rate(profile)
        assert isinstance(bpm, int)

    def test_within_physiological_range(self):
        profile = CUSTOMER_PROFILES["C001"]
        for _ in range(500):
            bpm = generate_heart_rate(profile)
            assert 30 <= bpm <= 220, f"BPM {bpm} out of expected range"

    def test_normal_values_cluster_around_baseline(self):
        """Without anomalies, mean BPM should be close to the baseline."""
        profile = CUSTOMER_PROFILES["C003"]  # baseline=62
        # Patch random.random to never trigger anomaly
        import random
        original = random.random
        random.random = lambda: 0.99   # always > ANOMALY_PROBABILITY
        readings = [generate_heart_rate(profile) for _ in range(200)]
        random.random = original
        mean = sum(readings) / len(readings)
        assert abs(mean - profile["baseline"]) < 10, \
            f"Mean {mean:.1f} too far from baseline {profile['baseline']}"


class TestGenerateReading:
    """Individual record generation."""

    def test_required_keys_present(self):
        record = generate_reading("C001")
        for key in ("customer_id", "customer_name", "timestamp", "heart_rate", "age"):
            assert key in record, f"Missing key: {key}"

    def test_customer_id_matches(self):
        record = generate_reading("C002")
        assert record["customer_id"] == "C002"

    def test_timestamp_is_iso_format(self):
        record = generate_reading("C001")
        # Should not raise
        dt = datetime.fromisoformat(record["timestamp"])
        assert dt.tzinfo is not None, "Timestamp should be timezone-aware"

    def test_all_customers_generate_records(self):
        for cid in CUSTOMER_PROFILES:
            record = generate_reading(cid)
            assert record["customer_id"] == cid


class TestStreamReadings:
    """Stream generator yields one record per customer per tick."""

    def test_yields_dicts(self):
        gen = stream_readings(burst=True)
        record = next(gen)
        assert isinstance(record, dict)

    def test_covers_all_customers(self):
        """First N records should include all customer IDs."""
        gen = stream_readings(burst=True)
        n = len(CUSTOMER_PROFILES)
        seen = {next(gen)["customer_id"] for _ in range(n)}
        assert seen == set(CUSTOMER_PROFILES.keys())


# =============================================================================
# Consumer Validation Tests
# =============================================================================

class TestValidateMessage:

    def _good(self):
        return {
            "customer_id": "C001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heart_rate": 72,
        }

    def test_valid_message(self):
        ok, reason = validate_message(self._good())
        assert ok is True
        assert reason == "OK"

    def test_missing_customer_id(self):
        msg = self._good()
        del msg["customer_id"]
        ok, _ = validate_message(msg)
        assert ok is False

    def test_missing_timestamp(self):
        msg = self._good()
        del msg["timestamp"]
        ok, _ = validate_message(msg)
        assert ok is False

    def test_missing_heart_rate(self):
        msg = self._good()
        del msg["heart_rate"]
        ok, _ = validate_message(msg)
        assert ok is False

    def test_heart_rate_zero_invalid(self):
        msg = self._good()
        msg["heart_rate"] = 0
        ok, _ = validate_message(msg)
        assert ok is False

    def test_heart_rate_300_invalid(self):
        msg = self._good()
        msg["heart_rate"] = 300
        ok, _ = validate_message(msg)
        assert ok is False

    def test_heart_rate_string_invalid(self):
        msg = self._good()
        msg["heart_rate"] = "fast"
        ok, _ = validate_message(msg)
        assert ok is False

    def test_bad_timestamp(self):
        msg = self._good()
        msg["timestamp"] = "not-a-date"
        ok, _ = validate_message(msg)
        assert ok is False


class TestClassifyHeartRate:

    def test_normal_range(self):
        for bpm in [50, 72, 100, 130]:
            assert classify_heart_rate(bpm) == "NORMAL", f"Expected NORMAL for {bpm}"

    def test_warning_low(self):
        assert classify_heart_rate(45) == "WARNING"

    def test_warning_high(self):
        assert classify_heart_rate(145) == "WARNING"

    def test_critical_low(self):
        assert classify_heart_rate(35) == "CRITICAL"

    def test_critical_high(self):
        assert classify_heart_rate(180) == "CRITICAL"

    def test_boundary_warning_low(self):
        assert classify_heart_rate(49) == "WARNING"
        assert classify_heart_rate(50) == "NORMAL"

    def test_boundary_warning_high(self):
        assert classify_heart_rate(130) == "NORMAL"
        assert classify_heart_rate(131) == "WARNING"

    def test_boundary_critical_low(self):
        assert classify_heart_rate(40) == "CRITICAL"
        assert classify_heart_rate(41) == "WARNING"

    def test_boundary_critical_high(self):
        assert classify_heart_rate(150) == "CRITICAL"
        assert classify_heart_rate(149) == "WARNING"


# =============================================================================
# Integration tests (require running docker-compose stack)
# =============================================================================

@pytest.mark.integration
class TestDatabaseIntegration:
    """
    These tests connect to the real PostgreSQL instance.
    Run only when the docker-compose stack is up:
        docker-compose -f docker/docker-compose.yml up -d
        pytest tests/test_pipeline.py -v -m integration
    """

    @pytest.fixture(scope="class")
    def db_conn(self):
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="heartbeat_db",
            user="heartbeat_user",
            password="heartbeat_pass",
        )
        yield conn
        conn.close()

    def test_table_exists(self, db_conn):
        cur = db_conn.cursor()
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'heart_rate_readings'
            )
        """)
        assert cur.fetchone()[0] is True

    def test_insert_and_retrieve(self, db_conn):
        cur = db_conn.cursor()
        ts = datetime.now(timezone.utc).isoformat()
        cur.execute("""
            INSERT INTO heart_rate_readings
                (customer_id, customer_name, reading_timestamp,
                 heart_rate, status, age)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, ("C001", "Alice Kamali", ts, 72, "NORMAL", 32))
        db_conn.commit()

        cur.execute("""
            SELECT heart_rate, status FROM heart_rate_readings
            WHERE customer_id = 'C001'
            ORDER BY reading_timestamp DESC LIMIT 1
        """)
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 72
        assert row[1] == "NORMAL"
