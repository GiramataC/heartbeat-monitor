"""
data_generator.py
-----------------
Simulates realistic heart rate sensor readings for multiple customers.
Generates continuous heart beat data with realistic patterns including:
  - Normal resting ranges per customer profile
  - Gradual drift (simulating activity changes)
  - Occasional anomalies for testing alert logic
"""

import random
import time
import json
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Customer profiles – each customer has a baseline resting heart rate and a
# realistic variance band.  This makes the synthetic data more believable.
# ---------------------------------------------------------------------------
CUSTOMER_PROFILES = {
    "C001": {"name": "Alice Kamali",    "baseline": 68, "variance": 10, "age": 32},
    "C002": {"name": "Bob Nkurunziza",  "baseline": 74, "variance": 12, "age": 45},
    "C003": {"name": "Carol Uwimana",   "baseline": 62, "variance": 8,  "age": 28},
    "C004": {"name": "David Habimana",  "baseline": 80, "variance": 15, "age": 55},
    "C005": {"name": "Eva Mukamana",    "baseline": 70, "variance": 11, "age": 38},
}

# Probability of injecting an anomaly reading (out-of-range value)
ANOMALY_PROBABILITY = 0.05   # 5 % of readings are anomalous


def generate_heart_rate(profile: dict) -> int:
    """Return a heart rate value for a given customer profile."""
    if random.random() < ANOMALY_PROBABILITY:
        # Anomaly: very high or very low
        return random.choice([
            random.randint(20, 39),    # dangerously low
            random.randint(151, 220),  # dangerously high
        ])
    # Normal reading: Gaussian around baseline
    bpm = int(random.gauss(profile["baseline"], profile["variance"]))
    return max(30, min(220, bpm))  # clamp to physiological range


def generate_reading(customer_id: str) -> dict:
    """Build a single heart beat record."""
    profile = CUSTOMER_PROFILES[customer_id]
    return {
        "customer_id":  customer_id,
        "customer_name": profile["name"],
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "heart_rate":   generate_heart_rate(profile),
        "age":          profile["age"],
    }


def stream_readings(interval_seconds: float = 1.0, burst: bool = False):
    """
    Generator: yields one reading per customer on every tick.

    Parameters
    ----------
    interval_seconds : float
        Pause between ticks (default 1 s).
    burst : bool
        If True, yield all readings instantly (useful for tests / back-fill).
    """
    customer_ids = list(CUSTOMER_PROFILES.keys())
    while True:
        random.shuffle(customer_ids)          # vary the order each tick
        for cid in customer_ids:
            reading = generate_reading(cid)
            yield reading
        if not burst:
            time.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# Standalone usage – run directly to preview data in the terminal
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== Heart Beat Data Generator ===")
    print("Streaming readings (Ctrl-C to stop)...\n")
    try:
        for record in stream_readings(interval_seconds=0.5):
            print(json.dumps(record))
    except KeyboardInterrupt:
        print("\nStopped.")
