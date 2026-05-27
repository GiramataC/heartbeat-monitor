# ❤️ Real-Time Customer Heart Beat Monitoring System

A complete data engineering pipeline that simulates heart rate sensor data, streams it through Apache Kafka, and stores it in PostgreSQL — with a live Streamlit dashboard.

---

## Architecture Overview

```
┌─────────────────────┐        ┌──────────────────────┐        ┌──────────────────────┐
│   Data Generator    │──────▶│    Kafka Broker       │──────▶│   Kafka Consumer     │
│  data_generator.py  │       │   Topic: heartbeat    │       │  kafka_consumer.py   │
│  kafka_producer.py  │       │  (Confluent Kafka 7.x)│       │  • Validates data    │
│                     │       │                       │       │  • Flags anomalies   │
│  5 Customers        │       │  Messages: JSON        │       │  • Inserts to DB     │
│  ~1 reading/sec     │       │  Retention: 7 days    │       └──────────┬───────────┘
└─────────────────────┘        └──────────────────────┘                  │
                                                                          ▼
                                                               ┌──────────────────────┐
                                                               │     PostgreSQL        │
                                                               │   heartbeat_db        │
                                                               │  • heart_rate_readings│
                                                               │  • customers          │
                                                               │  • mv_hourly_stats    │
                                                               └──────────┬───────────┘
                                                                          │
                                                                          ▼
                                                               ┌──────────────────────┐
                                                               │  Streamlit Dashboard  │
                                                               │   dashboard.py        │
                                                               │                       │
                                                               └──────────────────────┘
```

### Data Flow

1. **`data_generator.py`** creates realistic heart rate readings (with ~5 % anomalies) for 5 customers at ~1 Hz.
2. **`kafka_producer.py`** serialises each reading as JSON and publishes it to the `heartbeat` Kafka topic.
3. **`kafka_consumer.py`** reads from the topic, validates schema, classifies BPM as NORMAL / WARNING / CRITICAL, and inserts valid records into PostgreSQL.
4. **`dashboard.py`** queries PostgreSQL in real time and displays live charts and alerts.

---

## Project Structure

```
heartbeat/
├── scripts/
│   ├── data_generator.py   # Synthetic data simulation
│   ├── kafka_producer.py   # Kafka producer
│   ├── kafka_consumer.py   # Kafka consumer + DB writer
│   └── dashboard.py        # Streamlit dashboard
├── sql/
│   └── schema.sql          # PostgreSQL schema
├── docker/
│   └── docker-compose.yml  # Full stack: Kafka + ZK + Postgres + UIs
├── tests/
│   └── test_pipeline.py    # Unit & integration tests
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | ≥ 3.11 | |
| Docker | ≥ 24 | |
| Docker Compose | ≥ 2.x | bundled with Docker Desktop |

### 1 – Clone & install Python dependencies

```bash
git clone https://github.com/https://github.com/GiramataC/heartbeat-monitor.git
cd heartbeat-monitor
pip install -r requirements.txt
```

### 2 – Start the infrastructure

```bash
cd docker
docker-compose up -d
```

Wait ~30 s for all services to be healthy:

```bash
docker-compose ps
# All services should show "healthy"
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | – |
| pgAdmin | http://localhost:5050 | admin@heartbeat.local / admin |
| PostgreSQL | localhost:5434 | heartbeat_user / heartbeat_pass |

### 3 – Run the producer (Terminal 1)

```bash
cd scripts
python kafka_producer.py
# Optional flags:
# --bootstrap localhost:9092   (default)
# --topic heartbeat            (default)
# --interval 1.0               (seconds between rounds)
```

### 4 – Run the consumer (Terminal 2)

```bash
cd scripts
python kafka_consumer.py
```

You'll see log lines like:

```
2025-05-24 10:00:01 [INFO]  ✓  [C001] Alice Kamali         BPM=72   status=NORMAL
2025-05-24 10:00:01 [WARNING] ⚠  ANOMALY [C004] David Habimana – BPM=162  status=CRITICAL
```

### 5 – (Optional) Launch the dashboard

```bash
cd scripts
streamlit run dashboard.py
# Opens at http://localhost:8501
```

---

## Configuration

All scripts accept environment variables for connection settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DB` | `heartbeat_db` | Database name |
| `PG_USER` | `heartbeat_user` | DB user |
| `PG_PASSWORD` | `heartbeat_pass` | DB password |

Example override:

```bash
PG_HOST=myserver PG_PASSWORD=secret python kafka_consumer.py
```

---

## Heart Rate Classification

| Status | BPM Range | Action |
|--------|-----------|--------|
| `CRITICAL` | < 40 or > 150 | Immediate alert logged |
| `WARNING` | 40–49 or 131–150 | Warning logged |
| `NORMAL` | 50–130 | Stored silently |

---

## Database Schema

```sql
-- Main fact table
heart_rate_readings (
    id                BIGSERIAL PRIMARY KEY,
    customer_id       VARCHAR(10),        -- FK → customers
    customer_name     VARCHAR(120),
    reading_timestamp TIMESTAMPTZ,        -- sensor time (UTC)
    heart_rate        SMALLINT,           -- BPM
    status            VARCHAR(10),        -- NORMAL | WARNING | CRITICAL
    age               SMALLINT,
    ingested_at       TIMESTAMPTZ         -- pipeline write time
)
```

Indexes on `reading_timestamp`, `(customer_id, reading_timestamp)`, and a partial index on anomalous `status` values for fast dashboard queries.

---

## Running Tests

```bash
# Unit tests only (no infrastructure needed)
pytest tests/test_pipeline.py -v

# All tests including integration (requires docker-compose stack)
pytest tests/test_pipeline.py -v -m "not integration"   # skip integration
pytest tests/test_pipeline.py -v                        # run all
```

---

## Stopping Everything

```bash
cd docker
docker-compose down          # stop containers, keep volumes
docker-compose down -v       # stop + delete all data
```

---

## Useful SQL Queries

```sql
-- Latest reading per customer
SELECT DISTINCT ON (customer_id)
    customer_id, customer_name, heart_rate, status, reading_timestamp
FROM heart_rate_readings
ORDER BY customer_id, reading_timestamp DESC;

-- Anomalies in the last hour
SELECT * FROM heart_rate_readings
WHERE status <> 'NORMAL'
  AND reading_timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY reading_timestamp DESC;

-- Average BPM per customer (last 30 min)
SELECT customer_id, ROUND(AVG(heart_rate)::NUMERIC, 1) AS avg_bpm
FROM heart_rate_readings
WHERE reading_timestamp >= NOW() - INTERVAL '30 minutes'
GROUP BY customer_id;
```

---

