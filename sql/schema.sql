-- =============================================================================
-- schema.sql
-- PostgreSQL schema for the Real-Time Customer Heart Beat Monitoring System
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Database & user setup
--    Run these commands as the postgres superuser BEFORE running the rest.
-- ---------------------------------------------------------------------------
-- CREATE DATABASE heartbeat_db;
-- CREATE USER heartbeat_user WITH ENCRYPTED PASSWORD 'heartbeat_pass';
-- GRANT ALL PRIVILEGES ON DATABASE heartbeat_db TO heartbeat_user;
-- \c heartbeat_db heartbeat_user


-- ---------------------------------------------------------------------------
-- 2. Extensions
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- for uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";  -- optional query tuning


-- ---------------------------------------------------------------------------
-- 3. Customer dimension table
--    Stores profile information; kept separate so it can be updated
--    without touching the time-series fact table.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(10)  PRIMARY KEY,
    customer_name   VARCHAR(120) NOT NULL,
    age             SMALLINT     CHECK (age BETWEEN 0 AND 130),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed with the five demo customers
INSERT INTO customers (customer_id, customer_name, age) VALUES
    ('C001', 'Alice Kamali',   32),
    ('C002', 'Bob Nkurunziza', 45),
    ('C003', 'Carol Uwimana',  28),
    ('C004', 'David Habimana', 55),
    ('C005', 'Eva Mukamana',   38)
ON CONFLICT (customer_id) DO NOTHING;


-- ---------------------------------------------------------------------------
-- 4. Heart rate readings – the main time-series fact table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS heart_rate_readings (
    id                  BIGSERIAL    PRIMARY KEY,
    customer_id         VARCHAR(10)  NOT NULL
                            REFERENCES customers(customer_id) ON DELETE CASCADE,
    customer_name       VARCHAR(120),                 -- denormalised for fast reads
    reading_timestamp   TIMESTAMPTZ  NOT NULL,        -- when the sensor fired
    heart_rate          SMALLINT     NOT NULL
                            CHECK (heart_rate BETWEEN 1 AND 300),
    status              VARCHAR(10)  NOT NULL
                            CHECK (status IN ('NORMAL', 'WARNING', 'CRITICAL')),
    age                 SMALLINT,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),  -- when we stored it

    -- Prevent duplicate readings for the same customer at the exact same moment
    UNIQUE (customer_id, reading_timestamp)
);

COMMENT ON TABLE  heart_rate_readings                IS 'Raw heart rate measurements from customer sensors.';
COMMENT ON COLUMN heart_rate_readings.reading_timestamp IS 'UTC timestamp reported by the sensor device.';
COMMENT ON COLUMN heart_rate_readings.status         IS 'NORMAL | WARNING | CRITICAL – set by the consumer.';
COMMENT ON COLUMN heart_rate_readings.ingested_at    IS 'Wall-clock time the consumer wrote this row.';


-- ---------------------------------------------------------------------------
-- 5. Indexes for efficient querying
-- ---------------------------------------------------------------------------

-- Time-range scans (most common pattern in time-series workloads)
CREATE INDEX IF NOT EXISTS idx_hrr_reading_timestamp
    ON heart_rate_readings (reading_timestamp DESC);

-- Customer-level history lookups
CREATE INDEX IF NOT EXISTS idx_hrr_customer_time
    ON heart_rate_readings (customer_id, reading_timestamp DESC);

-- Alert / anomaly dashboard queries
CREATE INDEX IF NOT EXISTS idx_hrr_status
    ON heart_rate_readings (status)
    WHERE status <> 'NORMAL';   -- partial index – only the interesting rows

-- Ingestion-lag monitoring (ops use-case)
CREATE INDEX IF NOT EXISTS idx_hrr_ingested_at
    ON heart_rate_readings (ingested_at DESC);


-- ---------------------------------------------------------------------------
-- 6. Aggregated hourly summary (materialised view – optional but useful)
--    Refresh manually: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_stats;
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_stats AS
SELECT
    customer_id,
    date_trunc('hour', reading_timestamp)   AS hour_bucket,
    COUNT(*)                                AS reading_count,
    ROUND(AVG(heart_rate)::NUMERIC, 1)      AS avg_bpm,
    MIN(heart_rate)                         AS min_bpm,
    MAX(heart_rate)                         AS max_bpm,
    COUNT(*) FILTER (WHERE status = 'WARNING')  AS warnings,
    COUNT(*) FILTER (WHERE status = 'CRITICAL') AS criticals
FROM heart_rate_readings
GROUP BY customer_id, hour_bucket
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hourly_stats_pk
    ON mv_hourly_stats (customer_id, hour_bucket);


-- ---------------------------------------------------------------------------
-- 7. Useful query examples (reference)
-- ---------------------------------------------------------------------------

-- Last 10 readings for all customers, newest first:
-- SELECT customer_id, customer_name, reading_timestamp, heart_rate, status
-- FROM   heart_rate_readings
-- ORDER  BY reading_timestamp DESC
-- LIMIT  10;

-- Anomaly summary per customer for the last 24 hours:
-- SELECT customer_id, status, COUNT(*) AS cnt
-- FROM   heart_rate_readings
-- WHERE  reading_timestamp >= NOW() - INTERVAL '24 hours'
--   AND  status <> 'NORMAL'
-- GROUP  BY customer_id, status
-- ORDER  BY cnt DESC;

-- Average BPM per customer over the last hour:
-- SELECT customer_id, ROUND(AVG(heart_rate)::NUMERIC, 1) AS avg_bpm
-- FROM   heart_rate_readings
-- WHERE  reading_timestamp >= NOW() - INTERVAL '1 hour'
-- GROUP  BY customer_id;

-- Ingestion lag (sensor time vs DB write time):
-- SELECT customer_id,
--        EXTRACT(EPOCH FROM (ingested_at - reading_timestamp)) AS lag_seconds
-- FROM   heart_rate_readings
-- ORDER  BY reading_timestamp DESC
-- LIMIT  20;
