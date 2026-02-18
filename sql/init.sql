-- ============================================================================
-- Real-Time Customer Heartbeat Monitoring System
-- Database Initialization Script
-- ============================================================================
-- This script creates the schema, tables, indexes, and functions needed
-- for storing and querying customer heartbeat time-series data.
-- ============================================================================

-- Create extension for UUID generation (if needed in future)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─── Schema ────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS heartbeat;

-- ─── Customers Table ───────────────────────────────────────────────────────
-- Stores the synthetic customer profiles
CREATE TABLE IF NOT EXISTS heartbeat.customers (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    age             INTEGER CHECK (age > 0 AND age < 150),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE heartbeat.customers IS 'Stores synthetic customer profile data';

-- ─── Heart Rate Readings Table ─────────────────────────────────────────────
-- Core time-series table for heart rate data
CREATE TABLE IF NOT EXISTS heartbeat.heart_rate_readings (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     VARCHAR(50) NOT NULL REFERENCES heartbeat.customers(customer_id),
    heart_rate      INTEGER NOT NULL CHECK (heart_rate > 0 AND heart_rate < 300),
    timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    is_anomaly      BOOLEAN DEFAULT FALSE,
    anomaly_type    VARCHAR(20),  -- 'HIGH', 'LOW', or NULL
    ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE heartbeat.heart_rate_readings IS 'Stores real-time heart rate readings from customers';
COMMENT ON COLUMN heartbeat.heart_rate_readings.is_anomaly IS 'Flag indicating if the reading is outside normal thresholds';
COMMENT ON COLUMN heartbeat.heart_rate_readings.anomaly_type IS 'Type of anomaly: HIGH (>150 bpm) or LOW (<50 bpm)';

-- ─── Indexes for Efficient Querying ────────────────────────────────────────
-- Index on timestamp for time-range queries (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_readings_timestamp
    ON heartbeat.heart_rate_readings (timestamp DESC);

-- Composite index for customer + time range queries
CREATE INDEX IF NOT EXISTS idx_readings_customer_timestamp
    ON heartbeat.heart_rate_readings (customer_id, timestamp DESC);

-- Index for anomaly filtering
CREATE INDEX IF NOT EXISTS idx_readings_anomaly
    ON heartbeat.heart_rate_readings (is_anomaly)
    WHERE is_anomaly = TRUE;

-- Index on ingested_at for data pipeline monitoring
CREATE INDEX IF NOT EXISTS idx_readings_ingested_at
    ON heartbeat.heart_rate_readings (ingested_at DESC);

-- ─── Anomaly Log Table ─────────────────────────────────────────────────────
-- Dedicated table for anomaly events (for alerting and reporting)
CREATE TABLE IF NOT EXISTS heartbeat.anomaly_log (
    id              BIGSERIAL PRIMARY KEY,
    reading_id      BIGINT REFERENCES heartbeat.heart_rate_readings(id),
    customer_id     VARCHAR(50) NOT NULL REFERENCES heartbeat.customers(customer_id),
    heart_rate      INTEGER NOT NULL,
    anomaly_type    VARCHAR(20) NOT NULL,
    timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
    detected_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE heartbeat.anomaly_log IS 'Dedicated log for anomalous heart rate events';

CREATE INDEX IF NOT EXISTS idx_anomaly_log_customer
    ON heartbeat.anomaly_log (customer_id, timestamp DESC);

-- ─── Pipeline Metrics Table ────────────────────────────────────────────────
-- Tracks pipeline performance and health
CREATE TABLE IF NOT EXISTS heartbeat.pipeline_metrics (
    id              BIGSERIAL PRIMARY KEY,
    metric_name     VARCHAR(100) NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    recorded_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE heartbeat.pipeline_metrics IS 'Pipeline performance and health metrics';

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name_time
    ON heartbeat.pipeline_metrics (metric_name, recorded_at DESC);

-- ─── Useful Views ──────────────────────────────────────────────────────────

-- Latest reading per customer
CREATE OR REPLACE VIEW heartbeat.latest_readings AS
SELECT DISTINCT ON (customer_id)
    customer_id,
    heart_rate,
    timestamp,
    is_anomaly,
    anomaly_type
FROM heartbeat.heart_rate_readings
ORDER BY customer_id, timestamp DESC;

-- Hourly average heart rate per customer
CREATE OR REPLACE VIEW heartbeat.hourly_avg_heart_rate AS
SELECT
    customer_id,
    DATE_TRUNC('hour', timestamp) AS hour,
    ROUND(AVG(heart_rate)::numeric, 1) AS avg_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    MAX(heart_rate) AS max_heart_rate,
    COUNT(*) AS reading_count,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
FROM heartbeat.heart_rate_readings
GROUP BY customer_id, DATE_TRUNC('hour', timestamp)
ORDER BY customer_id, hour DESC;

-- Customer summary statistics
CREATE OR REPLACE VIEW heartbeat.customer_summary AS
SELECT
    c.customer_id,
    c.customer_name,
    c.age,
    COUNT(r.id) AS total_readings,
    ROUND(AVG(r.heart_rate)::numeric, 1) AS avg_heart_rate,
    MIN(r.heart_rate) AS min_heart_rate,
    MAX(r.heart_rate) AS max_heart_rate,
    ROUND(STDDEV(r.heart_rate)::numeric, 2) AS stddev_heart_rate,
    SUM(CASE WHEN r.is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
    MIN(r.timestamp) AS first_reading,
    MAX(r.timestamp) AS last_reading
FROM heartbeat.customers c
LEFT JOIN heartbeat.heart_rate_readings r ON c.customer_id = r.customer_id
GROUP BY c.customer_id, c.customer_name, c.age;

-- ─── Helper Functions ──────────────────────────────────────────────────────

-- Function to insert a heart rate reading with automatic anomaly detection
CREATE OR REPLACE FUNCTION heartbeat.insert_reading(
    p_customer_id VARCHAR(50),
    p_heart_rate INTEGER,
    p_timestamp TIMESTAMP WITH TIME ZONE,
    p_anomaly_low_threshold INTEGER DEFAULT 50,
    p_anomaly_high_threshold INTEGER DEFAULT 150
) RETURNS BIGINT AS $$
DECLARE
    v_is_anomaly BOOLEAN;
    v_anomaly_type VARCHAR(20);
    v_reading_id BIGINT;
BEGIN
    -- Determine anomaly status
    IF p_heart_rate < p_anomaly_low_threshold THEN
        v_is_anomaly := TRUE;
        v_anomaly_type := 'LOW';
    ELSIF p_heart_rate > p_anomaly_high_threshold THEN
        v_is_anomaly := TRUE;
        v_anomaly_type := 'HIGH';
    ELSE
        v_is_anomaly := FALSE;
        v_anomaly_type := NULL;
    END IF;

    -- Insert the reading
    INSERT INTO heartbeat.heart_rate_readings (
        customer_id, heart_rate, timestamp, is_anomaly, anomaly_type
    ) VALUES (
        p_customer_id, p_heart_rate, p_timestamp, v_is_anomaly, v_anomaly_type
    ) RETURNING id INTO v_reading_id;

    -- If anomaly, also log to anomaly table
    IF v_is_anomaly THEN
        INSERT INTO heartbeat.anomaly_log (
            reading_id, customer_id, heart_rate, anomaly_type, timestamp
        ) VALUES (
            v_reading_id, p_customer_id, p_heart_rate, v_anomaly_type, p_timestamp
        );
    END IF;

    RETURN v_reading_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION heartbeat.insert_reading IS 'Inserts a heart rate reading with automatic anomaly detection and logging';

-- ─── Seed Initial Customers ────────────────────────────────────────────────
INSERT INTO heartbeat.customers (customer_id, customer_name, age) VALUES
    ('CUST-001', 'Alice Johnson', 32),
    ('CUST-002', 'Bob Smith', 45),
    ('CUST-003', 'Charlie Brown', 28),
    ('CUST-004', 'Diana Prince', 55),
    ('CUST-005', 'Edward Norton', 38),
    ('CUST-006', 'Fiona Apple', 41),
    ('CUST-007', 'George Lucas', 67),
    ('CUST-008', 'Hannah Montana', 23),
    ('CUST-009', 'Ivan Drago', 50),
    ('CUST-010', 'Julia Roberts', 36)
ON CONFLICT (customer_id) DO NOTHING;

-- ─── Grant permissions (for application user) ──────────────────────────────
-- These would be adjusted in production for principle of least privilege
GRANT USAGE ON SCHEMA heartbeat TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA heartbeat TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA heartbeat TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA heartbeat TO PUBLIC;
