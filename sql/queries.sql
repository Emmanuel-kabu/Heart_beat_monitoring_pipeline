-- ============================================================================
-- Useful Queries for Heartbeat Monitoring System
-- ============================================================================

-- 1. Get the latest heart rate for all customers
SELECT * FROM heartbeat.latest_readings;

-- 2. Get all anomalies in the last hour
SELECT
    al.customer_id,
    c.customer_name,
    al.heart_rate,
    al.anomaly_type,
    al.timestamp
FROM heartbeat.anomaly_log al
JOIN heartbeat.customers c ON al.customer_id = c.customer_id
WHERE al.timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY al.timestamp DESC;

-- 3. Hourly average heart rate for a specific customer
SELECT *
FROM heartbeat.hourly_avg_heart_rate
WHERE customer_id = 'CUST-001'
ORDER BY hour DESC
LIMIT 24;

-- 4. Customers with the most anomalies
SELECT *
FROM heartbeat.customer_summary
ORDER BY anomaly_count DESC;

-- 5. Heart rate distribution (histogram)
SELECT
    CASE
        WHEN heart_rate < 50 THEN 'Very Low (<50)'
        WHEN heart_rate BETWEEN 50 AND 59 THEN 'Low (50-59)'
        WHEN heart_rate BETWEEN 60 AND 100 THEN 'Normal (60-100)'
        WHEN heart_rate BETWEEN 101 AND 150 THEN 'Elevated (101-150)'
        ELSE 'Very High (>150)'
    END AS heart_rate_range,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM heartbeat.heart_rate_readings
GROUP BY 1
ORDER BY MIN(heart_rate);

-- 6. Rolling average heart rate (5-minute window)
SELECT
    customer_id,
    timestamp,
    heart_rate,
    ROUND(AVG(heart_rate) OVER (
        PARTITION BY customer_id
        ORDER BY timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )::numeric, 1) AS rolling_avg_5
FROM heartbeat.heart_rate_readings
WHERE customer_id = 'CUST-001'
ORDER BY timestamp DESC
LIMIT 50;

-- 7. Pipeline ingestion delay statistics
SELECT
    ROUND(AVG(EXTRACT(EPOCH FROM (ingested_at - timestamp)))::numeric, 3) AS avg_delay_sec,
    ROUND(MAX(EXTRACT(EPOCH FROM (ingested_at - timestamp)))::numeric, 3) AS max_delay_sec,
    ROUND(MIN(EXTRACT(EPOCH FROM (ingested_at - timestamp)))::numeric, 3) AS min_delay_sec,
    COUNT(*) AS total_readings
FROM heartbeat.heart_rate_readings
WHERE ingested_at >= NOW() - INTERVAL '1 hour';

-- 8. Total records count and date range
SELECT
    COUNT(*) AS total_records,
    MIN(timestamp) AS earliest_reading,
    MAX(timestamp) AS latest_reading,
    MAX(timestamp) - MIN(timestamp) AS data_span
FROM heartbeat.heart_rate_readings;
