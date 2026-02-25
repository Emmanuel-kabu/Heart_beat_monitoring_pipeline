# 💓 Real-Time Customer Heartbeat Monitoring System

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.x-orange.svg)](https://kafka.apache.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C.svg)](https://prometheus.io/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade data pipeline that simulates, streams, validates, and stores real-time heart rate data using **Python**, **Apache Kafka**, and **PostgreSQL**. Features **Prometheus** metrics collection with **Grafana** dashboarding, a **Dead Letter Queue (DLQ)** for failed messages, edge-case detection for 5+ scenarios, **Slack & Email** alerting, scheduled daily/weekly reports, and a **Kafka UI** for topic inspection.

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Running the Pipeline](#-running-the-pipeline)
- [Monitoring & Observability](#-monitoring--observability)
- [Edge-Case Detection](#-edge-case-detection)
- [Dead Letter Queue (DLQ)](#-dead-letter-queue-dlq)
- [Alerting (Slack & Email)](#-alerting-slack--email)
- [Scheduled Reporting](#-scheduled-reporting)
- [Dashboards](#-dashboards)
- [Testing](#-testing)
- [Database Schema](#-database-schema)
- [Data Flow](#-data-flow)
- [API Reference](#-api-reference)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Project Overview

This system demonstrates a modern data engineering pipeline that:

1. **Generates** synthetic heartbeat sensor data for 10 simulated customers
2. **Streams** data in real-time through Apache Kafka
3. **Validates** and detects anomalies (heart rates too high or too low)
4. **Stores** enriched data in a PostgreSQL database with optimized indexes
5. **Monitors** pipeline health via Prometheus metrics (30+ metrics)
6. **Detects** edge cases — device delay, device failure, sudden spikes, data quality degradation, system health
7. **Alerts** via Slack (all severities) and Email (critical issues)
8. **Reports** daily and weekly summaries automatically via Slack
9. **Quarantines** failed messages in a Dead Letter Queue for retry
10. **Validates** data quality with a dedicated engine (Great Expectations + custom rules)
11. **Visualizes** data through Grafana (Prometheus + PostgreSQL) dashboards

### Key Features

- ✅ Realistic synthetic data generation with Gaussian distributions
- ✅ Kafka-based real-time streaming with exactly-once semantics
- ✅ Configurable anomaly detection (LOW < 50 bpm, HIGH > 150 bpm)
- ✅ PostgreSQL with connection pooling and optimized time-series indexes
- ✅ Batch processing for high-throughput database writes
- ✅ **Prometheus metrics** — 30+ counters, gauges, histograms exposed at `/metrics`
- ✅ **5 Edge-case detectors** — heartbeat delay, device failure, sudden spike, data quality, system health
- ✅ **Dead Letter Queue (DLQ)** — failed messages routed to `customer_heartbeat_dlq` with retry logic
- ✅ **Slack alerting** — data quality reports, system health, spike alerts, device failures, daily/weekly reports
- ✅ **Email alerting** — critical issues (device failure, sustained anomalies, pipeline down, DLQ overflow)
- ✅ **Kafka UI** — web-based topic, consumer, and cluster inspection
- ✅ **Data Quality Engine** — Great Expectations + custom pandas rules with circuit breaker
- ✅ **Scheduled reporting** — daily & weekly Slack summaries + monthly email reports via Gmail SMTP
- ✅ **Prometheus alert rules** — 10 pre-configured rules with Alertmanager routing
- ✅ Graceful shutdown handling with signal trapping
- ✅ Comprehensive logging with rotation
- ✅ Docker Compose for one-command infrastructure setup
- ✅ Full test suite (unit + integration)

---

## 🏗️ Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Synthetic   │    │    Kafka     │    │    Kafka     │    │  PostgreSQL  │
│     Data      │───▶│   Producer   │───▶│   Consumer   │───▶│   Database   │
│   Generator   │    │              │    │  + Validator  │    │              │
└──────────────┘    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
                           │                   │                   │
                    ┌──────▼───────┐     ┌──────▼───────┐   ┌──────▼───────┐
                    │   Kafka UI   │     │  DLQ Topic   │   │   Grafana    │
                    │  (port 8080) │     │ (retry/park) │   │ (port 3000)  │
                    └──────────────┘     └──────────────┘   └──────┬───────┘
                                                                   │
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────▼───────┐
│  Edge-Case   │───▶│    Slack     │    │    Email     │    │  Prometheus  │
│  Detector    │───▶│   Webhook    │    │   (SMTP)     │    │ (port 9090)  │
└──────┬───────┘    └──────────────┘    └──────────────┘    └──────┬───────┘
       │                                                          │
┌──────▼───────┐                                           ┌──────▼───────┐
│  Reporter    │                                           │ Alertmanager │
│ (daily/wkly) │                                           │ (port 9093)  │
└──────────────┘                                           └──────────────┘
```

For detailed diagrams, see [docs/data_flow_diagram.md](docs/data_flow_diagram.md).

---

## 🛠️ Tech Stack

| Component          | Technology                          | Purpose                          |
|--------------------|-------------------------------------|----------------------------------|
| Data Generator     | Python 3.11                         | Synthetic heartbeat data         |
| Message Broker     | Apache Kafka 7.5 (Confluent)        | Real-time data streaming         |
| Coordination       | Zookeeper                           | Kafka cluster management         |
| Database           | PostgreSQL 16                       | Persistent data storage          |
| Python Kafka       | confluent-kafka 2.3.0               | Producer/Consumer implementation |
| Python DB          | psycopg2 2.9.9                      | PostgreSQL adapter               |
| Metrics Collection | Prometheus 2.48 + prometheus-client | Pipeline & system metrics        |
| Alert Routing      | Alertmanager 0.26                   | Prometheus alert delivery        |
| Dashboard          | Grafana 10.2                        | Prometheus + PostgreSQL + DQ dashboards |
| Kafka Monitoring   | Kafka UI 0.7.1 (Provectus)         | Topic, consumer & cluster UI     |
| Alerting           | Slack Webhooks / SMTP Email         | Notifications & critical alerts  |
| Orchestration      | Docker Compose                      | Infrastructure management        |
| Testing            | pytest 7.4                          | Unit and integration tests       |

---

## 📁 Project Structure

```
Heart_beat_monitoring_pipeline/
├── docker-compose_kafka.yml    # Infrastructure services definition
├── Dockerfile                  # Python application container
├── Makefile                    # Convenience commands
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Python project configuration
├── setup.py                    # Package setup script
├── .env.example                # Environment variables template
├── .gitignore                  # Git ignore rules
├── CONTRIBUTING.md             # Contribution guidelines
├── LICENSE                     # MIT License
│
├── src/                        # Source code
│   ├── __init__.py
│   ├── config.py               # Centralized configuration (incl. monitoring & alerting)
│   ├── logger.py               # Structured logging setup
│   ├── models.py               # Data models (HeartbeatReading, CustomerProfile)
│   ├── main.py                 # Pipeline orchestrator & CLI entry point
│   │
│   ├── generator/              # Data generation module
│   │   ├── __init__.py
│   │   └── heartbeat_generator.py
│   │
│   ├── kafka_client/           # Kafka producer & consumer
│   │   ├── __init__.py
│   │   ├── producer.py
│   │   └── consumer.py
│   │
│   ├── database/               # PostgreSQL handler
│   │   ├── __init__.py
│   │   └── db_handler.py
│   │
│   ├── validation/             # Data validation & anomaly detection
│   │   ├── __init__.py
│   │   └── validator.py
│   │
│   ├── monitoring/             # Observability & alerting
│   │   ├── __init__.py
│   │   ├── metrics.py          # Prometheus metrics (30+ counters/gauges/histograms)
│   │   ├── alerting.py         # Slack & Email alert senders
│   │   ├── dlq.py              # Dead Letter Queue producer & consumer
│   │   ├── edge_cases.py       # 5 edge-case detectors
│   │   └── reporting.py        # Daily, weekly & monthly scheduled reports
│   │
│   ├── data_quality/           # Data quality validation engine
│   │   ├── __init__.py
│   │   ├── expectations.py     # Great Expectations rule definitions
│   │   ├── quality_engine.py   # DQ engine (GE + custom pandas rules)
│   │   ├── quality_reporter.py # Scheduled DQ reports (daily/weekly)
│   │   └── quality_store.py    # In-memory DQ snapshot store & trends
│   │
│   └── dashboard/              # Streamlit dashboard (legacy)
│       ├── __init__.py
│       └── app.py
│
├── sql/                        # Database scripts
│   ├── init.sql                # Schema creation & seed data
│   └── queries.sql             # Useful analytical queries
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── conftest.py             # Pytest fixtures
│   ├── test_generator.py       # Unit tests: data generator
│   ├── test_validator.py       # Unit tests: validator
│   ├── test_integration.py     # Integration tests
│   └── test_pipeline_script.py # Standalone test script
│
├── grafana/                    # Grafana configuration
│   └── provisioning/
│       ├── dashboards/
│       │   ├── dashboard.yml
│       │   ├── heartbeat_dashboard.json       # PostgreSQL-backed dashboard
│       │   ├── data_quality_dashboard.json    # Data quality metrics dashboard
│       │   └── prometheus_dashboard.json      # Prometheus metrics dashboard
│       └── datasources/
│           └── datasource.yml                 # PostgreSQL + Prometheus datasources
│
├── prometheus/                 # Prometheus & Alertmanager config
│   ├── prometheus.yml          # Scrape targets & settings
│   ├── alert_rules.yml         # 10 alert rules (5 edge cases + DLQ + anomaly)
│   └── alertmanager.yml        # Routing: Slack (all) + Email (critical)
│
├── docs/                       # Documentation
│   ├── architecture_diagram.md # Architecture diagrams (Mermaid)
│   ├── data_flow_diagram.md    # Data flow diagrams (Mermaid)
│   └── architecture_decisions.md
│
└── logs/                       # Runtime log files (gitignored)
```

---

## Prerequisites

- **Python 3.10+** (3.11 recommended)
- **Docker** & **Docker Compose** (for infrastructure services)
- **Git** (for version control)

### Optional
- **Make** (for convenience commands via Makefile)

### Docker Services Started by `docker-compose_kafka.yml`

| Service        | Image                            | Port(s)           | Purpose                       |
|---------------|----------------------------------|--------------------|-------------------------------|
| Zookeeper     | confluentinc/cp-zookeeper:7.5.0  | 2181               | Kafka coordination            |
| Kafka         | confluentinc/cp-kafka:7.5.0      | 9092, 29092        | Message broker                |
| PostgreSQL    | postgres:16-alpine               | 5433               | Data storage                  |
| Prometheus    | prom/prometheus:v2.48.0          | 9090               | Metrics collection & alerting |
| Alertmanager  | prom/alertmanager:v0.26.0        | 9093               | Alert routing (Slack/Email)   |
| Grafana       | grafana/grafana:10.2.0           | 3000               | Dashboarding (admin/bukes123) |
| Kafka UI      | provectuslabs/kafka-ui:v0.7.1   | 8080               | Kafka topics & consumers UI   |

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Emmanuel-kabu/Heart_beat_monitoring_pipeline.git
cd Heart_beat_monitoring_pipeline
```

### 2. Set Up Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate         # Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment config
cp .env.example .env
```

### 3. Start Infrastructure

```bash
# Start Kafka, Zookeeper, PostgreSQL, Grafana, Prometheus, Alertmanager, and Kafka UI
docker-compose -f docker-compose_kafka.yml up -d

# Wait for services to be healthy (~15-30 seconds)
docker-compose -f docker-compose_kafka.yml ps
```

### 4. Run the Pipeline

```bash
# Full pipeline (producer + consumer + Prometheus metrics + edge detection + DLQ + reporting)
python -m src.main

# Or use Makefile
make start-pipeline
```

### 5. Access the UIs

| Service          | URL                         | Credentials    |
|------------------|-----------------------------|--------------|
| Kafka UI         | http://localhost:8080        | —            |
| Prometheus       | http://localhost:9090        | —            |
| Alertmanager     | http://localhost:9093        | —            |
| Grafana          | http://localhost:3000        | admin/bukes123 |
| Pipeline Metrics | http://localhost:8000/metrics | —            |

```bash
# Open all UIs at once (Windows)
make open-all-uis
```

---

## Configuration

All settings can be configured via environment variables (`.env` file):

| Variable                    | Default              | Description                        |
|-----------------------------|----------------------|------------------------------------|
| **PostgreSQL** | | |
| `POSTGRES_HOST`             | `localhost`          | PostgreSQL host                    |
| `POSTGRES_PORT`             | `5433`               | PostgreSQL port                    |
| `POSTGRES_DB`               | `heartbeat_db`       | Database name                      |
| `POSTGRES_USER`             | `heartbeat_user`     | Database user                      |
| `POSTGRES_PASSWORD`         | `heartbeat_pass123`  | Database password                  |
| **Kafka** | | |
| `KAFKA_BOOTSTRAP_SERVERS`   | `localhost:9092`     | Kafka broker addresses             |
| `KAFKA_TOPIC`               | `customer_heartbeat` | Kafka topic name                   |
| `KAFKA_GROUP_ID`            | `heartbeat_consumer_group` | Consumer group ID            |
| **Generator** | | |
| `NUM_CUSTOMERS`             | `10`                 | Number of simulated customers      |
| `GENERATION_INTERVAL_SEC`   | `1.0`                | Seconds between data generation    |
| `ANOMALY_LOW_THRESHOLD`     | `50`                 | Low anomaly threshold (bpm)        |
| `ANOMALY_HIGH_THRESHOLD`    | `150`                | High anomaly threshold (bpm)       |
| **Monitoring & Metrics** | | |
| `PROMETHEUS_METRICS_PORT`   | `8000`               | Port for Prometheus `/metrics` endpoint |
| `HEARTBEAT_DELAY_THRESHOLD_SEC` | `30`             | Seconds before a device is considered delayed |
| `DEVICE_FAILURE_THRESHOLD_SEC` | `120`              | Seconds before a device is marked FAILED |
| `SPIKE_DELTA_THRESHOLD`     | `40`                 | Min HR delta between consecutive reads for spike alert |
| `SUSTAINED_ANOMALY_COUNT`   | `5`                  | Anomalies in window to trigger critical email |
| `SUSTAINED_ANOMALY_WINDOW_MIN` | `10`              | Rolling window in minutes for sustained anomalies |
| `EDGE_CHECK_INTERVAL_SEC`   | `15`                 | Background device-health check interval |
| `DLQ_ALERT_THRESHOLD`       | `100`                | DLQ message count to trigger email alert |
| **Alerting** | | |
| `SLACK_WEBHOOK_URL`         | *(empty)*            | Slack Incoming Webhook URL (general alerts) |
| `SLACK_DAILY_WEBHOOK_URL`   | *(empty)*            | Slack webhook for daily reports    |
| `SLACK_DAILY_CHANNEL`       | *(empty)*            | Slack channel for daily reports    |
| `SLACK_WEEKLY_WEBHOOK_URL`  | *(empty)*            | Slack webhook for weekly reports   |
| `SLACK_WEEKLY_CHANNEL`      | *(empty)*            | Slack channel for weekly reports   |
| `SMTP_HOST`                 | *(empty)*            | SMTP server hostname               |
| `SMTP_PORT`                 | `587`                | SMTP server port (TLS)             |
| `SMTP_USER`                 | *(empty)*            | SMTP username / sender email       |
| `SMTP_PASSWORD`             | *(empty)*            | SMTP password or app password      |
| `ALERT_EMAIL_RECIPIENTS`    | *(empty)*            | Comma-separated recipient emails   |
| **General** | | |
| `LOG_LEVEL`                 | `INFO`               | Logging level                      |

---

## ▶️ Running the Pipeline

### Full Pipeline

Runs producer, consumer, Prometheus metrics server, edge-case detector, DLQ, and reporter in separate threads:

```bash
python -m src.main --mode full
```

### Producer Only

Generates data and sends to Kafka (no database writes):

```bash
python -m src.main --mode producer
```

### Consumer Only

Reads from Kafka and writes to PostgreSQL, with DLQ routing, edge-case detection, and reporting:

```bash
python -m src.main --mode consumer
```

### DLQ Retry

Reprocesses messages from the Dead Letter Queue — validates and re-inserts into DB:

```bash
python -m src.main --mode dlq-retry
```

### Data Quality Report

Prints an on-demand data quality status report to the console:

```bash
python -m src.main --mode dq-report
```

### Background Start / Stop

```bash
# Start the pipeline in the background (Windows)
make start

# Stop the background pipeline
make stop
```

### CLI Options

```bash
python -m src.main --help

# Options:
#   --mode {full,producer,consumer,dlq-retry,dq-report}  Pipeline mode (default: full)
#   --log-level {DEBUG,INFO,WARNING,ERROR}      Override log level
```

---

## 📊 Monitoring & Observability

### Prometheus Metrics

The pipeline exposes **30+ metrics** at `http://localhost:8000/metrics`, scraped by Prometheus every 10 seconds:

| Category | Metrics | Type |
|----------|---------|------|
| Throughput | `heartbeat_readings_generated_total`, `_produced_total`, `_consumed_total`, `_persisted_total`, `_rejected_total` | Counter |
| Device Health | `heartbeat_device_last_seen_timestamp`, `_delay_seconds`, `_status`, `_consecutive_errors` | Gauge |
| Spikes | `heartbeat_spike_detected_total` | Counter |
| Anomalies | `heartbeat_anomalies_detected_total`, `_anomaly_rate_percent` | Counter/Gauge |
| Data Quality | `heartbeat_data_quality_valid_total`, `_invalid_total`, `_score_percent` | Counter/Gauge |
| DLQ | `heartbeat_dlq_messages_total`, `_retries_total`, `_retry_success_total` | Counter |
| Latency | `heartbeat_processing_latency_seconds`, `_kafka_produce_latency_seconds`, `_db_insert_latency_seconds` | Histogram |
| System | `heartbeat_kafka_producer_connected`, `_consumer_connected`, `_database_connected`, `_pipeline_uptime_seconds` | Gauge |
| Reporting | `heartbeat_report_readings_daily`, `_weekly`, `_anomalies_daily`, `_anomalies_weekly` | Gauge |

### Prometheus Alert Rules

10 pre-configured rules in `prometheus/alert_rules.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `HeartbeatDeviceDelayed` | No data from device > 30s | warning |
| `HeartbeatDeviceFailed` | Device status = 0 for 2m | critical |
| `HeartbeatSuddenSpike` | > 3 spikes in 5 min | warning |
| `HeartbeatDataQualityLow` | Quality score < 90% for 5m | warning |
| `HeartbeatHighRejectionRate` | Rejection rate > 10% over 5m | warning |
| `KafkaProducerDown` | Producer disconnected 1m | critical |
| `KafkaConsumerDown` | Consumer disconnected 1m | critical |
| `DatabaseDown` | DB unreachable 1m | critical |
| `HighProcessingLatency` | P95 latency > 5s for 5m | warning |
| `DLQMessageAccumulation` | > 50 DLQ messages in 1h | warning |
| `HighAnomalyRate` | Anomaly rate > 15% for 5m | warning |

---

## 🔍 Edge-Case Detection

The `EdgeCaseDetector` runs a background thread and processes every reading:

| # | Edge Case | Detection Logic | Alert |
|---|-----------|----------------|-------|
| 1 | **Heartbeat Delay** | No reading from device for > 30s (configurable) | Slack warning |
| 2 | **Device Failure** | No reading for > 120s (configurable) → marked FAILED | Slack critical + Email |
| 3 | **Sudden Spike** | HR delta between consecutive reads > 40 bpm (configurable) | Slack warning |
| 4 | **Data Quality** | Tracks valid/invalid ratio; alerts if quality < 90% | Prometheus alert |
| 5 | **System Health** | Monitors Kafka producer/consumer and DB connectivity | Prometheus alert + Email |

Additional: **Sustained anomalies** — if a device produces ≥ 5 anomalies within 10 minutes, a critical email is sent.

All thresholds are configurable via environment variables and have built-in cooldowns (5 min) to prevent alert storms.

---

## 📬 Dead Letter Queue (DLQ)

Messages that fail at any stage are routed to the Kafka topic `customer_heartbeat_dlq`:

| Failure Type | Route to DLQ |
|-------------|-------------|
| Deserialization error | ✅ |
| Validation rejection | ✅ (with reason) |
| Database persist error | ✅ (all batch members) |

Each DLQ message is wrapped in an envelope:
```json
{
  "original_value": "{...}",
  "failure_reason": "validation_error: heart_rate 310 above absolute maximum",
  "failed_at": "2026-02-24T12:00:00+00:00",
  "original_topic": "customer_heartbeat",
  "original_partition": 0,
  "original_offset": 12345,
  "retry_count": 0,
  "metadata": {}
}
```

### Reprocessing

```bash
# Retry DLQ messages (max 3 retries with exponential backoff)
python -m src.main --mode dlq-retry
```

You can also inspect DLQ messages via the **Kafka UI** at `http://localhost:8080`.

---

## 🔔 Alerting (Slack & Email)

### Slack Alerts

Set `SLACK_WEBHOOK_URL` in your `.env` file. The pipeline sends:

- Data quality reports (periodic summary)
- System health reports (infrastructure status)
- Spike alerts (sudden HR changes)
- Device failure alerts (device gone silent)
- Daily processing summaries
- Weekly processing summaries

### Email Alerts (Critical Only)

Set `SMTP_HOST`, `SMTP_USER`, `SMTP_PASSWORD`, and `ALERT_EMAIL_RECIPIENTS` in `.env`. Emails are sent for:

- Device failure (no data past threshold)
- Sustained anomalies (≥ 5 anomalies in 10 min window)
- Pipeline component down (Kafka/DB unreachable)
- DLQ threshold exceeded (> 100 messages)

### Alertmanager (Prometheus)

Prometheus alert rules are routed through Alertmanager (`prometheus/alertmanager.yml`):
- **Warning** alerts → Slack channel `#heartbeat-alerts`
- **Critical** alerts → Slack `#heartbeat-critical` + Email

Update the webhook URLs and email recipients in `prometheus/alertmanager.yml`.

---

## 📅 Scheduled Reporting

The `PipelineReporter` accumulates statistics and sends automated summaries:

| Report  | Schedule               | Channel       |
|---------|------------------------|---------------|
| Daily   | 23:00 UTC daily        | Slack         |
| Weekly  | Sunday 23:00 UTC       | Slack         |
| Monthly | 1st of month, 23:00 UTC | Email (Gmail SMTP) |

Each report includes: readings processed, anomalies, DLQ count, average latency, quality score, device failures.

Monthly reports are HTML-formatted emails with summary cards and a detail table, sent via Gmail SMTP.

Reporting metrics are also exposed as Prometheus gauges for Grafana visualization.

---

## Dashboards

### Grafana Dashboards

Available at `http://localhost:3000` (login: admin/bukes123)

**Three pre-provisioned dashboards:**

#### 1. Heartbeat Dashboard (PostgreSQL)
- Total readings counter
- Active customers gauge
- Anomaly count panel
- Heart rate time-series graph
- Recent anomalies table
- Customer summary table

#### 2. Prometheus Metrics Dashboard (20+ panels)
- **Pipeline Overview** — generated, consumed, persisted, rejected counts + quality gauge + uptime
- **Edge Case Monitoring** — device delay time-series, device status timeline, spike rate, HR distribution histogram
- **Anomalies & Data Quality** — anomaly rate gauge, anomalies by customer and type
- **DLQ & System Health** — DLQ messages by reason, processing latency (p50/p95/p99), component connectivity, DB insert latency, DLQ retry success rate
- **Reporting** — daily/weekly/monthly readings and anomaly counters

#### 3. Data Quality Dashboard
- Data quality score over time
- Failing rules breakdown
- Row-level failure distribution
- Circuit breaker trip history

### Kafka UI

Available at `http://localhost:8080` — inspect topics, consumers, messages, and cluster health.

---

## Testing

### Run Unit Tests

```bash
# Run all unit tests
pytest tests/ -v --ignore=tests/test_integration.py

# Or using Makefile
make test
```

### Run Integration Tests

```bash
# Requires running infrastructure
pytest tests/test_integration.py -v -m integration

# Or
make test-integration
```

### Run Standalone Test Script

```bash
# Quick pipeline validation without infrastructure
python tests/test_pipeline_script.py

# Or
make test-script
```

### Test Coverage

```bash
pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

# Or
make test-coverage
```

---

## Database Schema

### Tables

| Table                              | Description                              |
|------------------------------------|------------------------------------------|
| `heartbeat.customers`              | Customer profiles (10 seeded)            |
| `heartbeat.heart_rate_readings`    | Core time-series heart rate data         |
| `heartbeat.anomaly_log`            | Dedicated anomaly event log              |
| `heartbeat.pipeline_metrics`       | Pipeline health metrics                  |

### Views

| View                                | Description                             |
|-------------------------------------|-----------------------------------------|
| `heartbeat.latest_readings`        | Most recent reading per customer         |
| `heartbeat.hourly_avg_heart_rate`  | Hourly aggregated statistics             |
| `heartbeat.customer_summary`       | Per-customer summary statistics          |

### Indexes

- `idx_readings_timestamp` — Fast time-range queries
- `idx_readings_customer_timestamp` — Customer + time queries
- `idx_readings_anomaly` — Partial index on anomalies
- `idx_readings_ingested_at` — Pipeline monitoring

See [sql/init.sql](sql/init.sql) for the complete schema and [sql/queries.sql](sql/queries.sql) for useful analytical queries.

---

## Data Flow

1. **Generator** creates `HeartbeatReading` objects with `customer_id`, `heart_rate`, and `timestamp`
2. **Kafka Producer** serializes readings to JSON and sends to `customer_heartbeat` topic (partitioned by `customer_id`)
3. **Kafka Consumer** polls messages in batches of 50
4. **Edge-Case Detector** inspects every reading for delay, spikes, anomalies; updates Prometheus metrics
5. **Validator** checks structural integrity and detects anomalies
6. **Valid readings** → batch-inserted into PostgreSQL with automatic anomaly logging
7. **Invalid readings** → routed to the Dead Letter Queue (`customer_heartbeat_dlq`)
8. **Reporter** accumulates stats and sends daily/weekly Slack summaries
9. **Prometheus** scrapes the `/metrics` endpoint; **Alertmanager** fires alerts to Slack/Email
10. **Grafana** queries both PostgreSQL and Prometheus for real-time visualization
11. **Kafka UI** provides web-based topic and consumer inspection

---

## API Reference

### Data Generator

```python
from src.generator.heartbeat_generator import HeartbeatGenerator

generator = HeartbeatGenerator(anomaly_probability=0.05)
reading = generator.generate_single_reading()
batch = generator.generate_batch(batch_size=100)
```

### Validator

```python
from src.validation.validator import HeartbeatValidator

validator = HeartbeatValidator(low_threshold=50, high_threshold=150)
is_valid, enriched, error = validator.validate_and_enrich(reading)
valid_batch, rejected = validator.validate_batch(readings)
```

### Database Handler

```python
from src.database.db_handler import create_db_handler

db = create_db_handler()
db.insert_reading(reading)
db.insert_readings_batch(readings)
latest = db.get_latest_readings(limit=10)
anomalies = db.get_anomalies(limit=50)
```

### Prometheus Metrics

```python
from src.monitoring.metrics import start_metrics_server, READINGS_GENERATED

# Start the /metrics HTTP server on port 8000
start_metrics_server(port=8000)

# Increment a counter
READINGS_GENERATED.labels(customer_id="CUST-001").inc()
```

### Edge-Case Detector

```python
from src.monitoring.edge_cases import EdgeCaseDetector

detector = EdgeCaseDetector()
detector.start()            # Background thread monitors devices
detector.on_reading(reading) # Feed every reading
detector.on_valid()          # Track data quality
detector.on_invalid("bad_format")  # Track rejection
detector.stop()
```

### Dead Letter Queue

```python
from src.monitoring.dlq import DeadLetterQueueProducer, DeadLetterQueueConsumer

# Send a failed message to DLQ
dlq_producer = DeadLetterQueueProducer()
dlq_producer.connect()
dlq_producer.send_to_dlq(
    original_value='{"customer_id": "CUST-001", ...}',
    reason="validation_error: heart_rate out of range",
)

# Iterate DLQ for inspection
dlq_consumer = DeadLetterQueueConsumer()
dlq_consumer.connect()
for envelope in dlq_consumer.iterate(max_messages=10):
    print(envelope["failure_reason"])
```

### Alerting

```python
from src.monitoring.alerting import SlackAlerter, EmailAlerter

slack = SlackAlerter()
slack.send_alert("Test Alert", "Pipeline is healthy", severity="info")

email = EmailAlerter()
email.send_device_failure_email("CUST-001", "2026-02-24T12:00:00Z", 300)
```

---

## Troubleshooting

### Kafka Connection Refused

```bash
# Ensure Docker services are running
docker-compose ps

# Check Kafka logs
docker-compose logs kafka
```

### PostgreSQL Connection Error

```bash
# Check if PostgreSQL is healthy
docker-compose exec postgres pg_isready

# Verify the schema was created
docker-compose exec postgres psql -U heartbeat_user -d heartbeat_db -c "\\dt heartbeat.*"
```

### Consumer Not Receiving Messages

```bash
# Check topic exists and has data
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker-compose exec kafka kafka-console-consumer --topic customer_heartbeat --bootstrap-server localhost:9092 --from-beginning --max-messages 5
```

### Prometheus Not Scraping Metrics

```bash
# Check the pipeline exposes metrics
curl http://localhost:8000/metrics

# Check Prometheus targets page
# Open http://localhost:9090/targets — "heartbeat-pipeline" should be UP

# If running pipeline locally (not in Docker), Prometheus uses host.docker.internal
# Ensure your firewall allows connections on port 8000
```

### DLQ Messages Not Being Reprocessed

```bash
# Check DLQ topic has messages
docker-compose -f docker-compose_kafka.yml exec kafka kafka-console-consumer \
  --topic customer_heartbeat_dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 5

# Run retry mode
python -m src.main --mode dlq-retry
```

### Kafka UI Not Loading

```bash
# Ensure Kafka is healthy first
docker-compose -f docker-compose_kafka.yml logs kafka-ui

# Kafka UI depends on Kafka being healthy — it may take 30-60s after Kafka starts
```

### Reset Everything

```bash
# Stop and remove all volumes
docker-compose -f docker-compose_kafka.yml down -v

# Restart fresh
docker-compose -f docker-compose_kafka.yml up -d
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.

---

## Author

**Emmanuel Kabu**  
AmaliTech gGmbH

---

*Built as a Data Engineering Module Lab project demonstrating real-time data pipeline concepts with production-grade observability.*
