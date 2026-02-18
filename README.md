# 💓 Real-Time Customer Heartbeat Monitoring System

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.x-orange.svg)](https://kafka.apache.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade data pipeline that simulates, streams, validates, and stores real-time heart rate data using **Python**, **Apache Kafka**, and **PostgreSQL**. Includes optional dashboards built with **Streamlit** and **Grafana**.

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
5. **Visualizes** data through Streamlit and Grafana dashboards

### Key Features

- ✅ Realistic synthetic data generation with Gaussian distributions
- ✅ Kafka-based real-time streaming with exactly-once semantics
- ✅ Configurable anomaly detection (LOW < 50 bpm, HIGH > 150 bpm)
- ✅ PostgreSQL with connection pooling and optimized time-series indexes
- ✅ Batch processing for high-throughput database writes
- ✅ Graceful shutdown handling with signal trapping
- ✅ Comprehensive logging with rotation
- ✅ Docker Compose for one-command infrastructure setup
- ✅ Full test suite (unit + integration)
- ✅ Optional Streamlit and Grafana dashboards

---

## 🏗️ Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Synthetic   │    │    Kafka     │    │    Kafka     │    │  PostgreSQL  │
│     Data      │───▶│   Producer   │───▶│   Consumer   │───▶│   Database   │
│   Generator   │    │              │    │  + Validator  │    │              │
└──────────────┘    └──────────────┘    └──────────────┘    └──────┬───────┘
                                                                    │
                                                           ┌───────┴───────┐
                                                           │   Dashboards  │
                                                           │ Streamlit │ Grafana
                                                           └───────────────┘
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
| Dashboard          | Streamlit 1.29 / Grafana 10.2       | Data visualization               |
| Orchestration      | Docker Compose                      | Infrastructure management        |
| Testing            | pytest 7.4                          | Unit and integration tests       |

---

## 📁 Project Structure

```
Heart_beat_monitoring_pipeline/
├── docker-compose.yml          # Infrastructure services definition
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
│   ├── config.py               # Centralized configuration
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
│   └── dashboard/              # Streamlit dashboard
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
│       │   └── heartbeat_dashboard.json
│       └── datasources/
│           └── datasource.yml
│
├── docs/                       # Documentation
│   ├── data_flow_diagram.md    # System diagrams (Mermaid)
│   └── architecture_decisions.md
│
└── logs/                       # Runtime log files (gitignored)
```

---

## 📋 Prerequisites

- **Python 3.10+** (3.11 recommended)
- **Docker** & **Docker Compose** (for infrastructure services)
- **Git** (for version control)

### Optional
- **Make** (for convenience commands via Makefile)
- **Grafana** access at `localhost:3000` (runs in Docker)

---

## 🚀 Quick Start

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
# Start Kafka, Zookeeper, PostgreSQL, and Grafana
docker-compose up -d

# Wait for services to be healthy (~15-30 seconds)
docker-compose ps
```

### 4. Run the Pipeline

```bash
# Full pipeline (producer + consumer)
python -m src.main

# Or use Makefile
make start-pipeline
```

### 5. View the Dashboard (Optional)

```bash
# Streamlit
streamlit run src/dashboard/app.py

# Grafana: open http://localhost:3000 (admin/admin)
```

---

## ⚙️ Configuration

All settings can be configured via environment variables (`.env` file):

| Variable                    | Default              | Description                        |
|-----------------------------|----------------------|------------------------------------|
| `POSTGRES_HOST`             | `localhost`          | PostgreSQL host                    |
| `POSTGRES_PORT`             | `5432`               | PostgreSQL port                    |
| `POSTGRES_DB`               | `heartbeat_db`       | Database name                      |
| `POSTGRES_USER`             | `heartbeat_user`     | Database user                      |
| `POSTGRES_PASSWORD`         | `heartbeat_pass123`  | Database password                  |
| `KAFKA_BOOTSTRAP_SERVERS`   | `localhost:9092`     | Kafka broker addresses             |
| `KAFKA_TOPIC`               | `customer_heartbeat` | Kafka topic name                   |
| `KAFKA_GROUP_ID`            | `heartbeat_consumer_group` | Consumer group ID            |
| `NUM_CUSTOMERS`             | `10`                 | Number of simulated customers      |
| `GENERATION_INTERVAL_SEC`   | `1.0`                | Seconds between data generation    |
| `ANOMALY_LOW_THRESHOLD`     | `50`                 | Low anomaly threshold (bpm)        |
| `ANOMALY_HIGH_THRESHOLD`    | `150`                | High anomaly threshold (bpm)       |
| `LOG_LEVEL`                 | `INFO`               | Logging level                      |
| `DASHBOARD_PORT`            | `8501`               | Streamlit dashboard port           |

---

## ▶️ Running the Pipeline

### Full Pipeline

Runs both producer and consumer in separate threads:

```bash
python -m src.main --mode full
```

### Producer Only

Generates data and sends to Kafka (no database writes):

```bash
python -m src.main --mode producer
```

### Consumer Only

Reads from Kafka and writes to PostgreSQL (data must already be in Kafka):

```bash
python -m src.main --mode consumer
```

### CLI Options

```bash
python -m src.main --help

# Options:
#   --mode {full,producer,consumer}  Pipeline mode (default: full)
#   --log-level {DEBUG,INFO,WARNING,ERROR}  Override log level
```

---

## 📊 Dashboards

### Streamlit Dashboard

```bash
streamlit run src/dashboard/app.py
# Open http://localhost:8501
```

Features:
- Real-time KPI metrics (total readings, average HR, anomaly count)
- Interactive heart rate time-series chart per customer
- Anomaly event log with HIGH/LOW classification
- Customer summary statistics table
- Auto-refreshing display

### Grafana Dashboard

Available at `http://localhost:3000` (login: admin/admin)

Pre-provisioned dashboard includes:
- Total readings counter
- Active customers gauge
- Anomaly count panel
- Heart rate time-series graph
- Recent anomalies table
- Customer summary table

---

## 🧪 Testing

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

## 🗃️ Database Schema

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

## 🔄 Data Flow

1. **Generator** creates `HeartbeatReading` objects with `customer_id`, `heart_rate`, and `timestamp`
2. **Kafka Producer** serializes readings to JSON and sends to `customer_heartbeat` topic (partitioned by `customer_id`)
3. **Kafka Consumer** polls messages in batches of 50
4. **Validator** checks structural integrity and detects anomalies
5. **Database Handler** batch-inserts valid readings with automatic anomaly logging
6. **Dashboards** query PostgreSQL for real-time visualization

---

## 📚 API Reference

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

---

## 🔧 Troubleshooting

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

### Reset Everything

```bash
# Stop and remove all volumes
docker-compose down -v

# Restart fresh
docker-compose up -d
```

---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.

---

## 👤 Author

**Emmanuel Kabu**  
AmaliTech gGmbH

---

*Built as a Data Engineering Module Lab project demonstrating real-time data pipeline concepts.*
