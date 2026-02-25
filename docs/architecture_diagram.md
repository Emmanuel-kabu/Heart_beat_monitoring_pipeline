# Architecture Diagram

## System Overview

The **Real-Time Customer Heartbeat Monitoring System** is a production-grade data pipeline built with Python, Apache Kafka, PostgreSQL, Prometheus, and Grafana. It ingests simulated heartbeat sensor data, validates and enriches it in real time, persists it to a relational store, and provides full observability through metrics, alerts, and dashboards.

---

## High-Level Architecture

```mermaid
flowchart TB
    subgraph DataGen["1 - Data Generation"]
        GEN["HeartbeatGenerator\n10 customers\n1 reading/sec each"]
    end

    subgraph Kafka["2 - Streaming Layer (Apache Kafka)"]
        PROD["HeartbeatProducer\nJSON serialisation\nKey = customer_id"]
        TOPIC["customer_heartbeat\n3 partitions"]
        DLQ_TOPIC["customer_heartbeat_dlq\nDead Letter Queue"]
        PROD -->|produce| TOPIC
    end

    subgraph Processing["3 - Processing Layer"]
        CONS["HeartbeatConsumer\nbatch size = 50\nmanual offset commit"]
        DQ["DataQualityEngine\nGreat Expectations + pandas\ncompleteness / validity /\nconsistency / timeliness"]
        VAL["HeartbeatValidator\nschema + anomaly detection\nLOW &lt; 50 bpm | HIGH &gt; 150 bpm"]
        EDGE["EdgeCaseDetector\nheartbeat delay | device failure\nspike detection | sustained anomaly\nsystem health"]
    end

    subgraph Storage["4 - Storage Layer"]
        PG[("PostgreSQL 16\nheartbeat schema\nheart_rate_readings\nanomaly_log")]
    end

    subgraph Monitoring["5 - Observability"]
        PROM["Prometheus\nscrape :8000/metrics\n50+ metrics"]
        GRAFANA["Grafana\n3 dashboards\nPostgres + Prometheus"]
        ALERTMGR["Alertmanager\n10 alert rules\nSlack + Email routing"]
        KAFKAUI["Kafka UI\ntopic / consumer inspection"]
    end

    subgraph Alerting["6 - Alerting & Reporting"]
        SLACK["Slack\ndaily & weekly reports\nspike / failure alerts"]
        EMAIL["Email (Gmail SMTP)\nmonthly summary report\ncritical device alerts"]
        REPORTER["PipelineReporter\ndaily @ 23:00 UTC\nweekly on Sundays\nmonthly on 1st"]
    end

    GEN --> PROD
    TOPIC --> CONS
    CONS --> DQ
    DQ -->|passed| VAL
    DQ -->|quarantined| DLQ_TOPIC
    VAL -->|valid| PG
    VAL -->|rejected| DLQ_TOPIC
    CONS --> EDGE
    EDGE -->|alerts| SLACK
    EDGE -->|critical| EMAIL
    CONS -.->|metrics| PROM
    PROM --> GRAFANA
    PROM --> ALERTMGR
    ALERTMGR --> SLACK
    PG --> GRAFANA
    REPORTER --> SLACK
    REPORTER --> EMAIL

    style DataGen fill:#4CAF50,color:#fff,stroke:#388E3C
    style Kafka fill:#FF9800,color:#fff,stroke:#F57C00
    style Processing fill:#2196F3,color:#fff,stroke:#1976D2
    style Storage fill:#3F51B5,color:#fff,stroke:#303F9F
    style Monitoring fill:#9C27B0,color:#fff,stroke:#7B1FA2
    style Alerting fill:#F44336,color:#fff,stroke:#D32F2F
```

---

## Component Architecture

```mermaid
flowchart LR
    subgraph src["src/"]
        direction TB
        MAIN["main.py\nHeartbeatPipeline\norchestrates all components"]
        CONFIG["config.py\nAppConfig (dataclasses)\nenv-var driven"]
        MODELS["models.py\nHeartbeatReading\nCustomerProfile"]
        LOGGER["logger.py\nrotating file + console\nUTF-8 safe"]

        subgraph generator["generator/"]
            HG["heartbeat_generator.py\nHeartbeatGenerator\nGaussian distribution"]
        end

        subgraph kafka_client["kafka_client/"]
            KP["producer.py\nHeartbeatProducer\nidempotent, acks=all"]
            KC["consumer.py\nHeartbeatConsumer\nbatch processing"]
        end

        subgraph validation["validation/"]
            VV["validator.py\nHeartbeatValidator\nschema + range checks"]
        end

        subgraph database["database/"]
            DB["db_handler.py\nDatabaseHandler\npsycopg2, batch insert"]
        end

        subgraph data_quality["data_quality/"]
            EXP["expectations.py\nQualityRule definitions\n5 quality dimensions"]
            QE["quality_engine.py\nDataQualityEngine\nGE + pandas fallback"]
            QS["quality_store.py\nDataQualityStore\nin-memory snapshots"]
            QR["quality_reporter.py\nDataQualityReporter\nperiodic score reports"]
        end

        subgraph monitoring["monitoring/"]
            MET["metrics.py\n50+ Prometheus metrics\ncounters, gauges, histograms"]
            ALERT["alerting.py\nSlackAlerter\nEmailAlerter"]
            REP["reporting.py\nPipelineReporter\ndaily / weekly / monthly"]
            DLQ["dlq.py\nDLQ Producer & Consumer\nretry with backoff"]
            EDGEC["edge_cases.py\nEdgeCaseDetector\n5 detection strategies"]
        end
    end

    MAIN --> CONFIG
    MAIN --> HG
    MAIN --> KP
    MAIN --> KC
    MAIN --> VV
    MAIN --> DB
    MAIN --> QE
    MAIN --> QR
    MAIN --> ALERT
    MAIN --> REP
    MAIN --> DLQ
    MAIN --> EDGEC
    KC --> VV
    KC --> QE
    KC --> DB
    KC --> DLQ
    QE --> EXP
    QE --> QS
    QR --> QS
    REP --> ALERT
    EDGEC --> ALERT

    style src fill:#263238,color:#ECEFF1,stroke:#37474F
    style generator fill:#4CAF50,color:#fff,stroke:#388E3C
    style kafka_client fill:#FF9800,color:#fff,stroke:#F57C00
    style validation fill:#00BCD4,color:#fff,stroke:#0097A7
    style database fill:#3F51B5,color:#fff,stroke:#303F9F
    style data_quality fill:#8BC34A,color:#000,stroke:#689F38
    style monitoring fill:#9C27B0,color:#fff,stroke:#7B1FA2
```

---

## Infrastructure (Docker Compose)

```mermaid
flowchart TB
    subgraph docker["Docker Compose Network"]
        ZK["Zookeeper\nPort 2181\nKafka coordination"]
        KF["Kafka Broker\nPort 9092 / 29092\n3 partitions"]
        PG["PostgreSQL 16\nPort 5433 → 5432\nheartbeat schema"]
        PR["Prometheus\nPort 9090\nscrapes :8000"]
        GR["Grafana\nPort 3000\n3 provisioned dashboards"]
        AM["Alertmanager\nPort 9093\nSlack + Email routing"]
        KUI["Kafka UI\nPort 8080\ncluster inspection"]

        ZK --> KF
        KF -.->|scraped by| PR
        PG -.->|datasource| GR
        PR -->|datasource| GR
        PR -->|fires alerts| AM
    end

    subgraph host["Host Machine"]
        PIPE["Python Pipeline\nPort 8000 /metrics\nProducer + Consumer threads"]
    end

    PIPE -->|produce / consume| KF
    PIPE -->|INSERT readings| PG
    PIPE -->|expose metrics| PR

    style docker fill:#1565C0,color:#fff,stroke:#0D47A1
    style host fill:#2E7D32,color:#fff,stroke:#1B5E20
```

---

## Data Flow Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Gen as HeartbeatGenerator
    participant Prod as KafkaProducer
    participant Topic as Kafka Topic
    participant Cons as KafkaConsumer
    participant DQ as DataQualityEngine
    participant Val as HeartbeatValidator
    participant Edge as EdgeCaseDetector
    participant DB as PostgreSQL
    participant DLQ as Dead Letter Queue
    participant Prom as Prometheus
    participant Reporter as PipelineReporter
    participant Slack as Slack
    participant Email as Email

    loop Every 1 second
        Gen->>Prod: Generate batch (10 readings)
        Prod->>Topic: Produce JSON messages (key=customer_id)
    end

    loop Poll loop
        Topic->>Cons: Poll batch (up to 50 messages)
        Cons->>Edge: Feed each reading to EdgeCaseDetector
        Cons->>DQ: Validate batch (GE + pandas rules)
        alt DQ fails row
            DQ->>DLQ: Route quarantined rows
        end
        DQ-->>Cons: Passed readings
        Cons->>Val: Schema + anomaly validation
        alt Validation fails
            Val->>DLQ: Route rejected readings
        end
        Val-->>Cons: Valid readings
        Cons->>DB: Batch INSERT into heartbeat schema
        Cons->>Cons: Manual offset COMMIT
        Cons->>Prom: Update counters & gauges
        Cons->>Reporter: Record stats (reading / anomaly / latency)
    end

    Note over Reporter: Daily @ 23:00 UTC
    Reporter->>Slack: Daily summary report

    Note over Reporter: Weekly on Sun @ 23:00 UTC
    Reporter->>Slack: Weekly summary report

    Note over Reporter: Monthly on 1st @ 23:00 UTC
    Reporter->>Email: Monthly HTML report

    Note over Edge: Continuous monitoring thread
    Edge->>Slack: Spike / delay / failure alerts
    Edge->>Email: Critical device failure emails
```

---

## Data Quality Dimensions

```mermaid
flowchart LR
    subgraph DQ["Data Quality Engine"]
        direction TB
        C["Completeness\nnon-null customer_id\nnon-null heart_rate\nnon-null timestamp"]
        V["Validity\nheart_rate 1-300 bpm\ncustomer_id format CUST-XXX"]
        CON["Consistency\nheart_rate is integer\ntimestamp is ISO-8601"]
        T["Timeliness\nnot older than 5 min\nnot in the future"]
    end

    BATCH["Incoming Batch\n50 readings"] --> DQ
    DQ -->|all pass| PASS["Passed Readings\n→ Validator → DB"]
    DQ -->|any fail| FAIL["Failed Rows\n→ DLQ + metrics"]
    DQ -->|failure rate ≥ 50%| CB["Circuit Breaker\ntripped warning"]

    style DQ fill:#8BC34A,color:#000,stroke:#689F38
    style PASS fill:#4CAF50,color:#fff
    style FAIL fill:#F44336,color:#fff
    style CB fill:#FF9800,color:#fff
```

---

## Alerting & Reporting Architecture

```mermaid
flowchart TB
    subgraph Sources["Alert Sources"]
        EDGE2["EdgeCaseDetector"]
        DQ2["DataQualityEngine"]
        PROM2["Prometheus Alert Rules"]
        REPORTER2["PipelineReporter"]
    end

    subgraph Channels["Notification Channels"]
        SD["Slack (Daily)\n#kafka-daily-report"]
        SW["Slack (Weekly)\n#kafka-weekly-report"]
        SA["Slack (Alerts)\ndefault webhook"]
        EM["Email (Monthly)\nGmail SMTP"]
        EC["Email (Critical)\ndevice failure\nsustained anomalies"]
    end

    EDGE2 -->|spike, delay, failure| SA
    EDGE2 -->|critical failure| EC
    DQ2 -->|degradation alert| SA
    DQ2 -->|score < threshold| EC
    PROM2 -->|via Alertmanager| SA
    PROM2 -->|critical rules| EC
    REPORTER2 -->|daily summary| SD
    REPORTER2 -->|weekly summary| SW
    REPORTER2 -->|monthly report| EM

    style Sources fill:#1565C0,color:#fff,stroke:#0D47A1
    style Channels fill:#C62828,color:#fff,stroke:#B71C1C
```

---

## Monitoring Stack

| Component      | Port  | Purpose                                    |
|----------------|-------|--------------------------------------------|
| Prometheus     | 9090  | Scrapes pipeline metrics every 10s         |
| Grafana        | 3000  | 3 dashboards (heartbeat, prometheus, DQ)   |
| Alertmanager   | 9093  | Routes alerts to Slack & Email             |
| Kafka UI       | 8080  | Topic, consumer group, cluster inspection   |
| Pipeline `/metrics` | 8000 | 50+ Prometheus counters, gauges, histograms |

---

## Grafana Dashboards

| Dashboard                          | Datasource | Key Panels                                                   |
|------------------------------------|------------|--------------------------------------------------------------|
| Customer Heartbeat Monitor         | PostgreSQL | Total readings, active customers, anomalies, heart rate trend |
| Heartbeat Pipeline - Prometheus    | Prometheus | Produced/consumed/persisted, latency p50/p95/p99, DLQ, uptime |
| Heartbeat Pipeline - Data Quality  | Prometheus | Overall DQ score, dimension scores, circuit breaker, failures |

---

## Technology Stack

```mermaid
mindmap
  root((Heartbeat Pipeline))
    Streaming
      Apache Kafka 7.5
      Zookeeper
      confluent-kafka Python
    Storage
      PostgreSQL 16
      psycopg2
      heartbeat schema
    Monitoring
      Prometheus 2.48
      Grafana 10.2
      Alertmanager 0.26
      50+ custom metrics
    Alerting
      Slack Webhooks
      Gmail SMTP
      Daily / Weekly / Monthly
    Data Quality
      Great Expectations
      pandas fallback
      4 quality dimensions
      Circuit breaker
    Infrastructure
      Docker Compose
      Makefile automation
      .env configuration
    Testing
      pytest
      Unit + Integration
      Coverage reports
```
