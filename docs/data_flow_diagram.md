# Data Flow Diagram

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    REAL-TIME CUSTOMER HEARTBEAT MONITORING SYSTEM               │
│                                                                                 │
│  ┌───────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   Synthetic    │    │    Kafka      │    │    Kafka     │    │  PostgreSQL  │ │
│  │     Data       │───▶│   Producer   │───▶│   Consumer   │───▶│   Database   │ │
│  │   Generator    │    │              │    │              │    │              │ │
│  │               │    │  (Serialize   │    │ (Deserialize │    │ (Persist     │ │
│  │ (Python)      │    │   to JSON)   │    │  + Validate) │    │  readings)   │ │
│  └───────────────┘    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘ │
│                              │                    │                    │         │
│                              ▼                    │                    ▼         │
│                     ┌──────────────┐              │           ┌──────────────┐  │
│                     │  Kafka Topic │              │           │   Grafana    │  │
│                     │  (customer_  │──────────────┘           │  Dashboards  │  │
│                     │  heartbeat)  │                          │ (port 3000)  │  │
│                     └──────────────┘                          └──────────────┘  │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        VALIDATION & QUALITY LAYER                       │    │
│  │  • Structural validation (customer_id, heart_rate, timestamp)          │    │
│  │  • Anomaly detection (LOW < 50 bpm, HIGH > 150 bpm)                   │    │
│  │  • Data enrichment (is_anomaly flag, anomaly_type classification)     │    │
│  │  • Data Quality Engine (Great Expectations + custom pandas rules)      │    │
│  │  • Circuit breaker for sustained quality degradation                   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        MONITORING & ALERTING LAYER                      │    │
│  │  • Prometheus (port 9090) — 30+ metrics scraped from :8000/metrics     │    │
│  │  • Alertmanager (port 9093) — 10 alert rules, Slack + Email routing   │    │
│  │  • Edge-Case Detector — delay, failure, spike, quality, system health  │    │
│  │  • Slack webhooks — daily & weekly reports to separate channels        │    │
│  │  • Email (Gmail SMTP) — critical alerts + monthly summary reports      │    │
│  │  • Dead Letter Queue — failed messages to customer_heartbeat_dlq       │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        INFRASTRUCTURE (Docker Compose)                  │    │
│  │  • Zookeeper (port 2181) — Kafka coordination                          │    │
│  │  • Kafka (port 9092) — Message broker with 3 partitions               │    │
│  │  • PostgreSQL 16 (port 5433) — Persistent storage with indexes        │    │
│  │  • Prometheus (port 9090) — Metrics collection & alert evaluation     │    │
│  │  • Alertmanager (port 9093) — Alert routing (Slack / Email)           │    │
│  │  • Grafana (port 3000) — 3 dashboards (Heartbeat, Prometheus, DQ)     │    │
│  │  • Kafka UI (port 8080) — Topic, consumer & cluster inspection        │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Mermaid Diagram

```mermaid
flowchart LR
    subgraph Generation["Data Generation Layer"]
        A[Synthetic Data Generator<br/>Python Script] --> |HeartbeatReading<br/>customer_id, heart_rate, timestamp|B
    end

    subgraph Streaming["Kafka Streaming Layer"]
        B[Kafka Producer<br/>JSON Serialization] --> C[Kafka Topic<br/>customer_heartbeat<br/>3 Partitions]
        C --> D[Kafka Consumer<br/>JSON Deserialization]
    end

    subgraph Processing["Processing & Validation Layer"]
        D --> E{Data Validator}
        E --> |Valid| F[Anomaly Detector]
        E --> |Invalid| DLQ[Dead Letter Queue<br/>customer_heartbeat_dlq]
        F --> |Normal 60-100 bpm| H[Normal Reading]
        F --> |LOW < 50 bpm| I[Low Anomaly]
        F --> |HIGH > 150 bpm| J[High Anomaly]
        D --> EC[Edge-Case Detector]
        EC --> |Delay/Failure/Spike| ALERT[Alerting Layer]
    end

    subgraph Quality["Data Quality Layer"]
        H --> DQ[Data Quality Engine<br/>GE + Custom Rules]
        I --> DQ
        J --> DQ
        DQ --> |Pass| STORE[Batch Insert]
        DQ --> |Fail| DLQ
    end

    subgraph Storage["Storage Layer"]
        STORE --> K[(PostgreSQL<br/>heartbeat.heart_rate_readings)]
        I --> L[(PostgreSQL<br/>heartbeat.anomaly_log)]
        J --> L
    end

    subgraph Monitoring["Monitoring & Alerting"]
        K --> N[Grafana<br/>3 Dashboards<br/>Port 3000]
        L --> N
        PROM[Prometheus<br/>Port 9090] --> N
        PROM --> AM[Alertmanager<br/>Port 9093]
        AM --> SLACK[Slack Webhooks]
        AM --> EMAIL[Email SMTP]
        ALERT --> SLACK
        ALERT --> EMAIL
    end

    subgraph Reporting["Scheduled Reports"]
        REP[Pipeline Reporter] --> |Daily| SLACK
        REP --> |Weekly| SLACK
        REP --> |Monthly| EMAIL
    end

    style A fill:#4CAF50,color:#fff
    style C fill:#FF9800,color:#fff
    style K fill:#2196F3,color:#fff
    style L fill:#f44336,color:#fff
    style N fill:#9C27B0,color:#fff
    style DLQ fill:#FF5722,color:#fff
    style PROM fill:#E6522C,color:#fff
    style DQ fill:#00BCD4,color:#fff
```

## Data Schema Diagram

```mermaid
erDiagram
    CUSTOMERS ||--o{ HEART_RATE_READINGS : has
    CUSTOMERS ||--o{ ANOMALY_LOG : triggers
    HEART_RATE_READINGS ||--o| ANOMALY_LOG : "logged as"

    CUSTOMERS {
        varchar customer_id PK
        varchar customer_name
        int age
        timestamp created_at
    }

    HEART_RATE_READINGS {
        bigserial id PK
        varchar customer_id FK
        int heart_rate
        timestamptz timestamp
        boolean is_anomaly
        varchar anomaly_type
        timestamptz ingested_at
    }

    ANOMALY_LOG {
        bigserial id PK
        bigint reading_id FK
        varchar customer_id FK
        int heart_rate
        varchar anomaly_type
        timestamptz timestamp
        timestamptz detected_at
    }

    PIPELINE_METRICS {
        bigserial id PK
        varchar metric_name
        double metric_value
        timestamptz recorded_at
    }
```

## Component Interaction Sequence

```mermaid
sequenceDiagram
    participant Gen as Data Generator
    participant Prod as Kafka Producer
    participant Topic as Kafka Topic
    participant Cons as Kafka Consumer
    participant Val as Validator
    participant Edge as Edge-Case Detector
    participant DQ as Data Quality Engine
    participant DB as PostgreSQL
    participant DLQ as Dead Letter Queue
    participant Prom as Prometheus
    participant Rep as Pipeline Reporter
    participant Slack as Slack
    participant Email as Email (SMTP)

    loop Every 1 second
        Gen->>Gen: Generate heartbeat readings<br/>(10 customers)
        Gen->>Prod: HeartbeatReading objects
        Prod->>Topic: JSON serialized messages<br/>(key: customer_id)
        Prod->>Prom: Inc heartbeat_readings_produced_total
    end

    loop Consumer poll loop
        Topic->>Cons: Poll for messages<br/>(batch of 50)
        Cons->>Edge: Process each reading<br/>(delay, spike, failure checks)
        Cons->>Val: Validate batch
        Val-->>Cons: Valid + Rejected readings
        Cons->>DQ: Run quality rules on valid batch
        DQ-->>Cons: Pass / Fail per row
        Cons->>DB: INSERT valid readings (batch)
        Cons->>DLQ: Route invalid/failed rows
        Cons->>Prom: Update 30+ metrics
        Note over DB: Auto-detect anomalies<br/>Log to anomaly_log
    end

    Edge-->>Slack: Device delay / failure / spike alerts
    Edge-->>Email: Critical device failure emails

    loop Scheduled
        Rep->>Slack: Daily report (23:00 UTC)
        Rep->>Slack: Weekly report (Sunday 23:00 UTC)
        Rep->>Email: Monthly report (1st of month)
    end

    Prom->>Prom: Evaluate 10 alert rules
    Prom-->>Slack: Warning alerts via Alertmanager
    Prom-->>Email: Critical alerts via Alertmanager
```

## Dead Letter Queue Flow

```mermaid
flowchart TD
    A[Kafka Consumer] --> B{Validation}
    B --> |Invalid| C[DLQ Producer]
    A --> D{DB Insert}
    D --> |Error| C
    A --> E{Deserialization}
    E --> |Error| C

    C --> F[Kafka Topic<br/>customer_heartbeat_dlq]

    F --> G[DLQ Envelope]
    G --> |original_value| H[Original message JSON]
    G --> |failure_reason| I[Why it failed]
    G --> |retry_count| J[Number of retries]
    G --> |failed_at| K[ISO timestamp]

    L[make dlq-retry] --> M[DLQ Consumer]
    M --> F
    M --> N{Re-validate}
    N --> |Pass| O[Insert to DB]
    N --> |Fail & retries < 3| P[Back to DLQ<br/>retry_count + 1]
    N --> |Fail & retries >= 3| Q[Parked / Logged]

    style C fill:#FF5722,color:#fff
    style F fill:#FF5722,color:#fff
    style O fill:#4CAF50,color:#fff
    style Q fill:#9E9E9E,color:#fff
```

## Data Quality Engine Flow

```mermaid
flowchart LR
    A[Valid Readings Batch] --> B[DataQualityEngine]

    B --> C[Great Expectations Rules]
    C --> C1[heart_rate between 20-250]
    C --> C2[customer_id not null]
    C --> C3[timestamp parseable]

    B --> D[Custom Pandas Rules]
    D --> D1[No duplicate readings]
    D --> D2[No future timestamps]
    D --> D3[No stale timestamps > 1h]
    D --> D4[heart_rate is integer type]

    C --> E{Results}
    D --> E
    E --> |All pass| F[DQResult<br/>overall_pass = true]
    E --> |Failures| G[DQResult<br/>row-level RowFailure list]

    G --> H[Route failures to DLQ]
    F --> I[Proceed to DB insert]

    G --> J[DataQualityStore]
    F --> J
    J --> K[Quality Snapshots & Trends]
    K --> L[DataQualityReporter]
    L --> |Daily| M[Slack DQ Report]
    L --> |Weekly| N[Slack DQ Summary]

    style B fill:#00BCD4,color:#fff
    style H fill:#FF5722,color:#fff
    style I fill:#4CAF50,color:#fff
```
