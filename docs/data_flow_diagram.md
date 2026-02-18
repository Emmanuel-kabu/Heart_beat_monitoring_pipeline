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
│                     │  Kafka Topic │              │           │   Grafana /  │  │
│                     │  (customer_  │──────────────┘           │   Streamlit  │  │
│                     │  heartbeat)  │                          │  Dashboard   │  │
│                     └──────────────┘                          └──────────────┘  │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        VALIDATION LAYER                                 │    │
│  │  • Structural validation (customer_id, heart_rate, timestamp)          │    │
│  │  • Anomaly detection (LOW < 50 bpm, HIGH > 150 bpm)                   │    │
│  │  • Data enrichment (is_anomaly flag, anomaly_type classification)     │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        INFRASTRUCTURE (Docker Compose)                  │    │
│  │  • Zookeeper (port 2181) — Kafka coordination                          │    │
│  │  • Kafka (port 9092) — Message broker with 3 partitions               │    │
│  │  • PostgreSQL 16 (port 5432) — Persistent storage with indexes        │    │
│  │  • Grafana (port 3000) — Optional monitoring dashboard                │    │
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

    subgraph Processing["Processing Layer"]
        D --> E{Data Validator}
        E --> |Valid| F[Anomaly Detector]
        E --> |Invalid| G[Rejected<br/>Logged as Warning]
        F --> |Normal 60-100 bpm| H[Normal Reading]
        F --> |LOW < 50 bpm| I[Low Anomaly]
        F --> |HIGH > 150 bpm| J[High Anomaly]
    end

    subgraph Storage["Storage Layer"]
        H --> K[(PostgreSQL<br/>heartbeat.heart_rate_readings)]
        I --> K
        J --> K
        I --> L[(PostgreSQL<br/>heartbeat.anomaly_log)]
        J --> L
    end

    subgraph Visualization["Visualization Layer"]
        K --> M[Streamlit Dashboard<br/>Port 8501]
        K --> N[Grafana Dashboard<br/>Port 3000]
        L --> M
        L --> N
    end

    style A fill:#4CAF50,color:#fff
    style C fill:#FF9800,color:#fff
    style K fill:#2196F3,color:#fff
    style L fill:#f44336,color:#fff
    style M fill:#9C27B0,color:#fff
    style N fill:#9C27B0,color:#fff
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
    participant DB as PostgreSQL

    loop Every 1 second
        Gen->>Gen: Generate heartbeat readings<br/>(10 customers)
        Gen->>Prod: HeartbeatReading objects
        Prod->>Topic: JSON serialized messages<br/>(key: customer_id)
        Topic->>Cons: Poll for messages<br/>(batch of 50)
        Cons->>Val: Validate batch
        Val-->>Cons: Valid + Rejected readings
        Cons->>DB: INSERT valid readings
        Note over DB: Auto-detect anomalies<br/>Log to anomaly_log
    end
```
