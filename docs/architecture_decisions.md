# Architecture Decision Records

## ADR-001: Use Confluent Kafka Python Client

**Status:** Accepted  
**Date:** 2026-02-18

### Context
We need a Python library to interact with Apache Kafka for producing and consuming heartbeat messages.

### Decision
Use `confluent-kafka` over `kafka-python` for:
- Better performance (C-based librdkafka wrapper)
- More features (idempotent producer, exactly-once semantics)
- Active maintenance by Confluent
- Better documentation

### Consequences
- Requires `librdkafka` C library (handled by pip pre-built wheels)
- Slightly different API than `kafka-python`

---

## ADR-002: PostgreSQL with Schema Separation

**Status:** Accepted  
**Date:** 2026-02-18

### Context
We need a structured storage layer for time-series heartbeat data.

### Decision
Use PostgreSQL with a dedicated `heartbeat` schema for:
- Namespacing to avoid conflicts with default `public` schema
- Efficient time-series querying with proper indexing
- SQL functions for server-side anomaly detection
- Views for common query patterns

### Consequences
- Schema must be created before application can run
- All queries must reference `heartbeat.` schema prefix

---

## ADR-003: Manual Kafka Offset Commits

**Status:** Accepted  
**Date:** 2026-02-18

### Context
We need to ensure no data loss between Kafka consumption and database writes.

### Decision
Disable auto-commit and manually commit offsets after successful database insertion for at-least-once delivery guarantee.

### Consequences
- Slightly higher latency due to synchronous commits
- Possible duplicate processing on consumer restart (handled by DB constraints)
- More complex consumer code but better data reliability

---

## ADR-004: Connection Pooling for PostgreSQL

**Status:** Accepted  
**Date:** 2026-02-18

### Context
The consumer needs to efficiently write to PostgreSQL under load.

### Decision
Use `psycopg2.pool.ThreadedConnectionPool` for connection management.

### Consequences
- Efficient connection reuse
- Thread-safe for concurrent access
- Pool size limits prevent database connection exhaustion
