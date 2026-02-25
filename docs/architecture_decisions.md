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

---

## ADR-005: Prometheus for Pipeline Metrics

**Status:** Accepted  
**Date:** 2026-02-20

### Context
We need real-time observability into pipeline throughput, latency, anomaly rates, and component health.

### Decision
Expose 30+ metrics via `prometheus_client` on port 8000 and scrape with a Dockerised Prometheus instance.

### Consequences
- Counters, gauges, and histograms cover generation, consumption, persistence, rejection, latency, DLQ, and device status
- Prometheus alert rules (10 rules) enable proactive anomaly and outage detection
- Minimal overhead — `prometheus_client` is lightweight
- Requires `host.docker.internal` for Prometheus (Docker) → pipeline (host) connectivity on Windows

---

## ADR-006: Dead Letter Queue for Failed Messages

**Status:** Accepted  
**Date:** 2026-02-20

### Context
Deserialization errors, validation rejections, and database insert failures must not silently drop data.

### Decision
Route all failed messages to a dedicated Kafka topic `customer_heartbeat_dlq` wrapped in a JSON envelope containing original value, failure reason, timestamp, retry count, and original offset/partition.

### Consequences
- Failed messages are preserved and inspectable via Kafka UI
- `dlq-retry` CLI mode reprocesses with exponential backoff (max 3 retries)
- After 3 retries, messages are parked and logged for manual review
- DLQ accumulation triggers a Prometheus alert when count > 50

---

## ADR-007: Edge-Case Detection Engine

**Status:** Accepted  
**Date:** 2026-02-20

### Context
Beyond basic anomaly thresholds, we need to detect operational edge cases: silent devices, sudden spikes, sustained anomalies, system outages, and quality degradation.

### Decision
Implement `EdgeCaseDetector` with 5 detectors running in a background thread, processing every reading and periodically scanning all devices.

| Detector | Trigger | Alert |
|----------|---------|-------|
| Heartbeat Delay | No data > 30s | Slack warning |
| Device Failure | No data > 120s | Slack critical + Email |
| Sudden Spike | HR delta > 40 bpm | Slack warning |
| Data Quality | Quality score < 90% | Prometheus alert |
| System Health | Kafka/DB disconnected | Prometheus + Email |

### Consequences
- All thresholds configurable via environment variables
- 5-minute cooldown per device per alert type prevents alert storms
- Sustained anomaly detection (≥ 5 in 10 min) triggers critical email
- Background thread adds minimal CPU overhead

---

## ADR-008: Slack & Email Alerting with Separate Channels

**Status:** Accepted  
**Date:** 2026-02-22

### Context
We need multi-channel alerting: real-time Slack notifications for operational alerts, and email for critical issues and periodic reports.

### Decision
- **Slack**: Three separate webhooks — general alerts, daily reports (`#kafka-daily-report`), weekly reports (`#kafka-weekly-report`)
- **Email**: Gmail SMTP (TLS on port 587) with app password for critical alerts and monthly summaries
- **Alertmanager**: Routes Prometheus alert rules — warnings to Slack, critical to Slack + Email

### Consequences
- Daily and weekly reports go to purpose-specific Slack channels
- Monthly HTML email reports sent on 1st of each month via `EmailAlerter.send_monthly_report()`
- Gmail app password stored in `.env` (gitignored), never committed
- Alertmanager config requires manual webhook/email updates for new environments

---

## ADR-009: Data Quality Engine (Great Expectations + Custom Rules)

**Status:** Accepted  
**Date:** 2026-02-22

### Context
Beyond structural validation, we need deeper data quality checks: uniqueness, temporal validity, type correctness, and trend monitoring.

### Decision
Implement `DataQualityEngine` with two rule layers:
1. **Great Expectations** — column-level expectations (value ranges, not-null, parseable timestamps) with graceful fallback if GE is unavailable
2. **Custom pandas rules** — duplicate detection, future/stale timestamp checks, type validation

Results are stored in `DataQualityStore` (in-memory ring buffer of snapshots) and reported by `DataQualityReporter` on daily/weekly schedules via Slack.

### Consequences
- GE import uses nested try/except fallback to handle version incompatibilities
- Circuit breaker pattern: if quality drops below threshold, batch is rejected entirely
- Row-level failures are routed to DLQ with specific reason codes
- Quality trends (score over time, top failing rules, top failing customers) available via `dq-report` CLI mode
- A dedicated Grafana dashboard visualises quality metrics

---

## ADR-010: Scheduled Reporting (Daily / Weekly / Monthly)

**Status:** Accepted  
**Date:** 2026-02-22

### Context
Stakeholders need automated pipeline health summaries without manually checking dashboards.

### Decision
`PipelineReporter` accumulates counters (readings, anomalies, DLQ, failures, latencies) and sends:
- **Daily** (23:00 UTC) → Slack daily channel
- **Weekly** (Sunday 23:00 UTC) → Slack weekly channel
- **Monthly** (1st of month 23:00 UTC) → HTML email via Gmail SMTP

Counters reset after each report is sent.

### Consequences
- Separate daily/weekly/monthly counter sets avoid cross-contamination
- Monthly email includes HTML template with summary cards and detail table
- Report metrics (`REPORT_READINGS_DAILY`, `_WEEKLY`, `_MONTHLY`) also exposed as Prometheus gauges for Grafana

---

## ADR-011: Windows-Native Makefile Targets

**Status:** Accepted  
**Date:** 2026-02-24

### Context
The development environment is Windows (PowerShell). Standard Unix commands (`grep`, `awk`, `rm -rf`, `open`, `tail -f`) are unavailable.

### Decision
Rewrite all Makefile targets to use PowerShell commands:
- `help` → `Select-String` + `ForEach-Object` instead of `grep`/`awk`
- `clean` → `Get-ChildItem | Remove-Item` instead of `rm -rf`
- `logs` → `Get-Content -Tail -Wait` instead of `tail -f`
- `start` → `Start-Process -WindowStyle Hidden` for background pipeline
- `stop` → `Get-WmiObject Win32_Process` to find and kill by command-line match
- All browser targets → `start "" "URL"` instead of `open`

### Consequences
- Makefile only works on Windows/PowerShell (not cross-platform)
- `make stop` uses `-@` prefix to suppress errors when no process is found
- Signal handler in Kafka consumer guarded with `threading.current_thread() is threading.main_thread()` for thread safety

---

## ADR-012: PostgreSQL Port 5433 to Avoid Local Conflicts

**Status:** Accepted  
**Date:** 2026-02-24

### Context
The development machine runs a local PostgreSQL instance on the default port 5432.

### Decision
Map the Docker PostgreSQL container to host port 5433 (`5433:5432`) to avoid conflicts.

### Consequences
- `.env` and `config.py` default `POSTGRES_PORT` to `5433`
- Existing PostgreSQL tooling (pgAdmin, psql) can connect to both local (5432) and pipeline (5433) databases simultaneously
- New developers must set `POSTGRES_PORT=5433` or use the provided `.env.example`

---

## ADR-013: Grafana Provisioning with Explicit Datasource UIDs

**Status:** Accepted  
**Date:** 2026-02-24

### Context
Grafana dashboard JSON files reference datasources by UID. Auto-generated UIDs change on every volume recreation, breaking dashboard panels.

### Decision
Set explicit `uid` fields in `grafana/provisioning/datasources/datasource.yml`:
- `uid: heartbeat-postgres` for PostgreSQL
- `uid: prometheus` for Prometheus

Dashboard JSON files reference these stable UIDs directly (no `${DS_PROMETHEUS}` template variables).

### Consequences
- Dashboards work immediately after `docker-compose up` without manual datasource configuration
- Three pre-provisioned dashboards: Heartbeat (PostgreSQL), Prometheus Metrics, Data Quality
- Grafana volume must be deleted (`docker-compose down -v`) if UIDs are changed
