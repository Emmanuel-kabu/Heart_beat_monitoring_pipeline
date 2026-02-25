"""
Configuration Management Module

Centralizes all configuration using environment variables with sensible defaults.
Uses pydantic-settings for validation and type safety.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env file from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _get_env(key: str, default: str = "") -> str:
    """Get environment variable with fallback to default."""
    return os.getenv(key, default)


def _get_env_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer."""
    return int(os.getenv(key, str(default)))


def _get_env_float(key: str, default: float = 0.0) -> float:
    """Get environment variable as float."""
    return float(os.getenv(key, str(default)))


def _get_env_list(key: str, default: str = "") -> list[str]:
    """Get environment variable as comma-separated list."""
    raw = os.getenv(key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


@dataclass(frozen=True)
class PostgresConfig:
    """PostgreSQL database configuration."""

    host: str = field(default_factory=lambda: _get_env("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: _get_env_int("POSTGRES_PORT", 5433))
    database: str = field(default_factory=lambda: _get_env("POSTGRES_DB", "heartbeat_db"))
    user: str = field(default_factory=lambda: _get_env("POSTGRES_USER", "heartbeat_user"))
    password: str = field(
        default_factory=lambda: _get_env("POSTGRES_PASSWORD", "heartbeat_pass123")
    )

    @property
    def connection_string(self) -> str:
        """Build SQLAlchemy-compatible connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def dsn(self) -> str:
        """Build psycopg2-compatible DSN string."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class KafkaConfig:
    """Apache Kafka configuration."""

    bootstrap_servers: str = field(
        default_factory=lambda: _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(default_factory=lambda: _get_env("KAFKA_TOPIC", "customer_heartbeat"))
    group_id: str = field(
        default_factory=lambda: _get_env("KAFKA_GROUP_ID", "heartbeat_consumer_group")
    )
    auto_offset_reset: str = field(
        default_factory=lambda: _get_env("KAFKA_AUTO_OFFSET_RESET", "earliest")
    )


@dataclass(frozen=True)
class GeneratorConfig:
    """Synthetic data generator configuration."""

    num_customers: int = field(default_factory=lambda: _get_env_int("NUM_CUSTOMERS", 10))
    interval_sec: float = field(
        default_factory=lambda: _get_env_float("GENERATION_INTERVAL_SEC", 1.0)
    )
    heart_rate_min: int = field(default_factory=lambda: _get_env_int("HEART_RATE_MIN", 40))
    heart_rate_max: int = field(default_factory=lambda: _get_env_int("HEART_RATE_MAX", 200))
    normal_hr_min: int = field(default_factory=lambda: _get_env_int("NORMAL_HEART_RATE_MIN", 60))
    normal_hr_max: int = field(default_factory=lambda: _get_env_int("NORMAL_HEART_RATE_MAX", 100))


@dataclass(frozen=True)
class AnomalyConfig:
    """Anomaly detection thresholds."""

    low_threshold: int = field(default_factory=lambda: _get_env_int("ANOMALY_LOW_THRESHOLD", 50))
    high_threshold: int = field(default_factory=lambda: _get_env_int("ANOMALY_HIGH_THRESHOLD", 150))


@dataclass(frozen=True)
class LoggingConfig:
    """Logging configuration."""

    level: str = field(default_factory=lambda: _get_env("LOG_LEVEL", "INFO"))
    log_file: str = field(default_factory=lambda: _get_env("LOG_FILE", "logs/pipeline.log"))


@dataclass(frozen=True)
class DashboardConfig:
    """Dashboard configuration."""

    refresh_interval: int = field(
        default_factory=lambda: _get_env_int("DASHBOARD_REFRESH_INTERVAL", 5)
    )
    port: int = field(default_factory=lambda: _get_env_int("DASHBOARD_PORT", 8501))


@dataclass(frozen=True)
class MonitoringConfig:
    """Prometheus metrics and edge-case detection configuration."""

    prometheus_port: int = field(
        default_factory=lambda: _get_env_int("PROMETHEUS_METRICS_PORT", 8000)
    )
    heartbeat_delay_threshold_sec: float = field(
        default_factory=lambda: _get_env_float("HEARTBEAT_DELAY_THRESHOLD_SEC", 30.0)
    )
    device_failure_threshold_sec: float = field(
        default_factory=lambda: _get_env_float("DEVICE_FAILURE_THRESHOLD_SEC", 120.0)
    )
    spike_delta_threshold: int = field(
        default_factory=lambda: _get_env_int("SPIKE_DELTA_THRESHOLD", 40)
    )
    sustained_anomaly_count: int = field(
        default_factory=lambda: _get_env_int("SUSTAINED_ANOMALY_COUNT", 5)
    )
    sustained_anomaly_window_min: int = field(
        default_factory=lambda: _get_env_int("SUSTAINED_ANOMALY_WINDOW_MIN", 10)
    )
    edge_check_interval_sec: float = field(
        default_factory=lambda: _get_env_float("EDGE_CHECK_INTERVAL_SEC", 15.0)
    )
    dlq_alert_threshold: int = field(
        default_factory=lambda: _get_env_int("DLQ_ALERT_THRESHOLD", 100)
    )


@dataclass(frozen=True)
class AlertingConfig:
    """Slack and Email alerting configuration."""

    slack_webhook_url: str = field(default_factory=lambda: _get_env("SLACK_WEBHOOK_URL", ""))
    slack_daily_webhook_url: str = field(
        default_factory=lambda: _get_env("SLACK_DAILY_WEBHOOK_URL", "")
    )
    slack_daily_channel: str = field(default_factory=lambda: _get_env("SLACK_DAILY_CHANNEL", ""))
    slack_weekly_webhook_url: str = field(
        default_factory=lambda: _get_env("SLACK_WEEKLY_WEBHOOK_URL", "")
    )
    slack_weekly_channel: str = field(default_factory=lambda: _get_env("SLACK_WEEKLY_CHANNEL", ""))
    smtp_host: str = field(default_factory=lambda: _get_env("SMTP_HOST", ""))
    smtp_port: int = field(default_factory=lambda: _get_env_int("SMTP_PORT", 587))
    smtp_user: str = field(default_factory=lambda: _get_env("SMTP_USER", ""))
    smtp_password: str = field(default_factory=lambda: _get_env("SMTP_PASSWORD", ""))
    email_recipients: list = field(
        default_factory=lambda: _get_env_list("ALERT_EMAIL_RECIPIENTS", "")
    )


@dataclass(frozen=True)
class DataQualityConfig:
    """Data quality framework configuration."""

    enabled: bool = field(default_factory=lambda: _get_env("DQ_ENABLED", "true").lower() == "true")
    max_retries: int = field(default_factory=lambda: _get_env_int("DQ_MAX_RETRIES", 3))
    circuit_breaker_threshold: float = field(
        default_factory=lambda: _get_env_float("DQ_CIRCUIT_BREAKER_THRESHOLD", 0.5)
    )
    timeliness_max_age_sec: int = field(
        default_factory=lambda: _get_env_int("DQ_TIMELINESS_MAX_AGE_SEC", 300)
    )
    timeliness_future_tolerance_sec: int = field(
        default_factory=lambda: _get_env_int("DQ_TIMELINESS_FUTURE_TOLERANCE_SEC", 30)
    )
    degradation_email_threshold: float = field(
        default_factory=lambda: _get_env_float("DQ_DEGRADATION_EMAIL_THRESHOLD", 80.0)
    )


@dataclass(frozen=True)
class AppConfig:
    """Root application configuration aggregating all sub-configs."""

    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    alerting: AlertingConfig = field(default_factory=AlertingConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)


# Singleton configuration instance
config = AppConfig()
