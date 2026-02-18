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


@dataclass(frozen=True)
class PostgresConfig:
    """PostgreSQL database configuration."""
    host: str = field(default_factory=lambda: _get_env("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: _get_env_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _get_env("POSTGRES_DB", "heartbeat_db"))
    user: str = field(default_factory=lambda: _get_env("POSTGRES_USER", "heartbeat_user"))
    password: str = field(default_factory=lambda: _get_env("POSTGRES_PASSWORD", "heartbeat_pass123"))

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
    normal_hr_min: int = field(
        default_factory=lambda: _get_env_int("NORMAL_HEART_RATE_MIN", 60)
    )
    normal_hr_max: int = field(
        default_factory=lambda: _get_env_int("NORMAL_HEART_RATE_MAX", 100)
    )


@dataclass(frozen=True)
class AnomalyConfig:
    """Anomaly detection thresholds."""
    low_threshold: int = field(
        default_factory=lambda: _get_env_int("ANOMALY_LOW_THRESHOLD", 50)
    )
    high_threshold: int = field(
        default_factory=lambda: _get_env_int("ANOMALY_HIGH_THRESHOLD", 150)
    )


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
class AppConfig:
    """Root application configuration aggregating all sub-configs."""
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)


# Singleton configuration instance
config = AppConfig()
