"""
Data Quality Framework

Comprehensive data quality validation using Great Expectations,
integrated with DLQ, Prometheus metrics, Grafana dashboards,
and Slack/Email reporting.

Modules:
    expectations  – Declarative rule definitions (20+ rules across 6 dimensions)
    quality_engine – Validation engine (GE + pandas fallback)
    quality_store  – In-memory results store with trend tracking
    quality_reporter – Daily/weekly DQ reports via Slack & Email
"""

from src.data_quality.expectations import (
    QUALITY_RULES,
    QualityDimension,
    QualityRule,
    RuleSeverity,
    build_ge_expectation_suite,
)
from src.data_quality.quality_engine import DataQualityEngine, DQResult, RowFailure
from src.data_quality.quality_reporter import DataQualityReporter
from src.data_quality.quality_store import DataQualityStore

__all__ = [
    "QUALITY_RULES",
    "QualityDimension",
    "QualityRule",
    "RuleSeverity",
    "build_ge_expectation_suite",
    "DataQualityEngine",
    "DQResult",
    "RowFailure",
    "DataQualityStore",
    "DataQualityReporter",
]
