"""
Data Quality Rule Definitions

Defines 20+ data quality rules across 6 quality dimensions using
Great Expectations expectation types. Rules are declared as portable
dataclass objects so the engine can run them via GE *or* fall back to
pure‑pandas validation when GE is unavailable.

Dimensions:
    1. Completeness   – No missing required fields
    2. Validity        – Values within allowed domain / type / format
    3. Consistency     – Cross‑field business‑rule coherence
    4. Timeliness      – Timestamps within acceptable recency window
    5. Uniqueness      – No duplicate records in a batch
    6. Statistical     – Batch‑level distribution sanity‑checks
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ═══════════════════════════════════════════════════════════════════════════
# Enums
# ═══════════════════════════════════════════════════════════════════════════


class QualityDimension(str, enum.Enum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    STATISTICAL = "statistical"


class RuleSeverity(str, enum.Enum):
    """Determines whether a row failure triggers DLQ or just a warning."""
    CRITICAL = "critical"       # Row goes to DLQ
    WARNING = "warning"         # Metric + log only
    INFO = "info"               # Metric only


# ═══════════════════════════════════════════════════════════════════════════
# Rule dataclass
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class QualityRule:
    """
    A single data‑quality rule.

    Attributes:
        name: Unique machine-readable rule identifier.
        description: Human‑readable explanation.
        dimension: Quality dimension this rule belongs to.
        severity: How bad a failure is.
        ge_expectation_type: Great Expectations expectation class name.
        ge_kwargs: Keyword arguments forwarded to the GE expectation.
        retryable: Whether a failed row should be retried via DLQ or
            quarantined immediately (e.g. timeliness never improves).
        row_level: True → failures are per‑row (have unexpected indices).
            False → the rule is batch‑level (e.g. column mean).
        enabled: Toggle for runtime disablement.
    """

    name: str
    description: str
    dimension: QualityDimension
    severity: RuleSeverity
    ge_expectation_type: str
    ge_kwargs: Dict[str, Any] = field(default_factory=dict)
    retryable: bool = True
    row_level: bool = True
    enabled: bool = True


# ═══════════════════════════════════════════════════════════════════════════
# Rule definitions – 20 rules across 6 dimensions
# ═══════════════════════════════════════════════════════════════════════════

QUALITY_RULES: List[QualityRule] = [
    # ── 1. Completeness ─────────────────────────────────────────────────
    QualityRule(
        name="customer_id_not_null",
        description="customer_id must not be null or empty",
        dimension=QualityDimension.COMPLETENESS,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_not_be_null",
        ge_kwargs={"column": "customer_id"},
        retryable=False,
    ),
    QualityRule(
        name="heart_rate_not_null",
        description="heart_rate must not be null",
        dimension=QualityDimension.COMPLETENESS,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_not_be_null",
        ge_kwargs={"column": "heart_rate"},
        retryable=False,
    ),
    QualityRule(
        name="timestamp_not_null",
        description="timestamp must not be null or empty",
        dimension=QualityDimension.COMPLETENESS,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_not_be_null",
        ge_kwargs={"column": "timestamp"},
        retryable=False,
    ),
    QualityRule(
        name="required_columns_exist",
        description="All required columns must be present in the dataset",
        dimension=QualityDimension.COMPLETENESS,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_table_columns_to_match_set",
        ge_kwargs={
            "column_set": [
                "customer_id",
                "heart_rate",
                "timestamp",
                "is_anomaly",
                "anomaly_type",
            ],
            "exact_match": False,
        },
        retryable=False,
        row_level=False,
    ),

    # ── 2. Validity ─────────────────────────────────────────────────────
    QualityRule(
        name="customer_id_format",
        description="customer_id must match pattern CUST-NNN",
        dimension=QualityDimension.VALIDITY,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_match_regex",
        ge_kwargs={"column": "customer_id", "regex": r"^CUST-\d{3}$"},
        retryable=False,
    ),
    QualityRule(
        name="heart_rate_in_range",
        description="heart_rate must be between 20 and 300 bpm (physiological limits)",
        dimension=QualityDimension.VALIDITY,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_be_between",
        ge_kwargs={"column": "heart_rate", "min_value": 20, "max_value": 300},
        retryable=False,
    ),
    QualityRule(
        name="heart_rate_is_integer",
        description="heart_rate must be an integer (or integer‑valued float)",
        dimension=QualityDimension.VALIDITY,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_be_of_type",
        ge_kwargs={"column": "heart_rate", "type_": "int"},
        retryable=False,
    ),
    QualityRule(
        name="anomaly_type_valid_set",
        description="anomaly_type must be null, 'HIGH', or 'LOW'",
        dimension=QualityDimension.VALIDITY,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="expect_column_values_to_be_in_set",
        ge_kwargs={
            "column": "anomaly_type",
            "value_set": [None, "HIGH", "LOW"],
        },
        retryable=False,
    ),
    QualityRule(
        name="is_anomaly_is_boolean",
        description="is_anomaly must be a boolean value",
        dimension=QualityDimension.VALIDITY,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="expect_column_values_to_be_in_set",
        ge_kwargs={"column": "is_anomaly", "value_set": [True, False]},
        retryable=False,
    ),
    QualityRule(
        name="timestamp_parseable",
        description="timestamp must be a valid ISO 8601 date‑time string",
        dimension=QualityDimension.VALIDITY,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="expect_column_values_to_match_regex",
        ge_kwargs={
            "column": "timestamp",
            "regex": (
                r"^\d{4}-\d{2}-\d{2}"    # date
                r"[T ]\d{2}:\d{2}:\d{2}"  # time
            ),
        },
        retryable=False,
    ),

    # ── 3. Consistency ──────────────────────────────────────────────────
    QualityRule(
        name="anomaly_flag_type_agreement",
        description="When is_anomaly=True, anomaly_type must not be null",
        dimension=QualityDimension.CONSISTENCY,
        severity=RuleSeverity.WARNING,
        # Handled by custom pandas check — GE doesn't have cross‑column
        # conditional expectation out of the box. We use a placeholder.
        ge_expectation_type="_custom_anomaly_flag_agreement",
        ge_kwargs={},
        retryable=True,
    ),
    QualityRule(
        name="normal_flag_type_agreement",
        description="When is_anomaly=False, anomaly_type must be null",
        dimension=QualityDimension.CONSISTENCY,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="_custom_normal_flag_agreement",
        ge_kwargs={},
        retryable=True,
    ),
    QualityRule(
        name="anomaly_type_threshold_consistency",
        description="heart_rate < low_threshold with is_anomaly=True should have anomaly_type=LOW",
        dimension=QualityDimension.CONSISTENCY,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="_custom_threshold_consistency",
        ge_kwargs={},
        retryable=True,
    ),

    # ── 4. Timeliness ───────────────────────────────────────────────────
    QualityRule(
        name="timestamp_not_future",
        description="timestamp must not be more than 30 s in the future",
        dimension=QualityDimension.TIMELINESS,
        severity=RuleSeverity.CRITICAL,
        ge_expectation_type="_custom_timestamp_not_future",
        ge_kwargs={},
        retryable=False,   # time won't fix itself
    ),
    QualityRule(
        name="timestamp_not_stale",
        description="timestamp must not be older than the configured staleness window",
        dimension=QualityDimension.TIMELINESS,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="_custom_timestamp_not_stale",
        ge_kwargs={},
        retryable=False,
    ),

    # ── 5. Uniqueness ──────────────────────────────────────────────────
    QualityRule(
        name="no_exact_duplicates",
        description="No duplicate (customer_id, timestamp, heart_rate) tuples in a batch",
        dimension=QualityDimension.UNIQUENESS,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="expect_compound_columns_to_be_unique",
        ge_kwargs={
            "column_list": ["customer_id", "timestamp", "heart_rate"],
        },
        retryable=False,
    ),

    # ── 6. Statistical (batch‑level) ───────────────────────────────────
    QualityRule(
        name="heart_rate_mean_in_range",
        description="Batch mean heart rate should be between 40 and 160 bpm",
        dimension=QualityDimension.STATISTICAL,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="expect_column_mean_to_be_between",
        ge_kwargs={"column": "heart_rate", "min_value": 40, "max_value": 160},
        retryable=False,
        row_level=False,
    ),
    QualityRule(
        name="heart_rate_stdev_bounded",
        description="Batch heart‑rate standard deviation should not exceed 60",
        dimension=QualityDimension.STATISTICAL,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="expect_column_stdev_to_be_between",
        ge_kwargs={"column": "heart_rate", "min_value": 0, "max_value": 60},
        retryable=False,
        row_level=False,
    ),
    QualityRule(
        name="anomaly_rate_bounded",
        description="Batch anomaly rate should not exceed 30 %",
        dimension=QualityDimension.STATISTICAL,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="_custom_anomaly_rate_bounded",
        ge_kwargs={"threshold": 30.0},
        retryable=False,
        row_level=False,
    ),
    QualityRule(
        name="heart_rate_no_excessive_nulls",
        description="heart_rate should have <5 % null values in a batch",
        dimension=QualityDimension.STATISTICAL,
        severity=RuleSeverity.WARNING,
        ge_expectation_type="expect_column_values_to_not_be_null",
        ge_kwargs={"column": "heart_rate", "mostly": 0.95},
        retryable=False,
        row_level=False,
    ),
]


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════


def get_rules_by_dimension(
    dimension: QualityDimension,
) -> List[QualityRule]:
    """Return only rules for the given dimension."""
    return [r for r in QUALITY_RULES if r.dimension == dimension and r.enabled]


def get_critical_rules() -> List[QualityRule]:
    """Return only rules with CRITICAL severity."""
    return [r for r in QUALITY_RULES if r.severity == RuleSeverity.CRITICAL and r.enabled]


def get_row_level_rules() -> List[QualityRule]:
    """Return only per-row rules (those that identify individual failing rows)."""
    return [r for r in QUALITY_RULES if r.row_level and r.enabled]


def get_batch_level_rules() -> List[QualityRule]:
    """Return only batch-level rules."""
    return [r for r in QUALITY_RULES if not r.row_level and r.enabled]


def build_ge_expectation_suite(
    suite_name: str = "heartbeat_quality_suite",
) -> Optional[Any]:
    """
    Build a Great Expectations ``ExpectationSuite`` from ``QUALITY_RULES``.

    Skips custom rules (those starting with ``_custom_``) because they
    require the pandas fallback path.

    Returns:
        An ``ExpectationSuite`` instance, or None if GE is unavailable.
    """
    try:
        from great_expectations.core import ExpectationConfiguration
        from great_expectations.core.expectation_suite import ExpectationSuite

        suite = ExpectationSuite(expectation_suite_name=suite_name)
        for rule in QUALITY_RULES:
            if not rule.enabled:
                continue
            if rule.ge_expectation_type.startswith("_custom_"):
                continue  # handled by pandas fallback

            kwargs = dict(rule.ge_kwargs)

            ec = ExpectationConfiguration(
                expectation_type=rule.ge_expectation_type,
                kwargs=kwargs,
                meta={
                    "dq_rule_name": rule.name,
                    "dq_dimension": rule.dimension.value,
                    "dq_severity": rule.severity.value,
                },
            )
            suite.add_expectation(ec)
        return suite
    except ImportError:
        return None
    except Exception:
        return None
