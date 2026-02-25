"""
Data Quality Engine

The central orchestrator for data-quality validation. Converts each
batch of ``HeartbeatReading`` objects into a pandas DataFrame, runs
Great Expectations rules **and** custom pandas-based rules, then:

* Routes row-level failures to the Dead Letter Queue (DLQ)
* Marks non-retryable failures as quarantined after ``max_retries``
* Updates Prometheus metrics per rule / dimension
* Stores results in the ``DataQualityStore`` for trend analysis
* Implements a **circuit breaker** – if > ``threshold`` % of a batch
  fails, an alert is fired (but the pipeline is never stalled)

This module is designed to **never crash the pipeline**.  All GE
and pandas operations are wrapped in try/except blocks so that if
the quality engine itself errors out, the batch flows through
unblocked and the failure is logged + alerted.
"""

from __future__ import annotations

import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from src.config import config
from src.data_quality.expectations import (
    QUALITY_RULES,
    QualityDimension,
    QualityRule,
    RuleSeverity,
    build_ge_expectation_suite,
)
from src.logger import get_logger
from src.models import HeartbeatReading

logger = get_logger("data_quality.engine")

# Try importing Great Expectations -  the engine works without it
try:
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite

    # ExpectationConfiguration moved in newer GE versions
    try:
        from great_expectations.core import ExpectationConfiguration
    except ImportError:
        from great_expectations.expectations import ExpectationConfiguration

    GE_AVAILABLE = True
    logger.info("Great Expectations %s loaded - GE validation enabled", gx.__version__)
except Exception:
    GE_AVAILABLE = False
    logger.warning("Great Expectations not available - using pandas-only validation")


# ═══════════════════════════════════════════════════════════════════════════
# Result dataclasses
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class RowFailure:
    """Describes why a single row failed quality checks."""

    row_index: int
    reading: HeartbeatReading
    failed_rules: List[str]
    failed_dimensions: Set[str]
    failure_reasons: List[str]
    severity: RuleSeverity
    retryable: bool


@dataclass
class RuleResultDetail:
    """Outcome of a single rule execution."""

    rule_name: str
    dimension: str
    severity: str
    passed: bool
    total_rows: int
    pass_count: int
    fail_count: int
    failed_indices: List[int] = field(default_factory=list)
    details: str = ""


@dataclass
class DQResult:
    """
    Aggregate result of a batch quality‑validation run.

    Attributes:
        passed_readings: Readings that satisfied all critical rules.
        failed_rows: Per‑row failure records (routed to DLQ).
        rule_results: Per‑rule pass/fail details.
        dimension_scores: Score (0‑100) per quality dimension.
        overall_score: Weighted overall score.
        batch_size: Original batch size.
        validation_time_sec: Wall‑clock time for the validation.
        circuit_breaker_tripped: True if failure rate exceeded threshold.
        engine_used: 'great_expectations' or 'pandas_fallback'.
    """

    passed_readings: List[HeartbeatReading] = field(default_factory=list)
    failed_rows: List[RowFailure] = field(default_factory=list)
    rule_results: List[RuleResultDetail] = field(default_factory=list)
    dimension_scores: Dict[str, float] = field(default_factory=dict)
    overall_score: float = 100.0
    batch_size: int = 0
    validation_time_sec: float = 0.0
    circuit_breaker_tripped: bool = False
    engine_used: str = "pandas_fallback"


# ═══════════════════════════════════════════════════════════════════════════
# Engine
# ═══════════════════════════════════════════════════════════════════════════


class DataQualityEngine:
    """
    Comprehensive data‑quality validation engine.

    Usage::

        engine = DataQualityEngine(dlq_producer=dlq)
        result = engine.validate_batch(readings)
        # result.passed_readings → persist to DB
        # result.failed_rows     → already routed to DLQ
    """

    def __init__(
        self,
        dlq_producer=None,
        quality_store=None,
        max_retries: int | None = None,
        circuit_breaker_threshold: float | None = None,
        timeliness_max_age_sec: int | None = None,
        timeliness_future_tolerance_sec: int | None = None,
        low_threshold: int | None = None,
        high_threshold: int | None = None,
    ):
        self._dlq_producer = dlq_producer
        self._quality_store = quality_store
        self._max_retries = max_retries or getattr(config, "data_quality", None) and config.data_quality.max_retries or 3
        self._circuit_breaker_threshold = (
            circuit_breaker_threshold
            or getattr(config, "data_quality", None) and config.data_quality.circuit_breaker_threshold
            or 0.5
        )
        self._timeliness_max_age = (
            timeliness_max_age_sec
            or getattr(config, "data_quality", None) and config.data_quality.timeliness_max_age_sec
            or 300
        )
        self._timeliness_future_tol = (
            timeliness_future_tolerance_sec
            or getattr(config, "data_quality", None) and config.data_quality.timeliness_future_tolerance_sec
            or 30
        )
        self._low_threshold = low_threshold or config.anomaly.low_threshold
        self._high_threshold = high_threshold or config.anomaly.high_threshold

        # GE context (lazily initialized)
        self._ge_context = None
        self._ge_suite = None

        # Lifetime counters for metrics
        self._total_validated = 0
        self._total_passed = 0
        self._total_failed = 0
        self._total_quarantined = 0

        logger.info(
            "DataQualityEngine initialized (max_retries=%d, circuit_breaker=%.0f%%, "
            "timeliness_max_age=%ds, GE=%s)",
            self._max_retries,
            self._circuit_breaker_threshold * 100,
            self._timeliness_max_age,
            "available" if GE_AVAILABLE else "unavailable",
        )

    # ───────────────────────────────────────────────────────────────────
    # Public API
    # ───────────────────────────────────────────────────────────────────

    def validate_batch(
        self,
        readings: List[HeartbeatReading],
    ) -> DQResult:
        """
        Validate a batch of heartbeat readings against all quality rules.

        Tries Great Expectations first, falls back to pure pandas.
        Row failures are automatically routed to DLQ.

        Args:
            readings: Batch of HeartbeatReading objects.

        Returns:
            DQResult with passed readings, failures, scores, and details.
        """
        if not readings:
            return DQResult()

        t0 = time.time()
        df = self._readings_to_dataframe(readings)
        batch_size = len(df)

        # ── Run all rules ───────────────────────────────────────────────
        try:
            if GE_AVAILABLE:
                rule_results = self._run_ge_validation(df)
                engine_used = "great_expectations"
            else:
                rule_results = []
                engine_used = "pandas_fallback"
        except Exception as exc:
            logger.error("GE validation failed, falling back to pandas: %s", exc)
            rule_results = []
            engine_used = "pandas_fallback"

        # Always run custom pandas rules (consistency, timeliness, etc.)
        custom_results = self._run_custom_rules(df)
        rule_results.extend(custom_results)

        # ── Aggregate per-row failures ──────────────────────────────────
        row_failures_map: Dict[int, RowFailure] = {}
        for rr in rule_results:
            if not rr.passed and rr.failed_indices:
                rule_obj = self._rule_by_name(rr.rule_name)
                if rule_obj is None:
                    continue
                for idx in rr.failed_indices:
                    if idx not in row_failures_map:
                        reading = readings[idx] if idx < len(readings) else None
                        row_failures_map[idx] = RowFailure(
                            row_index=idx,
                            reading=reading,
                            failed_rules=[],
                            failed_dimensions=set(),
                            failure_reasons=[],
                            severity=RuleSeverity.INFO,
                            retryable=True,
                        )
                    rf = row_failures_map[idx]
                    rf.failed_rules.append(rr.rule_name)
                    rf.failed_dimensions.add(rr.dimension)
                    rf.failure_reasons.append(f"{rr.rule_name}: {rr.details or 'failed'}")
                    # Escalate severity
                    if rule_obj.severity == RuleSeverity.CRITICAL:
                        rf.severity = RuleSeverity.CRITICAL
                    elif rule_obj.severity == RuleSeverity.WARNING and rf.severity != RuleSeverity.CRITICAL:
                        rf.severity = RuleSeverity.WARNING
                    # Non‑retryable if ANY failing rule is non‑retryable
                    if not rule_obj.retryable:
                        rf.retryable = False

        # Separate critical failures (DLQ) from warnings (metric only)
        dlq_failures: List[RowFailure] = []
        warning_failures: List[RowFailure] = []
        for rf in row_failures_map.values():
            if rf.severity == RuleSeverity.CRITICAL:
                dlq_failures.append(rf)
            else:
                warning_failures.append(rf)

        # ── Determine passed rows ───────────────────────────────────────
        critical_failed_indices = {rf.row_index for rf in dlq_failures}
        passed_readings = [
            r for i, r in enumerate(readings) if i not in critical_failed_indices
        ]

        # ── Dimension scores ────────────────────────────────────────────
        dimension_scores = self._compute_dimension_scores(rule_results, batch_size)
        overall_score = self._compute_overall_score(dimension_scores)

        # ── Circuit breaker ─────────────────────────────────────────────
        fail_rate = len(dlq_failures) / batch_size if batch_size > 0 else 0
        circuit_tripped = fail_rate >= self._circuit_breaker_threshold

        elapsed = time.time() - t0

        result = DQResult(
            passed_readings=passed_readings,
            failed_rows=dlq_failures + warning_failures,
            rule_results=rule_results,
            dimension_scores=dimension_scores,
            overall_score=overall_score,
            batch_size=batch_size,
            validation_time_sec=elapsed,
            circuit_breaker_tripped=circuit_tripped,
            engine_used=engine_used,
        )

        # ── Side effects: DLQ, metrics, store ───────────────────────────
        self._route_to_dlq(dlq_failures)
        self._update_metrics(result)
        if self._quality_store:
            self._quality_store.record(result)

        # Lifetime counters
        self._total_validated += batch_size
        self._total_passed += len(passed_readings)
        self._total_failed += len(dlq_failures)

        # Logging
        logger.info(
            "DQ batch validated: size=%d passed=%d failed=%d warnings=%d "
            "score=%.1f%% time=%.3fs engine=%s%s",
            batch_size,
            len(passed_readings),
            len(dlq_failures),
            len(warning_failures),
            overall_score,
            elapsed,
            engine_used,
            " [CIRCUIT BREAKER]" if circuit_tripped else "",
        )

        return result

    # ───────────────────────────────────────────────────────────────────
    # Great Expectations validation path
    # ───────────────────────────────────────────────────────────────────

    def _run_ge_validation(self, df: pd.DataFrame) -> List[RuleResultDetail]:
        """Run GE expectations and translate results."""
        results: List[RuleResultDetail] = []

        try:
            context = self._get_ge_context()

            # Create a validator from the DataFrame
            datasource = context.sources.add_or_update_pandas(name="heartbeat_dq")
            asset = datasource.add_dataframe_asset(name="readings_asset")
            batch_request = asset.build_batch_request(dataframe=df)

            suite = build_ge_expectation_suite("heartbeat_quality_suite")
            if suite is None:
                logger.warning("Could not build GE suite; skipping GE validation")
                return results

            context.add_or_update_expectation_suite(expectation_suite=suite)
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name="heartbeat_quality_suite",
            )

            # Run validation with COMPLETE format to get per-row failures
            validation_result = validator.validate(
                result_format={
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": [],
                    "include_unexpected_rows": True,
                },
            )

            # Parse GE results
            for exp_result in validation_result.results:
                exp_config = exp_result.expectation_config
                meta = exp_config.meta or {}
                rule_name = meta.get("dq_rule_name", exp_config.expectation_type)
                dimension = meta.get("dq_dimension", "unknown")
                severity = meta.get("dq_severity", "warning")

                result_detail = exp_result.result or {}
                element_count = result_detail.get("element_count", len(df))
                unexpected_count = result_detail.get("unexpected_count", 0)
                pass_count = element_count - unexpected_count
                failed_indices = result_detail.get("unexpected_index_list", []) or []

                results.append(
                    RuleResultDetail(
                        rule_name=rule_name,
                        dimension=dimension,
                        severity=severity,
                        passed=exp_result.success,
                        total_rows=element_count,
                        pass_count=pass_count,
                        fail_count=unexpected_count,
                        failed_indices=list(failed_indices),
                        details=self._ge_detail_string(exp_result),
                    )
                )

        except Exception as exc:
            logger.error("GE validation error: %s\n%s", exc, traceback.format_exc())

        return results

    def _get_ge_context(self):
        """Lazily create an ephemeral GE DataContext."""
        if self._ge_context is None:
            self._ge_context = gx.get_context()
        return self._ge_context

    @staticmethod
    def _ge_detail_string(exp_result) -> str:
        """Build a human‑readable detail string from a GE result."""
        try:
            result = exp_result.result or {}
            parts = []
            if "unexpected_percent" in result:
                parts.append(f"unexpected={result['unexpected_percent']:.1f}%")
            if "observed_value" in result:
                parts.append(f"observed={result['observed_value']}")
            if "partial_unexpected_list" in result:
                sample = result["partial_unexpected_list"][:5]
                parts.append(f"sample={sample}")
            return "; ".join(parts) if parts else ""
        except Exception:
            return ""

    # ───────────────────────────────────────────────────────────────────
    # Custom pandas rules (consistency, timeliness, etc.)
    # ───────────────────────────────────────────────────────────────────

    def _run_custom_rules(self, df: pd.DataFrame) -> List[RuleResultDetail]:
        """Run rules that need cross‑column or temporal logic."""
        results: List[RuleResultDetail] = []
        n = len(df)
        if n == 0:
            return results

        # ── Consistency: anomaly_flag_type_agreement ────────────────────
        mask_bad_flag = (df["is_anomaly"] == True) & (df["anomaly_type"].isna())  # noqa: E712
        results.append(self._mask_to_result(
            mask_bad_flag, n, "anomaly_flag_type_agreement",
            QualityDimension.CONSISTENCY, RuleSeverity.WARNING,
            "is_anomaly=True but anomaly_type is null",
        ))

        # ── Consistency: normal_flag_type_agreement ─────────────────────
        mask_bad_normal = (df["is_anomaly"] == False) & (df["anomaly_type"].notna())  # noqa: E712
        results.append(self._mask_to_result(
            mask_bad_normal, n, "normal_flag_type_agreement",
            QualityDimension.CONSISTENCY, RuleSeverity.WARNING,
            "is_anomaly=False but anomaly_type is set",
        ))

        # ── Consistency: threshold consistency ──────────────────────────
        mask_low_wrong = (
            (df["heart_rate"] < self._low_threshold)
            & (df["is_anomaly"] == True)  # noqa: E712
            & (df["anomaly_type"] != "LOW")
        )
        mask_high_wrong = (
            (df["heart_rate"] > self._high_threshold)
            & (df["is_anomaly"] == True)  # noqa: E712
            & (df["anomaly_type"] != "HIGH")
        )
        mask_threshold = mask_low_wrong | mask_high_wrong
        results.append(self._mask_to_result(
            mask_threshold, n, "anomaly_type_threshold_consistency",
            QualityDimension.CONSISTENCY, RuleSeverity.WARNING,
            "anomaly_type does not match threshold direction",
        ))

        # ── Timeliness: not future ──────────────────────────────────────
        now_ts = datetime.now(timezone.utc)
        future_tolerance_sec = self._timeliness_future_tol

        def _is_future(ts_str):
            try:
                ts = datetime.fromisoformat(str(ts_str))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return (ts - now_ts).total_seconds() > future_tolerance_sec
            except Exception:
                return False

        mask_future = df["timestamp"].apply(_is_future)
        results.append(self._mask_to_result(
            mask_future, n, "timestamp_not_future",
            QualityDimension.TIMELINESS, RuleSeverity.CRITICAL,
            f"timestamp is more than {future_tolerance_sec}s in the future",
        ))

        # ── Timeliness: not stale ───────────────────────────────────────
        max_age_sec = self._timeliness_max_age

        def _is_stale(ts_str):
            try:
                ts = datetime.fromisoformat(str(ts_str))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return (now_ts - ts).total_seconds() > max_age_sec
            except Exception:
                return False

        mask_stale = df["timestamp"].apply(_is_stale)
        results.append(self._mask_to_result(
            mask_stale, n, "timestamp_not_stale",
            QualityDimension.TIMELINESS, RuleSeverity.WARNING,
            f"timestamp is older than {max_age_sec}s",
        ))

        # ── Statistical: anomaly rate bounded ───────────────────────────
        if n > 0:
            anomaly_rate = (df["is_anomaly"].sum() / n) * 100
            threshold = 30.0
            results.append(RuleResultDetail(
                rule_name="anomaly_rate_bounded",
                dimension=QualityDimension.STATISTICAL.value,
                severity=RuleSeverity.WARNING.value,
                passed=anomaly_rate <= threshold,
                total_rows=n,
                pass_count=n if anomaly_rate <= threshold else 0,
                fail_count=0 if anomaly_rate <= threshold else n,
                failed_indices=[],
                details=f"anomaly_rate={anomaly_rate:.1f}% (threshold={threshold}%)",
            ))

        # ── Pandas‑only fallback for GE row rules when GE unavailable ──
        if not GE_AVAILABLE:
            results.extend(self._pandas_fallback_row_rules(df))

        return results

    def _pandas_fallback_row_rules(self, df: pd.DataFrame) -> List[RuleResultDetail]:
        """Pure pandas implementations of the GE row‑level rules."""
        results: List[RuleResultDetail] = []
        n = len(df)
        if n == 0:
            return results

        # customer_id not null
        mask = df["customer_id"].isna() | (df["customer_id"] == "")
        results.append(self._mask_to_result(
            mask, n, "customer_id_not_null",
            QualityDimension.COMPLETENESS, RuleSeverity.CRITICAL,
            "customer_id is null or empty",
        ))

        # heart_rate not null
        mask = df["heart_rate"].isna()
        results.append(self._mask_to_result(
            mask, n, "heart_rate_not_null",
            QualityDimension.COMPLETENESS, RuleSeverity.CRITICAL,
            "heart_rate is null",
        ))

        # timestamp not null
        mask = df["timestamp"].isna() | (df["timestamp"] == "")
        results.append(self._mask_to_result(
            mask, n, "timestamp_not_null",
            QualityDimension.COMPLETENESS, RuleSeverity.CRITICAL,
            "timestamp is null or empty",
        ))

        # customer_id format
        mask = ~df["customer_id"].astype(str).str.match(r"^CUST-\d{3}$", na=False)
        # Only flag rows where customer_id is not null
        mask = mask & df["customer_id"].notna()
        results.append(self._mask_to_result(
            mask, n, "customer_id_format",
            QualityDimension.VALIDITY, RuleSeverity.CRITICAL,
            "customer_id does not match CUST-NNN pattern",
        ))

        # heart_rate in range
        mask = (df["heart_rate"] < 20) | (df["heart_rate"] > 300)
        mask = mask & df["heart_rate"].notna()
        results.append(self._mask_to_result(
            mask, n, "heart_rate_in_range",
            QualityDimension.VALIDITY, RuleSeverity.CRITICAL,
            "heart_rate outside 20–300 range",
        ))

        # anomaly_type valid set
        valid_types = {None, "HIGH", "LOW"}
        mask = ~df["anomaly_type"].isin(valid_types) & df["anomaly_type"].notna()
        results.append(self._mask_to_result(
            mask, n, "anomaly_type_valid_set",
            QualityDimension.VALIDITY, RuleSeverity.WARNING,
            "anomaly_type not in {null, HIGH, LOW}",
        ))

        # is_anomaly boolean
        mask = ~df["is_anomaly"].isin([True, False])
        results.append(self._mask_to_result(
            mask, n, "is_anomaly_is_boolean",
            QualityDimension.VALIDITY, RuleSeverity.WARNING,
            "is_anomaly is not boolean",
        ))

        # timestamp_parseable
        def _parseable(ts):
            try:
                datetime.fromisoformat(str(ts))
                return True
            except Exception:
                return False

        mask = ~df["timestamp"].apply(_parseable)
        mask = mask & df["timestamp"].notna()
        results.append(self._mask_to_result(
            mask, n, "timestamp_parseable",
            QualityDimension.VALIDITY, RuleSeverity.CRITICAL,
            "timestamp is not a valid ISO 8601 string",
        ))

        # no exact duplicates
        dup_mask = df.duplicated(subset=["customer_id", "timestamp", "heart_rate"], keep="first")
        results.append(self._mask_to_result(
            dup_mask, n, "no_exact_duplicates",
            QualityDimension.UNIQUENESS, RuleSeverity.WARNING,
            "duplicate (customer_id, timestamp, heart_rate) row",
        ))

        # Batch-level: mean
        mean_hr = df["heart_rate"].mean()
        results.append(RuleResultDetail(
            rule_name="heart_rate_mean_in_range",
            dimension=QualityDimension.STATISTICAL.value,
            severity=RuleSeverity.WARNING.value,
            passed=40 <= mean_hr <= 160 if pd.notna(mean_hr) else True,
            total_rows=n,
            pass_count=n,
            fail_count=0,
            details=f"mean={mean_hr:.1f}" if pd.notna(mean_hr) else "no data",
        ))

        # Batch-level: stdev
        std_hr = df["heart_rate"].std()
        results.append(RuleResultDetail(
            rule_name="heart_rate_stdev_bounded",
            dimension=QualityDimension.STATISTICAL.value,
            severity=RuleSeverity.WARNING.value,
            passed=std_hr <= 60 if pd.notna(std_hr) else True,
            total_rows=n,
            pass_count=n,
            fail_count=0,
            details=f"stdev={std_hr:.1f}" if pd.notna(std_hr) else "no data",
        ))

        return results

    # ───────────────────────────────────────────────────────────────────
    # DLQ integration
    # ───────────────────────────────────────────────────────────────────

    def _route_to_dlq(self, failures: List[RowFailure]) -> None:
        """Route critical row failures to the Dead Letter Queue."""
        if not self._dlq_producer or not failures:
            return

        for rf in failures:
            if rf.reading is None:
                continue

            reason = f"dq_validation: {', '.join(rf.failed_rules)}"
            extra = {
                "failed_rules": rf.failed_rules,
                "failed_dimensions": list(rf.failed_dimensions),
                "failure_reasons": rf.failure_reasons,
                "retryable": rf.retryable,
                "dq_severity": rf.severity.value,
            }

            try:
                self._dlq_producer.send_to_dlq(
                    original_value=rf.reading.to_json(),
                    reason=reason,
                    extra_metadata=extra,
                )
            except Exception as exc:
                logger.error(
                    "Failed to route DQ failure to DLQ (row %d): %s",
                    rf.row_index,
                    exc,
                )

    # ───────────────────────────────────────────────────────────────────
    # Prometheus metrics
    # ───────────────────────────────────────────────────────────────────

    def _update_metrics(self, result: DQResult) -> None:
        """Push validation results into Prometheus gauges/counters."""
        try:
            from src.monitoring.metrics import (
                DQ_BATCH_DURATION,
                DQ_CIRCUIT_BREAKER_TRIPS,
                DQ_COMPLETENESS_SCORE,
                DQ_CONSISTENCY_SCORE,
                DQ_OVERALL_SCORE,
                DQ_QUARANTINED_TOTAL,
                DQ_ROWS_FAILED,
                DQ_ROWS_PASSED,
                DQ_RULE_FAILURES,
                DQ_TIMELINESS_SCORE,
                DQ_VALIDATIONS_TOTAL,
                DQ_VALIDITY_SCORE,
            )

            # Overall
            DQ_VALIDATIONS_TOTAL.labels(result="pass").inc(len(result.passed_readings))
            DQ_VALIDATIONS_TOTAL.labels(result="fail").inc(
                len([f for f in result.failed_rows if f.severity == RuleSeverity.CRITICAL])
            )
            DQ_ROWS_PASSED.inc(len(result.passed_readings))

            # Per dimension
            for rf in result.failed_rows:
                if rf.severity == RuleSeverity.CRITICAL:
                    for dim in rf.failed_dimensions:
                        DQ_ROWS_FAILED.labels(dimension=dim).inc()
                if not rf.retryable:
                    DQ_QUARANTINED_TOTAL.inc()

            # Per rule
            for rr in result.rule_results:
                if not rr.passed:
                    DQ_RULE_FAILURES.labels(
                        rule_name=rr.rule_name, dimension=rr.dimension
                    ).inc(rr.fail_count)

            # Dimension scores
            scores = result.dimension_scores
            DQ_COMPLETENESS_SCORE.set(scores.get("completeness", 100))
            DQ_VALIDITY_SCORE.set(scores.get("validity", 100))
            DQ_CONSISTENCY_SCORE.set(scores.get("consistency", 100))
            DQ_TIMELINESS_SCORE.set(scores.get("timeliness", 100))
            DQ_OVERALL_SCORE.set(result.overall_score)

            # Duration
            DQ_BATCH_DURATION.observe(result.validation_time_sec)

            # Circuit breaker
            if result.circuit_breaker_tripped:
                DQ_CIRCUIT_BREAKER_TRIPS.inc()

        except ImportError:
            pass  # metrics module not loaded
        except Exception as exc:
            logger.error("Failed to update DQ metrics: %s", exc)

    # ───────────────────────────────────────────────────────────────────
    # Scoring helpers
    # ───────────────────────────────────────────────────────────────────

    def _compute_dimension_scores(
        self,
        rule_results: List[RuleResultDetail],
        batch_size: int,
    ) -> Dict[str, float]:
        """Compute a 0–100 score per quality dimension."""
        dim_pass: Dict[str, int] = defaultdict(int)
        dim_total: Dict[str, int] = defaultdict(int)

        for rr in rule_results:
            dim = rr.dimension
            if rr.total_rows > 0 and rr.row_level if hasattr(rr, 'row_level') else True:
                dim_pass[dim] += rr.pass_count
                dim_total[dim] += rr.total_rows
            elif not rr.passed:
                # Batch‑level rule failed
                dim_pass[dim] += 0
                dim_total[dim] += 1
            else:
                dim_pass[dim] += 1
                dim_total[dim] += 1

        scores = {}
        for dim in QualityDimension:
            total = dim_total.get(dim.value, 0)
            passed = dim_pass.get(dim.value, 0)
            scores[dim.value] = (passed / total * 100) if total > 0 else 100.0

        return scores

    @staticmethod
    def _compute_overall_score(dimension_scores: Dict[str, float]) -> float:
        """Weighted average of dimension scores."""
        weights = {
            "completeness": 0.25,
            "validity": 0.25,
            "consistency": 0.15,
            "timeliness": 0.15,
            "uniqueness": 0.10,
            "statistical": 0.10,
        }
        total_weight = 0
        total_score = 0
        for dim, weight in weights.items():
            if dim in dimension_scores:
                total_score += dimension_scores[dim] * weight
                total_weight += weight
        return (total_score / total_weight) if total_weight > 0 else 100.0

    # ───────────────────────────────────────────────────────────────────
    # Utilities
    # ───────────────────────────────────────────────────────────────────

    @staticmethod
    def _readings_to_dataframe(readings: List[HeartbeatReading]) -> pd.DataFrame:
        """Convert a list of HeartbeatReading to a pandas DataFrame."""
        records = []
        for r in readings:
            records.append({
                "customer_id": r.customer_id,
                "heart_rate": r.heart_rate,
                "timestamp": r.timestamp,
                "is_anomaly": r.is_anomaly,
                "anomaly_type": r.anomaly_type,
            })
        return pd.DataFrame(records)

    @staticmethod
    def _mask_to_result(
        mask: pd.Series,
        total: int,
        rule_name: str,
        dimension: QualityDimension,
        severity: RuleSeverity,
        detail: str,
    ) -> RuleResultDetail:
        """Convert a boolean mask to a RuleResultDetail."""
        failed_idx = list(mask[mask].index)
        fail_count = len(failed_idx)
        return RuleResultDetail(
            rule_name=rule_name,
            dimension=dimension.value,
            severity=severity.value,
            passed=fail_count == 0,
            total_rows=total,
            pass_count=total - fail_count,
            fail_count=fail_count,
            failed_indices=failed_idx,
            details=detail if fail_count > 0 else "",
        )

    @staticmethod
    def _rule_by_name(name: str) -> Optional[QualityRule]:
        """Look up a QualityRule by name."""
        for r in QUALITY_RULES:
            if r.name == name:
                return r
        return None

    @property
    def stats(self) -> Dict[str, Any]:
        """Return lifetime quality statistics."""
        return {
            "total_validated": self._total_validated,
            "total_passed": self._total_passed,
            "total_failed": self._total_failed,
            "total_quarantined": self._total_quarantined,
            "pass_rate": (
                self._total_passed / self._total_validated * 100
                if self._total_validated > 0
                else 100.0
            ),
            "ge_available": GE_AVAILABLE,
        }
