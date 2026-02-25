"""
Data Quality Store

Thread‑safe in‑memory store that accumulates DQ validation results
over time, enabling:

* Quality‑trend analysis (is quality improving or degrading?)
* Per‑rule failure frequency tracking (root‑cause identification)
* Per‑customer quality scores (which devices produce bad data?)
* Per‑dimension score history
* Rolling window statistics (1 h / 24 h / 7 d)

The store is intentionally in‑memory.  For long‑term persistence the
``DataQualityReporter`` snapshots the store state into Slack / Email
reports and the Prometheus gauges are scraped continuously.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

from src.logger import get_logger

logger = get_logger("data_quality.store")

# Maximum number of snapshots kept (prevent unbounded growth)
_MAX_SNAPSHOTS = 10_080  # ~7 days at 1‑minute granularity


@dataclass
class QualitySnapshot:
    """Point‑in‑time quality snapshot."""

    timestamp: float          # unix epoch
    batch_size: int
    passed: int
    failed: int
    overall_score: float
    dimension_scores: Dict[str, float]
    rule_failures: Dict[str, int]        # rule_name → count
    customer_failures: Dict[str, int]    # customer_id → count
    engine_used: str
    circuit_breaker_tripped: bool


class DataQualityStore:
    """
    Accumulates DQ results for trend analysis and root‑cause tracking.

    All public methods are thread‑safe.
    """

    def __init__(self, max_snapshots: int = _MAX_SNAPSHOTS):
        self._max_snapshots = max_snapshots
        self._snapshots: Deque[QualitySnapshot] = deque(maxlen=max_snapshots)
        self._lock = threading.Lock()

        # Lifetime aggregates
        self._total_batches = 0
        self._total_rows = 0
        self._total_passed = 0
        self._total_failed = 0

        # Rule failure counters (lifetime)
        self._rule_failures: Dict[str, int] = defaultdict(int)
        # Dimension score accumulators (for rolling average)
        self._dimension_score_sums: Dict[str, float] = defaultdict(float)
        self._dimension_score_counts: Dict[str, int] = defaultdict(int)
        # Customer‑level failure tracking
        self._customer_failures: Dict[str, int] = defaultdict(int)
        # Daily/weekly accumulators (reset by reporter)
        self._daily_batches = 0
        self._daily_rows = 0
        self._daily_passed = 0
        self._daily_failed = 0
        self._daily_scores: List[float] = []
        self._daily_rule_failures: Dict[str, int] = defaultdict(int)
        self._daily_customer_failures: Dict[str, int] = defaultdict(int)

        self._weekly_batches = 0
        self._weekly_rows = 0
        self._weekly_passed = 0
        self._weekly_failed = 0
        self._weekly_scores: List[float] = []
        self._weekly_rule_failures: Dict[str, int] = defaultdict(int)
        self._weekly_customer_failures: Dict[str, int] = defaultdict(int)

    # ───────────────────────────────────────────────────────────────────
    # Record a DQResult
    # ───────────────────────────────────────────────────────────────────

    def record(self, result) -> None:
        """
        Record a ``DQResult`` from the quality engine.

        Args:
            result: A ``DQResult`` dataclass instance.
        """
        from src.data_quality.quality_engine import DQResult, RuleSeverity

        with self._lock:
            now = time.time()
            critical_failures = [
                f for f in result.failed_rows
                if f.severity == RuleSeverity.CRITICAL
            ]
            n_passed = len(result.passed_readings)
            n_failed = len(critical_failures)

            # Per‑rule failures
            rule_fail_counts: Dict[str, int] = defaultdict(int)
            for rr in result.rule_results:
                if not rr.passed:
                    rule_fail_counts[rr.rule_name] += rr.fail_count

            # Per‑customer failures
            customer_fail_counts: Dict[str, int] = defaultdict(int)
            for rf in critical_failures:
                if rf.reading:
                    customer_fail_counts[rf.reading.customer_id] += 1

            snapshot = QualitySnapshot(
                timestamp=now,
                batch_size=result.batch_size,
                passed=n_passed,
                failed=n_failed,
                overall_score=result.overall_score,
                dimension_scores=dict(result.dimension_scores),
                rule_failures=dict(rule_fail_counts),
                customer_failures=dict(customer_fail_counts),
                engine_used=result.engine_used,
                circuit_breaker_tripped=result.circuit_breaker_tripped,
            )
            self._snapshots.append(snapshot)

            # Lifetime
            self._total_batches += 1
            self._total_rows += result.batch_size
            self._total_passed += n_passed
            self._total_failed += n_failed
            for rn, cnt in rule_fail_counts.items():
                self._rule_failures[rn] += cnt
            for cid, cnt in customer_fail_counts.items():
                self._customer_failures[cid] += cnt
            for dim, score in result.dimension_scores.items():
                self._dimension_score_sums[dim] += score
                self._dimension_score_counts[dim] += 1

            # Daily
            self._daily_batches += 1
            self._daily_rows += result.batch_size
            self._daily_passed += n_passed
            self._daily_failed += n_failed
            self._daily_scores.append(result.overall_score)
            for rn, cnt in rule_fail_counts.items():
                self._daily_rule_failures[rn] += cnt
            for cid, cnt in customer_fail_counts.items():
                self._daily_customer_failures[cid] += cnt

            # Weekly
            self._weekly_batches += 1
            self._weekly_rows += result.batch_size
            self._weekly_passed += n_passed
            self._weekly_failed += n_failed
            self._weekly_scores.append(result.overall_score)
            for rn, cnt in rule_fail_counts.items():
                self._weekly_rule_failures[rn] += cnt
            for cid, cnt in customer_fail_counts.items():
                self._weekly_customer_failures[cid] += cnt

    # ───────────────────────────────────────────────────────────────────
    # Queries
    # ───────────────────────────────────────────────────────────────────

    def get_lifetime_summary(self) -> Dict[str, Any]:
        """Return lifetime quality statistics."""
        with self._lock:
            return {
                "total_batches": self._total_batches,
                "total_rows": self._total_rows,
                "total_passed": self._total_passed,
                "total_failed": self._total_failed,
                "pass_rate": (
                    self._total_passed / self._total_rows * 100
                    if self._total_rows > 0
                    else 100.0
                ),
                "avg_dimension_scores": {
                    dim: (
                        self._dimension_score_sums[dim]
                        / self._dimension_score_counts[dim]
                    )
                    if self._dimension_score_counts.get(dim, 0) > 0
                    else 100.0
                    for dim in [
                        "completeness", "validity", "consistency",
                        "timeliness", "uniqueness", "statistical",
                    ]
                },
            }

    def get_top_failing_rules(self, n: int = 10) -> List[Tuple[str, int]]:
        """Return the top N most frequently failing rules (lifetime)."""
        with self._lock:
            sorted_rules = sorted(
                self._rule_failures.items(), key=lambda x: x[1], reverse=True
            )
            return sorted_rules[:n]

    def get_top_failing_customers(self, n: int = 10) -> List[Tuple[str, int]]:
        """Return the top N customers with most quality failures."""
        with self._lock:
            sorted_custs = sorted(
                self._customer_failures.items(), key=lambda x: x[1], reverse=True
            )
            return sorted_custs[:n]

    def get_quality_trend(self, window_minutes: int = 60) -> Dict[str, Any]:
        """
        Compute quality trend over a rolling window.

        Returns:
            Dict with 'current_score', 'previous_score', 'direction'
            ('improving', 'degrading', 'stable'), and 'delta'.
        """
        with self._lock:
            if not self._snapshots:
                return {
                    "current_score": 100.0,
                    "previous_score": 100.0,
                    "direction": "stable",
                    "delta": 0.0,
                }

            now = time.time()
            cutoff = now - (window_minutes * 60)
            half = now - (window_minutes * 30)

            recent_scores = [
                s.overall_score for s in self._snapshots if s.timestamp > half
            ]
            older_scores = [
                s.overall_score for s in self._snapshots
                if cutoff < s.timestamp <= half
            ]

            current = sum(recent_scores) / len(recent_scores) if recent_scores else 100.0
            previous = sum(older_scores) / len(older_scores) if older_scores else current

            delta = current - previous
            if delta > 1:
                direction = "improving"
            elif delta < -1:
                direction = "degrading"
            else:
                direction = "stable"

            return {
                "current_score": round(current, 2),
                "previous_score": round(previous, 2),
                "direction": direction,
                "delta": round(delta, 2),
            }

    def get_recent_circuit_breaker_trips(
        self, window_minutes: int = 60,
    ) -> int:
        """Count circuit breaker trips in the last N minutes."""
        with self._lock:
            cutoff = time.time() - (window_minutes * 60)
            return sum(
                1 for s in self._snapshots
                if s.timestamp > cutoff and s.circuit_breaker_tripped
            )

    # ───────────────────────────────────────────────────────────────────
    # Daily / Weekly report data  (called by reporter, then reset)
    # ───────────────────────────────────────────────────────────────────

    def pop_daily_report_data(self) -> Dict[str, Any]:
        """Retrieve and reset daily accumulators."""
        with self._lock:
            data = {
                "batches": self._daily_batches,
                "rows": self._daily_rows,
                "passed": self._daily_passed,
                "failed": self._daily_failed,
                "avg_score": (
                    sum(self._daily_scores) / len(self._daily_scores)
                    if self._daily_scores
                    else 100.0
                ),
                "top_failing_rules": sorted(
                    self._daily_rule_failures.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )[:5],
                "top_failing_customers": sorted(
                    self._daily_customer_failures.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )[:5],
                "quality_trend": self.get_quality_trend(window_minutes=1440),
            }
            # Reset
            self._daily_batches = 0
            self._daily_rows = 0
            self._daily_passed = 0
            self._daily_failed = 0
            self._daily_scores.clear()
            self._daily_rule_failures.clear()
            self._daily_customer_failures.clear()
            return data

    def pop_weekly_report_data(self) -> Dict[str, Any]:
        """Retrieve and reset weekly accumulators."""
        with self._lock:
            data = {
                "batches": self._weekly_batches,
                "rows": self._weekly_rows,
                "passed": self._weekly_passed,
                "failed": self._weekly_failed,
                "avg_score": (
                    sum(self._weekly_scores) / len(self._weekly_scores)
                    if self._weekly_scores
                    else 100.0
                ),
                "top_failing_rules": sorted(
                    self._weekly_rule_failures.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )[:10],
                "top_failing_customers": sorted(
                    self._weekly_customer_failures.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )[:10],
                "quality_trend": self.get_quality_trend(window_minutes=10080),
            }
            # Reset
            self._weekly_batches = 0
            self._weekly_rows = 0
            self._weekly_passed = 0
            self._weekly_failed = 0
            self._weekly_scores.clear()
            self._weekly_rule_failures.clear()
            self._weekly_customer_failures.clear()
            return data
