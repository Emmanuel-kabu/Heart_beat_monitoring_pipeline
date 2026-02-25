"""
Scheduled Reporting Module

Generates and sends daily, weekly, and monthly reports via Slack / Email.
Tracks cumulative counters and resets them on schedule boundaries.
Runs in a background thread alongside the main pipeline.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from src.logger import get_logger
from src.monitoring.alerting import EmailAlerter, SlackAlerter
from src.monitoring.metrics import (
    REPORT_ANOMALIES_DAILY,
    REPORT_ANOMALIES_MONTHLY,
    REPORT_ANOMALIES_WEEKLY,
    REPORT_READINGS_DAILY,
    REPORT_READINGS_MONTHLY,
    REPORT_READINGS_WEEKLY,
)

logger = get_logger("monitoring.reporting")


class PipelineReporter:
    """
    Accumulates pipeline statistics and pushes daily / weekly
    summaries to Slack.  Prometheus gauges are also updated so
    Grafana can visualise reporting metrics.
    """

    def __init__(
        self,
        slack: SlackAlerter | None = None,
        slack_daily: SlackAlerter | None = None,
        slack_weekly: SlackAlerter | None = None,
        email_monthly: EmailAlerter | None = None,
        daily_hour_utc: int = 23,
        weekly_day: int = 6,          # 0=Mon ... 6=Sun
        monthly_day: int = 1,         # Day of month to send monthly report
        check_interval_sec: float = 60,
    ):
        """
        Args:
            slack: Default SlackAlerter instance (used for alerts).
            slack_daily: SlackAlerter for daily reports (falls back to slack).
            slack_weekly: SlackAlerter for weekly reports (falls back to slack).
            email_monthly: EmailAlerter for monthly reports (None = disabled).
            daily_hour_utc: Hour (UTC) at which to send the daily report.
            weekly_day: ISO weekday (0=Mon) on which to send the weekly report.
            monthly_day: Day of month on which to send the monthly report.
            check_interval_sec: How often to check if a report is due.
        """
        self._slack = slack or SlackAlerter()
        self._slack_daily = slack_daily or self._slack
        self._slack_weekly = slack_weekly or self._slack
        self._email_monthly = email_monthly
        self._daily_hour = daily_hour_utc
        self._weekly_day = weekly_day
        self._monthly_day = monthly_day
        self._check_interval = check_interval_sec

        # Counters (reset daily)
        self._daily_readings = 0
        self._daily_anomalies = 0
        self._daily_dlq = 0
        self._daily_failures = 0
        self._daily_latencies: list[float] = []
        self._daily_valid = 0
        self._daily_invalid = 0

        # Counters (reset weekly)
        self._weekly_readings = 0
        self._weekly_anomalies = 0
        self._weekly_dlq = 0
        self._weekly_failures = 0
        self._weekly_latencies: list[float] = []
        self._weekly_valid = 0
        self._weekly_invalid = 0

        # Counters (reset monthly)
        self._monthly_readings = 0
        self._monthly_anomalies = 0
        self._monthly_dlq = 0
        self._monthly_failures = 0
        self._monthly_latencies: list[float] = []
        self._monthly_valid = 0
        self._monthly_invalid = 0

        # Schedule tracking
        self._last_daily_date: Optional[str] = None
        self._last_weekly_week: Optional[int] = None
        self._last_monthly_month: Optional[str] = None

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # ═══════════════════════════════════════════════════════════════════════
    # Increment API — called from the pipeline
    # ═══════════════════════════════════════════════════════════════════════

    def record_reading(self) -> None:
        with self._lock:
            self._daily_readings += 1
            self._weekly_readings += 1
            self._monthly_readings += 1
            REPORT_READINGS_DAILY.set(self._daily_readings)
            REPORT_READINGS_WEEKLY.set(self._weekly_readings)
            REPORT_READINGS_MONTHLY.set(self._monthly_readings)

    def record_anomaly(self) -> None:
        with self._lock:
            self._daily_anomalies += 1
            self._weekly_anomalies += 1
            self._monthly_anomalies += 1
            REPORT_ANOMALIES_DAILY.set(self._daily_anomalies)
            REPORT_ANOMALIES_WEEKLY.set(self._weekly_anomalies)
            REPORT_ANOMALIES_MONTHLY.set(self._monthly_anomalies)

    def record_dlq(self) -> None:
        with self._lock:
            self._daily_dlq += 1
            self._weekly_dlq += 1
            self._monthly_dlq += 1

    def record_device_failure(self) -> None:
        with self._lock:
            self._daily_failures += 1
            self._weekly_failures += 1
            self._monthly_failures += 1

    def record_latency(self, latency_ms: float) -> None:
        with self._lock:
            self._daily_latencies.append(latency_ms)
            self._weekly_latencies.append(latency_ms)
            self._monthly_latencies.append(latency_ms)

    def record_quality(self, valid: bool) -> None:
        with self._lock:
            if valid:
                self._daily_valid += 1
                self._weekly_valid += 1
                self._monthly_valid += 1
            else:
                self._daily_invalid += 1
                self._weekly_invalid += 1
                self._monthly_invalid += 1

    # ═══════════════════════════════════════════════════════════════════════
    # Lifecycle
    # ═══════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._schedule_loop,
            name="pipeline-reporter",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Pipeline reporter started (daily@%02d:00 UTC, weekly on day %d, monthly on day %d)",
            self._daily_hour, self._weekly_day, self._monthly_day,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("Pipeline reporter stopped")

    # ═══════════════════════════════════════════════════════════════════════
    # Internal
    # ═══════════════════════════════════════════════════════════════════════

    def _schedule_loop(self) -> None:
        while self._running:
            try:
                now = datetime.now(timezone.utc)
                self._maybe_send_daily(now)
                self._maybe_send_weekly(now)
                self._maybe_send_monthly(now)
            except Exception as e:
                logger.error("Reporter schedule error: %s", e)
            time.sleep(self._check_interval)

    def _maybe_send_daily(self, now: datetime) -> None:
        date_str = now.strftime("%Y-%m-%d")
        if now.hour == self._daily_hour and self._last_daily_date != date_str:
            self._last_daily_date = date_str
            report = self._build_daily_report(date_str)
            self._slack_daily.send_daily_report(report)
            logger.info("Daily report sent for %s", date_str)
            self._reset_daily()

    def _maybe_send_weekly(self, now: datetime) -> None:
        iso_week = now.isocalendar()[1]
        if now.weekday() == self._weekly_day and now.hour == self._daily_hour and self._last_weekly_week != iso_week:
            self._last_weekly_week = iso_week
            report = self._build_weekly_report(f"Week {iso_week}")
            self._slack_weekly.send_weekly_report(report)
            logger.info("Weekly report sent for week %d", iso_week)
            self._reset_weekly()

    def _build_daily_report(self, date_str: str) -> Dict:
        with self._lock:
            total = self._daily_valid + self._daily_invalid
            return {
                "date": date_str,
                "readings": self._daily_readings,
                "anomalies": self._daily_anomalies,
                "dlq": self._daily_dlq,
                "avg_latency_ms": (
                    sum(self._daily_latencies) / len(self._daily_latencies)
                    if self._daily_latencies
                    else 0
                ),
                "quality_score": (self._daily_valid / total * 100) if total > 0 else 100.0,
                "device_failures": self._daily_failures,
            }

    def _build_weekly_report(self, week_str: str) -> Dict:
        with self._lock:
            total = self._weekly_valid + self._weekly_invalid
            return {
                "week": week_str,
                "readings": self._weekly_readings,
                "anomalies": self._weekly_anomalies,
                "dlq": self._weekly_dlq,
                "avg_latency_ms": (
                    sum(self._weekly_latencies) / len(self._weekly_latencies)
                    if self._weekly_latencies
                    else 0
                ),
                "quality_score": (self._weekly_valid / total * 100) if total > 0 else 100.0,
                "device_failures": self._weekly_failures,
            }

    def _reset_daily(self) -> None:
        with self._lock:
            self._daily_readings = 0
            self._daily_anomalies = 0
            self._daily_dlq = 0
            self._daily_failures = 0
            self._daily_latencies.clear()
            self._daily_valid = 0
            self._daily_invalid = 0
            REPORT_READINGS_DAILY.set(0)
            REPORT_ANOMALIES_DAILY.set(0)

    def _reset_weekly(self) -> None:
        with self._lock:
            self._weekly_readings = 0
            self._weekly_anomalies = 0
            self._weekly_dlq = 0
            self._weekly_failures = 0
            self._weekly_latencies.clear()
            self._weekly_valid = 0
            self._weekly_invalid = 0
            REPORT_READINGS_WEEKLY.set(0)
            REPORT_ANOMALIES_WEEKLY.set(0)

    # ── Monthly ─────────────────────────────────────────────────────────

    def _maybe_send_monthly(self, now: datetime) -> None:
        month_str = now.strftime("%Y-%m")
        if (
            now.day == self._monthly_day
            and now.hour == self._daily_hour
            and self._last_monthly_month != month_str
        ):
            self._last_monthly_month = month_str
            # Use the *previous* month label (the month we're reporting on)
            prev_month = (now.replace(day=1) - timedelta(days=1))
            label = prev_month.strftime("%B %Y")
            report = self._build_monthly_report(label)

            if self._email_monthly:
                self._email_monthly.send_monthly_report(report)
                logger.info("Monthly email report sent for %s", label)
            else:
                logger.warning("Monthly email reporter not configured - skipping email")

            self._reset_monthly()

    def _build_monthly_report(self, month_label: str) -> Dict:
        with self._lock:
            total = self._monthly_valid + self._monthly_invalid
            return {
                "month": month_label,
                "readings": self._monthly_readings,
                "anomalies": self._monthly_anomalies,
                "dlq": self._monthly_dlq,
                "avg_latency_ms": (
                    sum(self._monthly_latencies) / len(self._monthly_latencies)
                    if self._monthly_latencies
                    else 0
                ),
                "quality_score": (self._monthly_valid / total * 100) if total > 0 else 100.0,
                "device_failures": self._monthly_failures,
            }

    def _reset_monthly(self) -> None:
        with self._lock:
            self._monthly_readings = 0
            self._monthly_anomalies = 0
            self._monthly_dlq = 0
            self._monthly_failures = 0
            self._monthly_latencies.clear()
            self._monthly_valid = 0
            self._monthly_invalid = 0
            REPORT_READINGS_MONTHLY.set(0)
            REPORT_ANOMALIES_MONTHLY.set(0)
