"""
Data Quality Reporter

Sends **daily** and **weekly** data‑quality reports via Slack and Email.
Reports include:

* Overall quality score & trend (improving / degrading / stable)
* Per‑dimension breakdown (completeness, validity, …)
* Top failing rules (root‑cause identification)
* Top problematic customers / devices
* DQ circuit‑breaker trip count
* Actionable recommendations

Runs as a background daemon thread alongside the pipeline.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from src.data_quality.quality_store import DataQualityStore
from src.logger import get_logger
from src.monitoring.alerting import EmailAlerter, SlackAlerter

logger = get_logger("data_quality.reporter")


class DataQualityReporter:
    """
    Scheduled reporter for data‑quality metrics.

    Pulls accumulated data from ``DataQualityStore`` and pushes
    formatted reports to Slack (all) and Email (critical degradation
    only).
    """

    def __init__(
        self,
        quality_store: DataQualityStore,
        slack: SlackAlerter | None = None,
        email: EmailAlerter | None = None,
        daily_hour_utc: int = 23,
        weekly_day: int = 6,  # 0=Mon … 6=Sun
        check_interval_sec: float = 60,
        degradation_email_threshold: float = 80.0,
    ):
        """
        Args:
            quality_store: DataQualityStore holding accumulated results.
            slack: SlackAlerter for Slack reports.
            email: EmailAlerter for critical degradation emails.
            daily_hour_utc: UTC hour to send daily report.
            weekly_day: ISO weekday for weekly report.
            check_interval_sec: How often to check schedule.
            degradation_email_threshold: Score below which a critical
                email is sent.
        """
        self._store = quality_store
        self._slack = slack or SlackAlerter()
        self._email = email or EmailAlerter()
        self._daily_hour = daily_hour_utc
        self._weekly_day = weekly_day
        self._check_interval = check_interval_sec
        self._degradation_threshold = degradation_email_threshold

        self._last_daily_date: Optional[str] = None
        self._last_weekly_week: Optional[int] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ───────────────────────────────────────────────────────────────────
    # Lifecycle
    # ───────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background reporter thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._schedule_loop,
            name="dq-reporter",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "DQ Reporter started (daily@%02d:00 UTC, weekly day=%d)",
            self._daily_hour,
            self._weekly_day,
        )

    def stop(self) -> None:
        """Stop the reporter."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("DQ Reporter stopped")

    def _schedule_loop(self) -> None:
        while self._running:
            try:
                now = datetime.now(timezone.utc)
                self._maybe_send_daily(now)
                self._maybe_send_weekly(now)
            except Exception as exc:
                logger.error("DQ Reporter schedule error: %s", exc)
            time.sleep(self._check_interval)

    # ───────────────────────────────────────────────────────────────────
    # Schedule checks
    # ───────────────────────────────────────────────────────────────────

    def _maybe_send_daily(self, now: datetime) -> None:
        date_str = now.strftime("%Y-%m-%d")
        if now.hour == self._daily_hour and self._last_daily_date != date_str:
            self._last_daily_date = date_str
            data = self._store.pop_daily_report_data()
            self._send_daily_slack(date_str, data)
            # Send email if quality degraded
            if data["avg_score"] < self._degradation_threshold:
                self._send_degradation_email("Daily", date_str, data)
            logger.info("DQ daily report sent for %s", date_str)

    def _maybe_send_weekly(self, now: datetime) -> None:
        iso_week = now.isocalendar()[1]
        if (
            now.weekday() == self._weekly_day
            and now.hour == self._daily_hour
            and self._last_weekly_week != iso_week
        ):
            self._last_weekly_week = iso_week
            data = self._store.pop_weekly_report_data()
            self._send_weekly_slack(f"Week {iso_week}", data)
            if data["avg_score"] < self._degradation_threshold:
                self._send_degradation_email("Weekly", f"Week {iso_week}", data)
            logger.info("DQ weekly report sent for week %d", iso_week)

    # ───────────────────────────────────────────────────────────────────
    # Slack reports
    # ───────────────────────────────────────────────────────────────────

    def _send_daily_slack(self, date_str: str, data: Dict) -> None:
        trend = data.get("quality_trend", {})
        trend_emoji = {
            "improving": "📈",
            "degrading": "📉",
            "stable": "➡️",
        }.get(trend.get("direction", "stable"), "➡️")

        top_rules = self._format_top_list(data.get("top_failing_rules", []))
        top_custs = self._format_top_list(data.get("top_failing_customers", []))

        fields = {
            "Rows Validated": f"{data.get('rows', 0):,}",
            "Passed": f"{data.get('passed', 0):,}",
            "Failed (DLQ)": f"{data.get('failed', 0):,}",
            "Avg Quality Score": f"{data.get('avg_score', 100):.1f}%",
            "Trend": f"{trend_emoji} {trend.get('direction', 'stable')} ({trend.get('delta', 0):+.1f}%)",
            "Top Failing Rules": top_rules or "None",
            "Top Failing Devices": top_custs or "None",
        }

        severity = "info"
        if data.get("avg_score", 100) < 90:
            severity = "warning"
        if data.get("avg_score", 100) < self._degradation_threshold:
            severity = "critical"

        self._slack.send_alert(
            title=f"📊 DQ Daily Report – {date_str}",
            message="Data quality summary for the last 24 hours.",
            severity=severity,
            fields=fields,
        )

    def _send_weekly_slack(self, week_str: str, data: Dict) -> None:
        trend = data.get("quality_trend", {})
        trend_emoji = {
            "improving": "📈",
            "degrading": "📉",
            "stable": "➡️",
        }.get(trend.get("direction", "stable"), "➡️")

        top_rules = self._format_top_list(data.get("top_failing_rules", []))
        top_custs = self._format_top_list(data.get("top_failing_customers", []))

        fields = {
            "Total Rows": f"{data.get('rows', 0):,}",
            "Total Batches": f"{data.get('batches', 0):,}",
            "Passed": f"{data.get('passed', 0):,}",
            "Failed (DLQ)": f"{data.get('failed', 0):,}",
            "Avg Quality Score": f"{data.get('avg_score', 100):.1f}%",
            "Trend": f"{trend_emoji} {trend.get('direction', 'stable')} ({trend.get('delta', 0):+.1f}%)",
            "Top Failing Rules": top_rules or "None",
            "Top Failing Devices": top_custs or "None",
        }

        severity = "info"
        if data.get("avg_score", 100) < 90:
            severity = "warning"
        if data.get("avg_score", 100) < self._degradation_threshold:
            severity = "critical"

        self._slack.send_alert(
            title=f"📊 DQ Weekly Report – {week_str}",
            message="Data quality summary for the past week.",
            severity=severity,
            fields=fields,
        )

    # ───────────────────────────────────────────────────────────────────
    # Email (critical degradation only)
    # ───────────────────────────────────────────────────────────────────

    def _send_degradation_email(
        self,
        period: str,
        label: str,
        data: Dict,
    ) -> None:
        top_rules = data.get("top_failing_rules", [])
        top_custs = data.get("top_failing_customers", [])
        trend = data.get("quality_trend", {})

        rules_html = (
            "".join(f"<tr><td>{name}</td><td>{count}</td></tr>" for name, count in top_rules)
            or "<tr><td colspan='2'>None</td></tr>"
        )

        custs_html = (
            "".join(f"<tr><td>{cid}</td><td>{count}</td></tr>" for cid, count in top_custs)
            or "<tr><td colspan='2'>None</td></tr>"
        )

        html = f"""
        <html><body style="font-family:Arial,sans-serif;">
        <h2 style="color:#cc0000;">⚠️ Data Quality Degradation – {period} Report ({label})</h2>
        <p>The pipeline data quality has fallen below the threshold of
        <b>{self._degradation_threshold:.0f}%</b>.</p>

        <h3>Summary</h3>
        <table border="1" cellpadding="6" cellspacing="0"
               style="border-collapse:collapse;">
            <tr><td><b>Rows Validated</b></td><td>{data.get('rows', 0):,}</td></tr>
            <tr><td><b>Passed</b></td><td>{data.get('passed', 0):,}</td></tr>
            <tr><td><b>Failed (DLQ)</b></td><td>{data.get('failed', 0):,}</td></tr>
            <tr><td><b>Avg Quality Score</b></td>
                <td style="color:red;"><b>{data.get('avg_score', 0):.1f}%</b></td></tr>
            <tr><td><b>Trend</b></td>
                <td>{trend.get('direction', 'unknown')}
                    ({trend.get('delta', 0):+.1f}%)</td></tr>
        </table>

        <h3>Top Failing Rules (Root Cause)</h3>
        <table border="1" cellpadding="6" cellspacing="0"
               style="border-collapse:collapse;">
            <tr><th>Rule</th><th>Failures</th></tr>
            {rules_html}
        </table>

        <h3>Top Failing Devices</h3>
        <table border="1" cellpadding="6" cellspacing="0"
               style="border-collapse:collapse;">
            <tr><th>Device / Customer</th><th>Failures</th></tr>
            {custs_html}
        </table>

        <h3>Recommended Actions</h3>
        <ul>
            <li>Investigate the top‑failing rules above to identify root cause.</li>
            <li>Check the DLQ topic (<code>customer_heartbeat_dlq</code>) for
                quarantined messages.</li>
            <li>Review device connectivity for the top‑failing customers.</li>
            <li>If quality continues to degrade, consider pausing the
                producer or rolling back recent changes.</li>
        </ul>

        <p style="color:#888;font-size:12px;">
            Heartbeat Pipeline – Data Quality Framework – Automated Report
        </p>
        </body></html>
        """

        self._email.send_email_async(
            subject=f"DQ DEGRADATION – {period} ({label}) Quality={data.get('avg_score', 0):.0f}%",
            html_body=html,
        )

    # ───────────────────────────────────────────────────────────────────
    # Helpers
    # ───────────────────────────────────────────────────────────────────

    @staticmethod
    def _format_top_list(items: List[Tuple[str, int]]) -> str:
        """Format [(name, count), …] into a compact string."""
        if not items:
            return ""
        return ", ".join(f"{name} ({count})" for name, count in items[:5])

    # ───────────────────────────────────────────────────────────────────
    # On‑demand report  (for CLI / API usage)
    # ───────────────────────────────────────────────────────────────────

    def send_immediate_report(self) -> None:
        """Send an immediate quality‑status report to Slack."""
        summary = self._store.get_lifetime_summary()
        top_rules = self._store.get_top_failing_rules(5)
        top_custs = self._store.get_top_failing_customers(5)
        trend = self._store.get_quality_trend(window_minutes=60)

        fields = {
            "Total Rows": f"{summary['total_rows']:,}",
            "Pass Rate": f"{summary['pass_rate']:.1f}%",
            "Trend (1h)": f"{trend['direction']} ({trend['delta']:+.1f}%)",
            "Top Failing Rules": self._format_top_list(top_rules) or "None",
            "Top Failing Devices": self._format_top_list(top_custs) or "None",
        }
        # Add dimension scores
        for dim, score in summary.get("avg_dimension_scores", {}).items():
            fields[f"{dim.capitalize()}"] = f"{score:.1f}%"

        self._slack.send_alert(
            title="📊 DQ Status Report (On‑Demand)",
            message="Current data quality status snapshot.",
            severity="info",
            fields=fields,
        )
        logger.info("Immediate DQ report sent")
