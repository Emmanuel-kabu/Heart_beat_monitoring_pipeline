"""
Alerting Module – Slack & Email Notifications

Sends alerts to Slack (data quality, system health, daily/weekly reports)
and Email (critical issues like device failure, sustained anomalies).

Environment variables required:
    SLACK_WEBHOOK_URL          – Slack Incoming Webhook URL
    SMTP_HOST                  – SMTP server hostname
    SMTP_PORT                  – SMTP server port (587 for TLS)
    SMTP_USER                  – SMTP username / sender email
    SMTP_PASSWORD              – SMTP password or app password
    ALERT_EMAIL_RECIPIENTS     – Comma-separated recipient email addresses
"""

from __future__ import annotations

import json
import smtplib
import threading
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional
from urllib.request import Request, urlopen

from src.config import config
from src.logger import get_logger

logger = get_logger("monitoring.alerting")


class SlackAlerter:
    """
    Sends formatted messages to a Slack channel via Incoming Webhook.
    """

    def __init__(self, webhook_url: str | None = None, channel: str | None = None):
        self._webhook_url = webhook_url or config.alerting.slack_webhook_url
        self._channel = channel
        if not self._webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set - Slack alerts disabled")

    # ─── Low-Level Send ─────────────────────────────────────────────────

    def _post(self, payload: dict) -> bool:
        """Post a JSON payload to the Slack webhook (fire-and-forget)."""
        if not self._webhook_url:
            return False
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                self._webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error("Slack send failed: %s", e)
            return False

    def send_async(self, payload: dict) -> None:
        """Non-blocking send in a daemon thread."""
        t = threading.Thread(target=self._post, args=(payload,), daemon=True)
        t.start()

    # ─── High-Level Helpers ─────────────────────────────────────────────

    def send_alert(
        self,
        title: str,
        message: str,
        severity: str = "warning",
        fields: dict | None = None,
    ) -> None:
        """
        Send a structured alert to Slack.

        Args:
            title: Alert headline.
            message: Alert body.
            severity: 'info', 'warning', 'critical'.
            fields: Optional key-value pairs for context.
        """
        color_map = {
            "info": "#36a64f",
            "warning": "#ffaa00",
            "critical": "#ff0000",
        }
        color = color_map.get(severity, "#cccccc")
        emoji_map = {
            "info": ":large_blue_circle:",
            "warning": ":warning:",
            "critical": ":red_circle:",
        }
        emoji = emoji_map.get(severity, ":grey_question:")

        attachment_fields = []
        if fields:
            for k, v in fields.items():
                attachment_fields.append({"title": k, "value": str(v), "short": True})

        payload = {
            "text": f"{emoji} *{title}*",
            "attachments": [
                {
                    "color": color,
                    "text": message,
                    "fields": attachment_fields,
                    "footer": "Heartbeat Pipeline Alerting",
                    "ts": int(datetime.now(timezone.utc).timestamp()),
                }
            ],
        }
        if self._channel:
            payload["channel"] = self._channel
        self.send_async(payload)
        logger.info("Slack alert queued: [%s] %s", severity, title)

    def send_data_quality_report(self, report: dict) -> None:
        """Send a data quality report summary to Slack."""
        fields = {
            "Total Processed": report.get("total_processed", 0),
            "Valid": report.get("valid", 0),
            "Rejected": report.get("rejected", 0),
            "Quality Score": f"{report.get('quality_score', 0):.1f}%",
            "Anomaly Rate": f"{report.get('anomaly_rate', 0):.1f}%",
            "DLQ Messages": report.get("dlq_count", 0),
        }
        self.send_alert(
            title="Data Quality Report",
            message="Periodic data quality summary from the heartbeat pipeline.",
            severity="info",
            fields=fields,
        )

    def send_system_health_report(self, report: dict) -> None:
        """Send a system health summary to Slack."""
        severity = "info"
        if not report.get("all_healthy", True):
            severity = "critical"

        fields = {
            "Kafka Producer": "UP" if report.get("kafka_producer") else "DOWN",
            "Kafka Consumer": "UP" if report.get("kafka_consumer") else "DOWN",
            "Database": "UP" if report.get("database") else "DOWN",
            "Uptime": f"{report.get('uptime_hours', 0):.1f} hours",
            "Consumer Lag": report.get("consumer_lag", "N/A"),
            "Active Devices": report.get("active_devices", "N/A"),
        }
        self.send_alert(
            title="System Health Report",
            message="Infrastructure health check from heartbeat pipeline.",
            severity=severity,
            fields=fields,
        )

    def send_daily_report(self, report: dict) -> None:
        """Send a daily processing summary to Slack."""
        self.send_alert(
            title=f"Daily Report - {report.get('date', 'N/A')}",
            message="Daily processing summary from the heartbeat pipeline.",
            severity="info",
            fields={
                "Readings Processed": report.get("readings", 0),
                "Anomalies": report.get("anomalies", 0),
                "DLQ Messages": report.get("dlq", 0),
                "Avg Latency": f"{report.get('avg_latency_ms', 0):.0f} ms",
                "Quality Score": f"{report.get('quality_score', 0):.1f}%",
                "Device Failures": report.get("device_failures", 0),
            },
        )

    def send_weekly_report(self, report: dict) -> None:
        """Send a weekly processing summary to Slack."""
        self.send_alert(
            title=f"Weekly Report - {report.get('week', 'N/A')}",
            message="Weekly processing summary from the heartbeat pipeline.",
            severity="info",
            fields={
                "Total Readings": report.get("readings", 0),
                "Total Anomalies": report.get("anomalies", 0),
                "DLQ Messages": report.get("dlq", 0),
                "Avg Latency": f"{report.get('avg_latency_ms', 0):.0f} ms",
                "Quality Score": f"{report.get('quality_score', 0):.1f}%",
                "Device Failures": report.get("device_failures", 0),
            },
        )

    def send_spike_alert(self, customer_id: str, prev_hr: int, curr_hr: int, delta: int) -> None:
        """Alert on sudden heart rate spike."""
        direction = "increase" if delta > 0 else "decrease"
        self.send_alert(
            title=f"Sudden Spike Detected – {customer_id}",
            message=(
                f"Heart rate {direction} of {abs(delta)} bpm detected. "
                f"Previous: {prev_hr} bpm → Current: {curr_hr} bpm."
            ),
            severity="warning",
            fields={"Customer": customer_id, "Delta": f"{delta:+d} bpm"},
        )

    def send_device_failure_alert(self, customer_id: str, last_seen: str, silent_sec: float) -> None:
        """Alert when a device is detected as failed."""
        self.send_alert(
            title=f"Device Failure – {customer_id}",
            message=(
                f"No data received from {customer_id} for {silent_sec:.0f} seconds. "
                f"Last seen: {last_seen}."
            ),
            severity="critical",
            fields={"Customer": customer_id, "Silent Duration": f"{silent_sec:.0f}s"},
        )


class EmailAlerter:
    """
    Sends critical alert emails via SMTP (TLS).
    """

    def __init__(
        self,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        smtp_user: str | None = None,
        smtp_password: str | None = None,
        recipients: list[str] | None = None,
    ):
        self._host = smtp_host or config.alerting.smtp_host
        self._port = smtp_port or config.alerting.smtp_port
        self._user = smtp_user or config.alerting.smtp_user
        self._password = smtp_password or config.alerting.smtp_password
        self._recipients = recipients or config.alerting.email_recipients

        if not all([self._host, self._user, self._password, self._recipients]):
            logger.warning("Email alerting not fully configured – emails disabled")
            self._enabled = False
        else:
            self._enabled = True

    def send_email(self, subject: str, html_body: str) -> bool:
        """
        Send an HTML email to configured recipients.

        Args:
            subject: Email subject line.
            html_body: HTML content body.

        Returns:
            True if sent successfully.
        """
        if not self._enabled:
            logger.debug("Email alerting disabled, skipping: %s", subject)
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[Heartbeat Pipeline] {subject}"
            msg["From"] = self._user
            msg["To"] = ", ".join(self._recipients)

            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self._host, self._port, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(self._user, self._password)
                server.sendmail(self._user, self._recipients, msg.as_string())

            logger.info("Email sent: %s → %s", subject, self._recipients)
            return True

        except Exception as e:
            logger.error("Failed to send email '%s': %s", subject, e)
            return False

    def send_email_async(self, subject: str, html_body: str) -> None:
        """Non-blocking email send."""
        t = threading.Thread(
            target=self.send_email, args=(subject, html_body), daemon=True
        )
        t.start()

    # ─── Pre-built Critical Alerts ──────────────────────────────────────

    def send_device_failure_email(
        self, customer_id: str, last_seen: str, silent_sec: float
    ) -> None:
        """Send a critical device failure email."""
        html = f"""
        <html><body style="font-family:Arial,sans-serif;">
        <h2 style="color:#cc0000;">🚨 Critical: Device Failure Detected</h2>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
            <tr><td><b>Customer / Device</b></td><td>{customer_id}</td></tr>
            <tr><td><b>Last Seen</b></td><td>{last_seen}</td></tr>
            <tr><td><b>Silent Duration</b></td><td>{silent_sec:.0f} seconds</td></tr>
            <tr><td><b>Severity</b></td><td style="color:red;"><b>CRITICAL</b></td></tr>
        </table>
        <p>Immediate investigation is required. The device may have failed or lost connectivity.</p>
        <p style="color:#888;font-size:12px;">Heartbeat Monitoring Pipeline – Automated Alert</p>
        </body></html>
        """
        self.send_email_async(f"CRITICAL – Device Failure: {customer_id}", html)

    def send_sustained_anomaly_email(
        self, customer_id: str, anomaly_count: int, window_min: int
    ) -> None:
        """Send a critical email for sustained anomalies."""
        html = f"""
        <html><body style="font-family:Arial,sans-serif;">
        <h2 style="color:#cc0000;">⚠️ Critical: Sustained Anomalies</h2>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
            <tr><td><b>Customer / Device</b></td><td>{customer_id}</td></tr>
            <tr><td><b>Anomaly Count</b></td><td>{anomaly_count}</td></tr>
            <tr><td><b>Time Window</b></td><td>Last {window_min} minutes</td></tr>
            <tr><td><b>Severity</b></td><td style="color:red;"><b>CRITICAL</b></td></tr>
        </table>
        <p>This device has reported an unusually high number of anomalous readings.
        Clinical review or device inspection recommended.</p>
        <p style="color:#888;font-size:12px;">Heartbeat Monitoring Pipeline – Automated Alert</p>
        </body></html>
        """
        self.send_email_async(
            f"CRITICAL – Sustained Anomalies: {customer_id}", html
        )

    def send_pipeline_down_email(self, component: str, error: str) -> None:
        """Send a critical email when a pipeline component goes down."""
        html = f"""
        <html><body style="font-family:Arial,sans-serif;">
        <h2 style="color:#cc0000;">🔥 Critical: Pipeline Component Down</h2>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
            <tr><td><b>Component</b></td><td>{component}</td></tr>
            <tr><td><b>Error</b></td><td>{error}</td></tr>
            <tr><td><b>Time</b></td><td>{datetime.now(timezone.utc).isoformat()}</td></tr>
            <tr><td><b>Severity</b></td><td style="color:red;"><b>CRITICAL</b></td></tr>
        </table>
        <p>The pipeline component is not functioning. Immediate action required.</p>
        <p style="color:#888;font-size:12px;">Heartbeat Monitoring Pipeline – Automated Alert</p>
        </body></html>
        """
        self.send_email_async(f"CRITICAL – {component} DOWN", html)

    def send_dlq_threshold_email(self, dlq_count: int, threshold: int) -> None:
        """Send email when DLQ message count exceeds threshold."""
        html = f"""
        <html><body style="font-family:Arial,sans-serif;">
        <h2 style="color:#cc6600;">⚠️ Warning: DLQ Threshold Exceeded</h2>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
            <tr><td><b>Current DLQ Count</b></td><td>{dlq_count}</td></tr>
            <tr><td><b>Threshold</b></td><td>{threshold}</td></tr>
            <tr><td><b>Time</b></td><td>{datetime.now(timezone.utc).isoformat()}</td></tr>
        </table>
        <p>The Dead Letter Queue has accumulated more messages than expected.
        Review and reprocess or discard stale messages.</p>
        <p style="color:#888;font-size:12px;">Heartbeat Monitoring Pipeline – Automated Alert</p>
        </body></html>
        """
        self.send_email_async(f"WARNING – DLQ Threshold Exceeded ({dlq_count})", html)

    def send_monthly_report(self, report: dict) -> None:
        """
        Send a monthly processing summary email.

        Args:
            report: Dictionary with keys: month, readings, anomalies, dlq,
                    avg_latency_ms, quality_score, device_failures, peak_day,
                    peak_day_readings.
        """
        month = report.get("month", "N/A")
        readings = report.get("readings", 0)
        anomalies = report.get("anomalies", 0)
        dlq = report.get("dlq", 0)
        avg_latency = report.get("avg_latency_ms", 0)
        quality_score = report.get("quality_score", 100.0)
        failures = report.get("device_failures", 0)
        anomaly_rate = (anomalies / readings * 100) if readings > 0 else 0

        # Decide quality colour
        if quality_score >= 95:
            q_color = "#2ecc71"
        elif quality_score >= 80:
            q_color = "#f39c12"
        else:
            q_color = "#e74c3c"

        html = f"""
        <html>
        <body style="font-family:Arial,Helvetica,sans-serif;background:#f4f6f8;padding:20px;">
        <div style="max-width:640px;margin:auto;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

          <!-- Header -->
          <div style="background:#1a73e8;color:#ffffff;padding:24px 32px;">
            <h1 style="margin:0;font-size:22px;">Monthly Pipeline Report</h1>
            <p style="margin:6px 0 0;font-size:14px;opacity:0.9;">{month}</p>
          </div>

          <!-- Summary cards -->
          <div style="padding:24px 32px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
              <tr>
                <td style="padding:12px;text-align:center;background:#f8f9fa;border-radius:6px;width:33%;">
                  <div style="font-size:28px;font-weight:bold;color:#1a73e8;">{readings:,}</div>
                  <div style="font-size:12px;color:#666;margin-top:4px;">Total Readings</div>
                </td>
                <td width="12"></td>
                <td style="padding:12px;text-align:center;background:#f8f9fa;border-radius:6px;width:33%;">
                  <div style="font-size:28px;font-weight:bold;color:{q_color};">{quality_score:.1f}%</div>
                  <div style="font-size:12px;color:#666;margin-top:4px;">Quality Score</div>
                </td>
                <td width="12"></td>
                <td style="padding:12px;text-align:center;background:#f8f9fa;border-radius:6px;width:33%;">
                  <div style="font-size:28px;font-weight:bold;color:#e74c3c;">{anomalies:,}</div>
                  <div style="font-size:12px;color:#666;margin-top:4px;">Anomalies</div>
                </td>
              </tr>
            </table>
          </div>

          <!-- Detail table -->
          <div style="padding:0 32px 24px;">
            <table width="100%" border="0" cellpadding="10" cellspacing="0"
                   style="border-collapse:collapse;border:1px solid #e0e0e0;border-radius:6px;">
              <tr style="background:#f8f9fa;">
                <td style="border-bottom:1px solid #e0e0e0;"><b>Metric</b></td>
                <td style="border-bottom:1px solid #e0e0e0;text-align:right;"><b>Value</b></td>
              </tr>
              <tr>
                <td style="border-bottom:1px solid #f0f0f0;">Readings Processed</td>
                <td style="border-bottom:1px solid #f0f0f0;text-align:right;">{readings:,}</td>
              </tr>
              <tr>
                <td style="border-bottom:1px solid #f0f0f0;">Anomalies Detected</td>
                <td style="border-bottom:1px solid #f0f0f0;text-align:right;">{anomalies:,} ({anomaly_rate:.1f}%)</td>
              </tr>
              <tr>
                <td style="border-bottom:1px solid #f0f0f0;">DLQ Messages</td>
                <td style="border-bottom:1px solid #f0f0f0;text-align:right;">{dlq:,}</td>
              </tr>
              <tr>
                <td style="border-bottom:1px solid #f0f0f0;">Device Failures</td>
                <td style="border-bottom:1px solid #f0f0f0;text-align:right;">{failures:,}</td>
              </tr>
              <tr>
                <td style="border-bottom:1px solid #f0f0f0;">Avg Latency</td>
                <td style="border-bottom:1px solid #f0f0f0;text-align:right;">{avg_latency:.0f} ms</td>
              </tr>
              <tr>
                <td>Quality Score</td>
                <td style="text-align:right;color:{q_color};font-weight:bold;">{quality_score:.1f}%</td>
              </tr>
            </table>
          </div>

          <!-- Footer -->
          <div style="background:#f8f9fa;padding:16px 32px;text-align:center;font-size:12px;color:#999;">
            Heartbeat Monitoring Pipeline &mdash; Automated Monthly Report<br>
            Generated at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
          </div>
        </div>
        </body>
        </html>
        """
        self.send_email_async(f"Monthly Report - {month}", html)
