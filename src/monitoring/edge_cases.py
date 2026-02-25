"""
Edge-Case Detection Engine

Monitors the heartbeat pipeline for five key edge cases and triggers
Prometheus metric updates + Slack/Email alerts:

    1. Heartbeat Delay   – Device has not sent data within expected interval
    2. Device Failure    – Prolonged silence → device marked FAILED
    3. Sudden Spike      – Heart-rate delta between consecutive reads exceeds threshold
    4. Data Quality      – Tracking validation pass/fail rate
    5. System Health     – Kafka, DB, and pipeline component liveness
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Optional

from src.config import config
from src.logger import get_logger
from src.models import HeartbeatReading
from src.monitoring.alerting import EmailAlerter, SlackAlerter
from src.monitoring.metrics import (
    ANOMALIES_DETECTED,
    ANOMALY_RATE,
    DATA_QUALITY_INVALID,
    DATA_QUALITY_SCORE,
    DATA_QUALITY_VALID,
    DEVICE_CONSECUTIVE_ERRORS,
    DEVICE_FAILURE_TOTAL,
    DEVICE_HEARTBEAT_DELAY_ALERT,
    DEVICE_HEARTBEAT_DELAY_SECONDS,
    DEVICE_LAST_SEEN,
    DEVICE_STATUS,
    HEART_RATE_HISTOGRAM,
    HEART_RATE_VALUE,
    SPIKE_DETECTED,
)

logger = get_logger("monitoring.edge_cases")


class EdgeCaseDetector:
    """
    Central edge-case detection engine.

    Call ``on_reading()`` for every new heartbeat reading; the detector
    updates Prometheus metrics and fires alerts as needed.  A background
    thread periodically checks for stale devices (delay / failure).
    """

    def __init__(
        self,
        slack: SlackAlerter | None = None,
        email: EmailAlerter | None = None,
        heartbeat_delay_threshold_sec: float | None = None,
        device_failure_threshold_sec: float | None = None,
        spike_delta_threshold: int | None = None,
        sustained_anomaly_count: int | None = None,
        sustained_anomaly_window_min: int | None = None,
        check_interval_sec: float | None = None,
    ):
        """
        Args:
            slack: SlackAlerter instance.
            email: EmailAlerter instance.
            heartbeat_delay_threshold_sec: Seconds after which a device
                is considered delayed (default from config).
            device_failure_threshold_sec: Seconds after which a delayed
                device is considered FAILED (default from config).
            spike_delta_threshold: Minimum HR delta between consecutive
                reads to trigger a spike alert.
            sustained_anomaly_count: Anomalies within the window that
                trigger a critical email.
            sustained_anomaly_window_min: Rolling window in minutes.
            check_interval_sec: How often the background thread checks
                for stale devices.
        """
        self._slack = slack or SlackAlerter()
        self._email = email or EmailAlerter()

        self._delay_threshold = heartbeat_delay_threshold_sec or config.monitoring.heartbeat_delay_threshold_sec
        self._failure_threshold = device_failure_threshold_sec or config.monitoring.device_failure_threshold_sec
        self._spike_delta = spike_delta_threshold or config.monitoring.spike_delta_threshold
        self._sustained_count = sustained_anomaly_count or config.monitoring.sustained_anomaly_count
        self._sustained_window = sustained_anomaly_window_min or config.monitoring.sustained_anomaly_window_min
        self._check_interval = check_interval_sec or config.monitoring.edge_check_interval_sec

        # Per-device tracking
        self._last_seen: Dict[str, float] = {}           # customer_id → unix ts
        self._last_hr: Dict[str, int] = {}               # customer_id → last heart rate
        self._device_status: Dict[str, str] = {}          # 'ACTIVE' | 'FAILED'
        self._consecutive_errors: Dict[str, int] = defaultdict(int)
        self._anomaly_window: Dict[str, list] = defaultdict(list)  # customer → [timestamps]

        # Global counters for quality/anomaly rate
        self._total_readings = 0
        self._total_valid = 0
        self._total_invalid = 0
        self._total_anomalies = 0

        # Alert cooldowns (avoid spamming) – customer_id → last alert ts
        self._delay_alert_cooldown: Dict[str, float] = {}
        self._failure_alert_cooldown: Dict[str, float] = {}
        self._spike_alert_cooldown: Dict[str, float] = {}
        self._sustained_alert_cooldown: Dict[str, float] = {}
        self._alert_cooldown_sec = 300  # 5 minutes

        # Background checker
        self._running = False
        self._checker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # ═══════════════════════════════════════════════════════════════════════
    # Public API
    # ═══════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        """Start the background device-health checker thread."""
        self._running = True
        self._checker_thread = threading.Thread(
            target=self._background_check_loop,
            name="edge-case-checker",
            daemon=True,
        )
        self._checker_thread.start()
        logger.info(
            "Edge-case detector started (delay=%ds, failure=%ds, spike_delta=%d, check_interval=%ds)",
            self._delay_threshold,
            self._failure_threshold,
            self._spike_delta,
            self._check_interval,
        )

    def stop(self) -> None:
        self._running = False
        if self._checker_thread and self._checker_thread.is_alive():
            self._checker_thread.join(timeout=10)
        logger.info("Edge-case detector stopped")

    def on_reading(self, reading: HeartbeatReading) -> None:
        """
        Process a new heartbeat reading for edge-case monitoring.

        Should be called for every reading that enters the pipeline.
        """
        now = time.time()
        cid = reading.customer_id

        with self._lock:
            # ── Update last-seen ────────────────────────────────────────
            self._last_seen[cid] = now
            DEVICE_LAST_SEEN.labels(customer_id=cid).set(now)
            DEVICE_HEARTBEAT_DELAY_SECONDS.labels(customer_id=cid).set(0)

            # Mark device ACTIVE if previously failed
            if self._device_status.get(cid) == "FAILED":
                logger.info("Device %s recovered (back ACTIVE)", cid)
            self._device_status[cid] = "ACTIVE"
            DEVICE_STATUS.labels(customer_id=cid).set(1)
            self._consecutive_errors[cid] = 0
            DEVICE_CONSECUTIVE_ERRORS.labels(customer_id=cid).set(0)

            # ── Heart rate metric ───────────────────────────────────────
            HEART_RATE_VALUE.labels(customer_id=cid).set(reading.heart_rate)
            HEART_RATE_HISTOGRAM.labels(customer_id=cid).observe(reading.heart_rate)

            # ── Spike detection (Edge Case 3) ───────────────────────────
            prev_hr = self._last_hr.get(cid)
            if prev_hr is not None:
                delta = reading.heart_rate - prev_hr
                if abs(delta) >= self._spike_delta:
                    direction = "up" if delta > 0 else "down"
                    SPIKE_DETECTED.labels(customer_id=cid, direction=direction).inc()
                    logger.warning(
                        "Spike detected: %s HR %d→%d (Δ%+d)",
                        cid, prev_hr, reading.heart_rate, delta,
                    )
                    if self._can_alert("spike", cid, now):
                        self._slack.send_spike_alert(cid, prev_hr, reading.heart_rate, delta)
            self._last_hr[cid] = reading.heart_rate

            # ── Anomaly tracking ────────────────────────────────────────
            if reading.is_anomaly:
                self._total_anomalies += 1
                anomaly_type = reading.anomaly_type or "UNKNOWN"
                ANOMALIES_DETECTED.labels(customer_id=cid, anomaly_type=anomaly_type).inc()

                # Sustained anomaly window
                self._anomaly_window[cid].append(now)
                # Trim window
                cutoff = now - (self._sustained_window * 60)
                self._anomaly_window[cid] = [
                    t for t in self._anomaly_window[cid] if t > cutoff
                ]
                if len(self._anomaly_window[cid]) >= self._sustained_count:
                    if self._can_alert("sustained", cid, now):
                        self._email.send_sustained_anomaly_email(
                            cid,
                            len(self._anomaly_window[cid]),
                            self._sustained_window,
                        )
                        self._slack.send_alert(
                            title=f"Sustained Anomalies – {cid}",
                            message=f"{len(self._anomaly_window[cid])} anomalies in {self._sustained_window} min",
                            severity="critical",
                        )

            self._total_readings += 1
            # Update anomaly rate gauge
            if self._total_readings > 0:
                ANOMALY_RATE.set(self._total_anomalies / self._total_readings * 100)

    def on_valid(self) -> None:
        """Record a valid reading (data quality tracking)."""
        self._total_valid += 1
        DATA_QUALITY_VALID.inc()
        self._update_quality_score()

    def on_invalid(self, reason: str = "unknown") -> None:
        """Record an invalid/rejected reading."""
        self._total_invalid += 1
        DATA_QUALITY_INVALID.labels(validation_error=reason).inc()
        self._update_quality_score()

    def on_device_error(self, customer_id: str) -> None:
        """Record a processing error for a device."""
        with self._lock:
            self._consecutive_errors[customer_id] += 1
            DEVICE_CONSECUTIVE_ERRORS.labels(customer_id=customer_id).set(
                self._consecutive_errors[customer_id]
            )

    # ═══════════════════════════════════════════════════════════════════════
    # Background Checker
    # ═══════════════════════════════════════════════════════════════════════

    def _background_check_loop(self) -> None:
        """Periodically check for delayed / failed devices."""
        while self._running:
            try:
                self._check_device_health()
            except Exception as e:
                logger.error("Edge-case checker error: %s", e)
            time.sleep(self._check_interval)

    def _check_device_health(self) -> None:
        """Inspect every known device for delay / failure."""
        now = time.time()
        with self._lock:
            for cid, last_ts in list(self._last_seen.items()):
                silent = now - last_ts
                DEVICE_HEARTBEAT_DELAY_SECONDS.labels(customer_id=cid).set(silent)

                if silent >= self._failure_threshold:
                    if self._device_status.get(cid) != "FAILED":
                        self._device_status[cid] = "FAILED"
                        DEVICE_STATUS.labels(customer_id=cid).set(0)
                        DEVICE_FAILURE_TOTAL.labels(customer_id=cid).inc()
                        last_seen_str = datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()
                        logger.error("Device FAILED: %s (silent %ds)", cid, int(silent))

                        if self._can_alert("failure", cid, now):
                            self._slack.send_device_failure_alert(cid, last_seen_str, silent)
                            self._email.send_device_failure_email(cid, last_seen_str, silent)

                elif silent >= self._delay_threshold:
                    DEVICE_HEARTBEAT_DELAY_ALERT.labels(customer_id=cid).inc()
                    if self._can_alert("delay", cid, now):
                        logger.warning("Device delayed: %s (silent %ds)", cid, int(silent))
                        self._slack.send_alert(
                            title=f"Heartbeat Delay – {cid}",
                            message=f"No data for {int(silent)}s (threshold: {self._delay_threshold}s)",
                            severity="warning",
                            fields={"Customer": cid, "Silent": f"{int(silent)}s"},
                        )

    # ═══════════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════════

    def _update_quality_score(self) -> None:
        total = self._total_valid + self._total_invalid
        if total > 0:
            DATA_QUALITY_SCORE.set(self._total_valid / total * 100)

    def _can_alert(self, alert_type: str, customer_id: str, now: float) -> bool:
        """Cooldown check to avoid alert storms."""
        cooldown_map = {
            "delay": self._delay_alert_cooldown,
            "failure": self._failure_alert_cooldown,
            "spike": self._spike_alert_cooldown,
            "sustained": self._sustained_alert_cooldown,
        }
        cd = cooldown_map.get(alert_type, {})
        last = cd.get(customer_id, 0)
        if now - last < self._alert_cooldown_sec:
            return False
        cd[customer_id] = now
        return True

    @property
    def device_statuses(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._device_status)

    @property
    def quality_stats(self) -> Dict[str, float]:
        total = self._total_valid + self._total_invalid
        return {
            "total_processed": total,
            "valid": self._total_valid,
            "rejected": self._total_invalid,
            "quality_score": (self._total_valid / total * 100) if total > 0 else 100.0,
            "anomaly_rate": (
                self._total_anomalies / self._total_readings * 100
                if self._total_readings > 0
                else 0.0
            ),
        }
