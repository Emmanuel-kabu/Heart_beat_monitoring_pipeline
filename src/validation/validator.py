"""
Data Validation Module

Provides validation and anomaly detection logic for heartbeat readings.
Ensures data quality before persistence to the database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Tuple

from src.config import config
from src.logger import get_logger
from src.models import HeartbeatReading

logger = get_logger("validation")


class HeartbeatValidator:
    """
    Validates and enriches heartbeat readings with anomaly detection.

    Performs structural validation (required fields, data types, ranges)
    and business-rule validation (anomaly thresholds).
    """

    # Absolute physiological limits for heart rate
    ABSOLUTE_MIN_HR = 20
    ABSOLUTE_MAX_HR = 300

    # Valid customer ID pattern
    CUSTOMER_ID_PREFIX = "CUST-"

    def __init__(
        self,
        low_threshold: int | None = None,
        high_threshold: int | None = None,
    ):
        """
        Initialize the validator with anomaly thresholds.

        Args:
            low_threshold: Heart rate below this is anomalous. Defaults to config.
            high_threshold: Heart rate above this is anomalous. Defaults to config.
        """
        self.low_threshold = low_threshold or config.anomaly.low_threshold
        self.high_threshold = high_threshold or config.anomaly.high_threshold
        self._validated_count = 0
        self._rejected_count = 0
        self._anomaly_count = 0

        logger.info(
            "HeartbeatValidator initialized (low=%d, high=%d)",
            self.low_threshold,
            self.high_threshold,
        )

    def validate_structure(self, reading: HeartbeatReading) -> Tuple[bool, str]:
        """
        Validate the structural integrity of a heartbeat reading.

        Checks:
            - customer_id is present and follows expected format
            - heart_rate is within physiological limits
            - timestamp is valid ISO 8601 format

        Args:
            reading: The HeartbeatReading to validate.

        Returns:
            Tuple of (is_valid, error_message). error_message is empty if valid.
        """
        # Check customer_id
        if not reading.customer_id:
            return False, "Missing customer_id"
        if not reading.customer_id.startswith(self.CUSTOMER_ID_PREFIX):
            return False, f"Invalid customer_id format: {reading.customer_id}"

        # Check heart rate range
        if not isinstance(reading.heart_rate, int):
            return False, f"heart_rate must be integer, got {type(reading.heart_rate)}"
        if reading.heart_rate < self.ABSOLUTE_MIN_HR:
            return False, f"heart_rate {reading.heart_rate} below absolute minimum ({self.ABSOLUTE_MIN_HR})"
        if reading.heart_rate > self.ABSOLUTE_MAX_HR:
            return False, f"heart_rate {reading.heart_rate} above absolute maximum ({self.ABSOLUTE_MAX_HR})"

        # Check timestamp
        if not reading.timestamp:
            return False, "Missing timestamp"
        try:
            datetime.fromisoformat(reading.timestamp)
        except (ValueError, TypeError):
            return False, f"Invalid timestamp format: {reading.timestamp}"

        return True, ""

    def detect_anomaly(self, reading: HeartbeatReading) -> HeartbeatReading:
        """
        Check if a reading is anomalous and enrich it with anomaly flags.

        Args:
            reading: The HeartbeatReading to check.

        Returns:
            The same reading with is_anomaly and anomaly_type fields set.
        """
        if reading.heart_rate < self.low_threshold:
            reading.is_anomaly = True
            reading.anomaly_type = "LOW"
            self._anomaly_count += 1
            logger.warning(
                "LOW anomaly detected: customer=%s, hr=%d bpm",
                reading.customer_id,
                reading.heart_rate,
            )
        elif reading.heart_rate > self.high_threshold:
            reading.is_anomaly = True
            reading.anomaly_type = "HIGH"
            self._anomaly_count += 1
            logger.warning(
                "HIGH anomaly detected: customer=%s, hr=%d bpm",
                reading.customer_id,
                reading.heart_rate,
            )
        else:
            reading.is_anomaly = False
            reading.anomaly_type = None

        return reading

    def validate_and_enrich(
        self, reading: HeartbeatReading
    ) -> Tuple[bool, HeartbeatReading, str]:
        """
        Full validation pipeline: structural check + anomaly detection.

        Args:
            reading: The HeartbeatReading to process.

        Returns:
            Tuple of (is_valid, enriched_reading, error_message).
        """
        # Step 1: Structural validation
        is_valid, error_msg = self.validate_structure(reading)
        if not is_valid:
            self._rejected_count += 1
            logger.error("Validation failed for reading: %s — %s", reading, error_msg)
            return False, reading, error_msg

        # Step 2: Anomaly detection
        enriched = self.detect_anomaly(reading)

        self._validated_count += 1
        return True, enriched, ""

    def validate_batch(
        self, readings: List[HeartbeatReading]
    ) -> Tuple[List[HeartbeatReading], List[Tuple[HeartbeatReading, str]]]:
        """
        Validate a batch of readings.

        Args:
            readings: List of HeartbeatReading instances.

        Returns:
            Tuple of (valid_readings, rejected_readings_with_errors).
        """
        valid = []
        rejected = []

        for reading in readings:
            is_valid, enriched, error_msg = self.validate_and_enrich(reading)
            if is_valid:
                valid.append(enriched)
            else:
                rejected.append((reading, error_msg))

        if rejected:
            logger.warning(
                "Batch validation: %d valid, %d rejected",
                len(valid),
                len(rejected),
            )

        return valid, rejected

    @property
    def stats(self) -> dict:
        """Return validation statistics."""
        total = self._validated_count + self._rejected_count
        return {
            "total_processed": total,
            "validated": self._validated_count,
            "rejected": self._rejected_count,
            "anomalies_detected": self._anomaly_count,
            "rejection_rate": (
                round(self._rejected_count / total * 100, 2) if total > 0 else 0.0
            ),
            "anomaly_rate": (
                round(self._anomaly_count / self._validated_count * 100, 2)
                if self._validated_count > 0
                else 0.0
            ),
        }


def create_validator(**kwargs) -> HeartbeatValidator:
    """Factory function to create a HeartbeatValidator."""
    return HeartbeatValidator(**kwargs)
