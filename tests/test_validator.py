"""
Unit Tests for Heartbeat Data Validator

Tests the validation logic including structural validation,
anomaly detection, batch processing, and edge cases.
"""

import pytest

from src.models import HeartbeatReading
from src.validation.validator import HeartbeatValidator, create_validator


class TestStructuralValidation:
    """Tests for structural validation of heartbeat readings."""

    @pytest.fixture
    def validator(self):
        """Create a validator instance for testing."""
        return HeartbeatValidator(low_threshold=50, high_threshold=150)

    @pytest.fixture
    def valid_reading(self):
        """Create a valid heartbeat reading for testing."""
        return HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )

    def test_valid_reading_passes(self, validator, valid_reading):
        """Test that a valid reading passes structural validation."""
        is_valid, error = validator.validate_structure(valid_reading)
        assert is_valid is True
        assert error == ""

    def test_missing_customer_id(self, validator):
        """Test rejection of empty customer_id."""
        reading = HeartbeatReading(
            customer_id="",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is False
        assert "customer_id" in error.lower()

    def test_invalid_customer_id_format(self, validator):
        """Test rejection of invalid customer_id format."""
        reading = HeartbeatReading(
            customer_id="INVALID-001",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is False
        assert "format" in error.lower()

    def test_heart_rate_too_low(self, validator):
        """Test rejection of physiologically impossible low HR."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=10,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is False
        assert "minimum" in error.lower()

    def test_heart_rate_too_high(self, validator):
        """Test rejection of physiologically impossible high HR."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=350,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is False
        assert "maximum" in error.lower()

    def test_missing_timestamp(self, validator):
        """Test rejection of empty timestamp."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is False
        assert "timestamp" in error.lower()

    def test_invalid_timestamp_format(self, validator):
        """Test rejection of invalid timestamp format."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="not-a-timestamp",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is False
        assert "timestamp" in error.lower()

    def test_boundary_heart_rate_min(self, validator):
        """Test minimum valid heart rate (boundary)."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=20,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is True

    def test_boundary_heart_rate_max(self, validator):
        """Test maximum valid heart rate (boundary)."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=300,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, error = validator.validate_structure(reading)
        assert is_valid is True


class TestAnomalyDetection:
    """Tests for anomaly detection logic."""

    @pytest.fixture
    def validator(self):
        return HeartbeatValidator(low_threshold=50, high_threshold=150)

    def test_normal_reading_not_anomaly(self, validator):
        """Test that normal heart rate is not flagged as anomaly."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is False
        assert result.anomaly_type is None

    def test_low_anomaly_detected(self, validator):
        """Test detection of low heart rate anomaly."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=45,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is True
        assert result.anomaly_type == "LOW"

    def test_high_anomaly_detected(self, validator):
        """Test detection of high heart rate anomaly."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=160,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is True
        assert result.anomaly_type == "HIGH"

    def test_boundary_low_threshold(self, validator):
        """Test exact low threshold is anomalous."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=49,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is True
        assert result.anomaly_type == "LOW"

    def test_boundary_at_low_threshold(self, validator):
        """Test exactly at low threshold is not anomalous."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=50,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is False

    def test_boundary_high_threshold(self, validator):
        """Test exact high threshold + 1 is anomalous."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=151,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is True
        assert result.anomaly_type == "HIGH"

    def test_boundary_at_high_threshold(self, validator):
        """Test exactly at high threshold is not anomalous."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=150,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        result = validator.detect_anomaly(reading)
        assert result.is_anomaly is False


class TestValidateAndEnrich:
    """Tests for the combined validation and enrichment pipeline."""

    @pytest.fixture
    def validator(self):
        return HeartbeatValidator(low_threshold=50, high_threshold=150)

    def test_valid_normal_reading(self, validator):
        """Test a valid normal reading passes and is enriched."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, enriched, error = validator.validate_and_enrich(reading)
        assert is_valid is True
        assert error == ""
        assert enriched.is_anomaly is False

    def test_valid_anomalous_reading(self, validator):
        """Test a valid anomalous reading passes and is flagged."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=180,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, enriched, error = validator.validate_and_enrich(reading)
        assert is_valid is True
        assert enriched.is_anomaly is True
        assert enriched.anomaly_type == "HIGH"

    def test_invalid_reading_rejected(self, validator):
        """Test an invalid reading is rejected before anomaly check."""
        reading = HeartbeatReading(
            customer_id="",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        is_valid, _, error = validator.validate_and_enrich(reading)
        assert is_valid is False
        assert error != ""


class TestBatchValidation:
    """Tests for batch validation."""

    @pytest.fixture
    def validator(self):
        return HeartbeatValidator(low_threshold=50, high_threshold=150)

    def test_batch_all_valid(self, validator):
        """Test batch with all valid readings."""
        readings = [
            HeartbeatReading("CUST-001", 72, "2026-01-01T12:00:00+00:00"),
            HeartbeatReading("CUST-002", 80, "2026-01-01T12:00:01+00:00"),
            HeartbeatReading("CUST-003", 65, "2026-01-01T12:00:02+00:00"),
        ]
        valid, rejected = validator.validate_batch(readings)
        assert len(valid) == 3
        assert len(rejected) == 0

    def test_batch_mixed(self, validator):
        """Test batch with mix of valid and invalid readings."""
        readings = [
            HeartbeatReading("CUST-001", 72, "2026-01-01T12:00:00+00:00"),
            HeartbeatReading("", 72, "2026-01-01T12:00:01+00:00"),  # Invalid
            HeartbeatReading("CUST-003", 160, "2026-01-01T12:00:02+00:00"),  # Anomaly
        ]
        valid, rejected = validator.validate_batch(readings)
        assert len(valid) == 2
        assert len(rejected) == 1
        assert valid[1].is_anomaly is True

    def test_batch_empty(self, validator):
        """Test empty batch."""
        valid, rejected = validator.validate_batch([])
        assert len(valid) == 0
        assert len(rejected) == 0


class TestValidatorStats:
    """Tests for validator statistics."""

    def test_stats_initial(self):
        """Test initial stats are zeroed."""
        validator = HeartbeatValidator()
        stats = validator.stats
        assert stats["total_processed"] == 0
        assert stats["validated"] == 0
        assert stats["rejected"] == 0

    def test_stats_after_processing(self):
        """Test stats update after processing."""
        validator = HeartbeatValidator(low_threshold=50, high_threshold=150)
        readings = [
            HeartbeatReading("CUST-001", 72, "2026-01-01T12:00:00+00:00"),
            HeartbeatReading("", 72, "2026-01-01T12:00:01+00:00"),  # Invalid
            HeartbeatReading("CUST-003", 160, "2026-01-01T12:00:02+00:00"),  # Anomaly
        ]
        validator.validate_batch(readings)

        stats = validator.stats
        assert stats["total_processed"] == 3
        assert stats["validated"] == 2
        assert stats["rejected"] == 1
        assert stats["anomalies_detected"] == 1

    def test_factory_function(self):
        """Test the create_validator factory function."""
        validator = create_validator(low_threshold=40, high_threshold=160)
        assert isinstance(validator, HeartbeatValidator)
        assert validator.low_threshold == 40
        assert validator.high_threshold == 160
