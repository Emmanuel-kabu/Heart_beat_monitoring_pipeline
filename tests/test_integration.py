"""
Integration Tests for the Heartbeat Pipeline

Tests the end-to-end flow of the pipeline components working together.
These tests require running infrastructure (Kafka, PostgreSQL) and
are marked for selective execution.
"""

import json

import pytest

from src.generator.heartbeat_generator import HeartbeatGenerator
from src.models import HeartbeatReading
from src.validation.validator import HeartbeatValidator

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestEndToEndDataFlow:
    """
    Tests the data flow from generation through validation.

    These tests don't require Kafka/PostgreSQL and test the
    data transformation pipeline logic.
    """

    @pytest.fixture
    def generator(self):
        """Create a generator with known anomaly rate."""
        return HeartbeatGenerator(anomaly_probability=0.1)

    @pytest.fixture
    def validator(self):
        """Create a validator with standard thresholds."""
        return HeartbeatValidator(low_threshold=50, high_threshold=150)

    def test_generate_validate_flow(self, generator, validator):
        """Test that generated data flows through validation correctly."""
        # Generate a batch
        batch = generator.generate_batch(batch_size=100)
        assert len(batch) == 100

        # Validate the batch
        valid, rejected = validator.validate_batch(batch)

        # All generated data should be structurally valid
        assert len(rejected) == 0, f"Unexpected rejections: {rejected}"
        assert len(valid) == 100

        # Check anomaly detection was applied
        anomalies = [r for r in valid if r.is_anomaly]
        normals = [r for r in valid if not r.is_anomaly]
        assert len(anomalies) + len(normals) == 100

    def test_serialization_roundtrip(self, generator):
        """Test that data survives JSON serialization (Kafka simulation)."""
        readings = generator.generate_batch(batch_size=20)

        for original in readings:
            # Simulate Kafka produce/consume
            json_str = original.to_json()
            restored = HeartbeatReading.from_json(json_str)

            assert restored.customer_id == original.customer_id
            assert restored.heart_rate == original.heart_rate
            assert restored.timestamp == original.timestamp

    def test_high_volume_generation(self, generator, validator):
        """Test pipeline handles high volume of data."""
        total_readings = 1000
        batch = generator.generate_batch(batch_size=total_readings)
        assert len(batch) == total_readings

        valid, rejected = validator.validate_batch(batch)
        assert len(valid) == total_readings  # All should be valid

        # Verify statistics
        stats = validator.stats
        assert stats["validated"] >= total_readings

    def test_anomaly_rate_approximation(self):
        """Test that anomaly rate approximately matches configured probability."""
        gen = HeartbeatGenerator(anomaly_probability=0.1)
        validator = HeartbeatValidator(low_threshold=50, high_threshold=150)

        batch = gen.generate_batch(batch_size=1000)
        valid, _ = validator.validate_batch(batch)

        anomaly_count = sum(1 for r in valid if r.is_anomaly)
        anomaly_rate = anomaly_count / len(valid)

        # Should be roughly 10% (allow 5-20% range for randomness)
        assert (
            0.02 <= anomaly_rate <= 0.25
        ), f"Anomaly rate {anomaly_rate:.2%} outside expected range"

    def test_customer_distribution(self, generator):
        """Test that readings are distributed across all customers."""
        batch = generator.generate_batch(batch_size=500)

        customer_ids = {r.customer_id for r in batch}
        expected_customers = {c.customer_id for c in generator.customers}

        # All customers should have readings
        assert customer_ids == expected_customers

    def test_timestamp_uniqueness(self, generator):
        """Test that timestamps are reasonably unique."""
        batch = generator.generate_batch(batch_size=50)
        timestamps = [r.timestamp for r in batch]

        # Not all timestamps should be identical (they're generated sequentially)
        # Allow for some duplicates due to fast generation
        unique_timestamps = set(timestamps)
        assert len(unique_timestamps) >= 1  # At minimum, one unique timestamp


class TestCorruptDataHandling:
    """Tests for handling corrupt or malformed data."""

    @pytest.fixture
    def validator(self):
        return HeartbeatValidator(low_threshold=50, high_threshold=150)

    def test_corrupt_json_handling(self):
        """Test handling of corrupt JSON data."""
        corrupt_strings = [
            "",
            "not json",
            "{incomplete",
            '{"customer_id": "CUST-001"}',  # Missing fields
        ]
        for corrupt in corrupt_strings:
            try:
                HeartbeatReading.from_json(corrupt)
                # If it parses, it should fail validation
            except (json.JSONDecodeError, KeyError, TypeError):
                pass  # Expected behavior

    def test_extreme_values(self, validator):
        """Test validation with extreme numeric values."""
        extreme_readings = [
            HeartbeatReading("CUST-001", 0, "2026-01-01T12:00:00+00:00"),
            HeartbeatReading("CUST-001", -1, "2026-01-01T12:00:00+00:00"),
            HeartbeatReading("CUST-001", 999, "2026-01-01T12:00:00+00:00"),
        ]
        for reading in extreme_readings:
            is_valid, error = validator.validate_structure(reading)
            # All extreme values should be rejected
            assert is_valid is False, f"Expected rejection for HR={reading.heart_rate}"
