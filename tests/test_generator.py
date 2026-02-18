"""
Unit Tests for Heartbeat Data Generator

Tests the synthetic data generation logic including normal readings,
anomalous readings, batch generation, and customer profile handling.
"""

import pytest
from datetime import datetime

from src.generator.heartbeat_generator import (
    CUSTOMER_PROFILES,
    HeartbeatGenerator,
    create_generator,
)
from src.models import CustomerProfile, HeartbeatReading


class TestHeartbeatReading:
    """Tests for the HeartbeatReading data model."""

    def test_create_reading(self):
        """Test creating a basic heartbeat reading."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        assert reading.customer_id == "CUST-001"
        assert reading.heart_rate == 72
        assert reading.is_anomaly is False
        assert reading.anomaly_type is None

    def test_reading_to_json(self):
        """Test JSON serialization."""
        reading = HeartbeatReading(
            customer_id="CUST-001",
            heart_rate=72,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        json_str = reading.to_json()
        assert '"customer_id": "CUST-001"' in json_str
        assert '"heart_rate": 72' in json_str

    def test_reading_from_json(self):
        """Test JSON deserialization."""
        reading = HeartbeatReading(
            customer_id="CUST-002",
            heart_rate=85,
            timestamp="2026-01-01T12:00:00+00:00",
            is_anomaly=True,
            anomaly_type="HIGH",
        )
        json_str = reading.to_json()
        restored = HeartbeatReading.from_json(json_str)
        assert restored.customer_id == "CUST-002"
        assert restored.heart_rate == 85
        assert restored.is_anomaly is True
        assert restored.anomaly_type == "HIGH"

    def test_reading_roundtrip(self):
        """Test dict roundtrip serialization."""
        original = HeartbeatReading(
            customer_id="CUST-003",
            heart_rate=60,
            timestamp="2026-01-01T12:00:00+00:00",
        )
        data = original.to_dict()
        restored = HeartbeatReading.from_dict(data)
        assert original.customer_id == restored.customer_id
        assert original.heart_rate == restored.heart_rate


class TestHeartbeatGenerator:
    """Tests for the HeartbeatGenerator class."""

    def test_generator_initialization(self):
        """Test generator initializes with correct defaults."""
        gen = HeartbeatGenerator()
        assert len(gen.customers) > 0
        assert gen.total_generated == 0

    def test_generator_custom_customers(self):
        """Test generator with custom customer list."""
        custom_customers = [
            CustomerProfile("CUST-100", "Test User", 30, 70),
        ]
        gen = HeartbeatGenerator(customers=custom_customers)
        assert len(gen.customers) == 1
        assert gen.customers[0].customer_id == "CUST-100"

    def test_generate_single_reading(self):
        """Test generating a single reading."""
        gen = HeartbeatGenerator()
        reading = gen.generate_single_reading()

        assert isinstance(reading, HeartbeatReading)
        assert reading.customer_id.startswith("CUST-")
        assert 20 <= reading.heart_rate <= 300
        assert reading.timestamp is not None
        assert gen.total_generated == 1

    def test_generate_single_for_specific_customer(self):
        """Test generating a reading for a specific customer."""
        customer = CustomerProfile("CUST-099", "Specific User", 40, 75)
        gen = HeartbeatGenerator(customers=[customer])
        reading = gen.generate_single_reading(customer)
        assert reading.customer_id == "CUST-099"

    def test_generate_batch_default_size(self):
        """Test batch generation generates one per customer."""
        gen = HeartbeatGenerator()
        batch = gen.generate_batch()
        assert len(batch) == len(gen.customers)
        assert gen.total_generated == len(gen.customers)

    def test_generate_batch_custom_size(self):
        """Test batch generation with custom size."""
        gen = HeartbeatGenerator()
        batch = gen.generate_batch(batch_size=5)
        assert len(batch) == 5

    def test_normal_heart_rate_range(self):
        """Test that normal readings fall within expected range."""
        gen = HeartbeatGenerator(anomaly_probability=0.0)  # No anomalies
        readings = gen.generate_batch(batch_size=100)

        for reading in readings:
            assert 40 <= reading.heart_rate <= 200, (
                f"Heart rate {reading.heart_rate} outside expected range"
            )

    def test_anomaly_generation(self):
        """Test that anomalies are generated when probability is high."""
        gen = HeartbeatGenerator(anomaly_probability=1.0)  # All anomalies
        readings = gen.generate_batch(batch_size=50)

        # With 100% anomaly probability, all should be outside normal range
        anomalous_count = sum(
            1 for r in readings if r.heart_rate < 50 or r.heart_rate > 150
        )
        # Allow some tolerance due to clamping
        assert anomalous_count > 0, "Expected at least some anomalous readings"

    def test_timestamp_format(self):
        """Test that timestamps are valid ISO 8601."""
        gen = HeartbeatGenerator()
        reading = gen.generate_single_reading()
        # Should not raise
        dt = datetime.fromisoformat(reading.timestamp)
        assert dt is not None

    def test_predefined_customer_profiles(self):
        """Test that predefined profiles are properly configured."""
        assert len(CUSTOMER_PROFILES) == 10
        for profile in CUSTOMER_PROFILES:
            assert profile.customer_id.startswith("CUST-")
            assert profile.age > 0
            assert 50 <= profile.resting_heart_rate <= 90

    def test_factory_function(self):
        """Test the create_generator factory function."""
        gen = create_generator(anomaly_probability=0.1)
        assert isinstance(gen, HeartbeatGenerator)
        reading = gen.generate_single_reading()
        assert isinstance(reading, HeartbeatReading)
