"""
Pytest Configuration

Defines markers, fixtures, and settings for the test suite.
"""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may need infrastructure)"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow-running"
    )


@pytest.fixture(scope="session")
def sample_readings():
    """Provide a set of sample readings for testing."""
    from src.models import HeartbeatReading

    return [
        HeartbeatReading("CUST-001", 72, "2026-01-01T12:00:00+00:00"),
        HeartbeatReading("CUST-002", 85, "2026-01-01T12:00:01+00:00"),
        HeartbeatReading("CUST-003", 45, "2026-01-01T12:00:02+00:00", True, "LOW"),
        HeartbeatReading("CUST-004", 160, "2026-01-01T12:00:03+00:00", True, "HIGH"),
        HeartbeatReading("CUST-005", 90, "2026-01-01T12:00:04+00:00"),
    ]
