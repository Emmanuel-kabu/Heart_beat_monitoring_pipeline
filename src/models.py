"""
Heartbeat Data Models

Defines the data structures used throughout the pipeline using dataclasses
for type safety and serialization support.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class HeartbeatReading:
    """
    Represents a single heart rate reading from a customer sensor.

    Attributes:
        customer_id: Unique identifier for the customer (e.g., 'CUST-001').
        heart_rate: Heart rate in beats per minute (bpm).
        timestamp: UTC timestamp when the reading was taken.
        is_anomaly: Whether this reading is flagged as anomalous.
        anomaly_type: Type of anomaly ('HIGH', 'LOW', or None).
    """

    customer_id: str
    heart_rate: int
    timestamp: str  # ISO 8601 format string
    is_anomaly: bool = False
    anomaly_type: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize to JSON string for Kafka messaging."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, data: dict) -> HeartbeatReading:
        """Create instance from dictionary."""
        return cls(
            customer_id=data["customer_id"],
            heart_rate=int(data["heart_rate"]),
            timestamp=data["timestamp"],
            is_anomaly=data.get("is_anomaly", False),
            anomaly_type=data.get("anomaly_type"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> HeartbeatReading:
        """Deserialize from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def __repr__(self) -> str:
        anomaly_str = f" [ANOMALY: {self.anomaly_type}]" if self.is_anomaly else ""
        return (
            f"HeartbeatReading(customer={self.customer_id}, "
            f"hr={self.heart_rate} bpm, "
            f"time={self.timestamp}{anomaly_str})"
        )


@dataclass
class CustomerProfile:
    """
    Represents a synthetic customer profile.

    Attributes:
        customer_id: Unique customer identifier.
        name: Customer full name.
        age: Customer age.
        resting_heart_rate: Baseline resting heart rate for this customer.
    """

    customer_id: str
    name: str
    age: int
    resting_heart_rate: int = 72  # Default resting HR

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
