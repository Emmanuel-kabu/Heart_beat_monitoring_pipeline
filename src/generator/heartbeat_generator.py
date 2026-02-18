"""
Synthetic Heartbeat Data Generator

Generates realistic heart rate data for multiple simulated customers.
Uses weighted random distributions to produce mostly normal readings
with occasional anomalies, mimicking real-world sensor behavior.
"""

import random
import time
from datetime import datetime, timezone
from typing import Generator, List

from src.config import config
from src.logger import get_logger
from src.models import CustomerProfile, HeartbeatReading

logger = get_logger("generator")

# Pre-defined customer profiles matching the SQL seed data
CUSTOMER_PROFILES: List[CustomerProfile] = [
    CustomerProfile("CUST-001", "Alice Johnson", 32, 68),
    CustomerProfile("CUST-002", "Bob Smith", 45, 74),
    CustomerProfile("CUST-003", "Charlie Brown", 28, 65),
    CustomerProfile("CUST-004", "Diana Prince", 55, 78),
    CustomerProfile("CUST-005", "Edward Norton", 38, 70),
    CustomerProfile("CUST-006", "Fiona Apple", 41, 72),
    CustomerProfile("CUST-007", "George Lucas", 67, 80),
    CustomerProfile("CUST-008", "Hannah Montana", 23, 62),
    CustomerProfile("CUST-009", "Ivan Drago", 50, 76),
    CustomerProfile("CUST-010", "Julia Roberts", 36, 69),
]


class HeartbeatGenerator:
    """
    Generates synthetic heartbeat sensor data.

    Uses weighted probability distributions centered around each
    customer's resting heart rate to produce realistic data with
    occasional anomalies (approximately 5% of readings).
    """

    def __init__(
        self,
        customers: List[CustomerProfile] | None = None,
        anomaly_probability: float = 0.05,
    ):
        """
        Initialize the heartbeat generator.

        Args:
            customers: List of customer profiles. Defaults to predefined list.
            anomaly_probability: Probability of generating an anomalous reading (0.0 to 1.0).
        """
        self.customers = (customers or CUSTOMER_PROFILES)[: config.generator.num_customers]
        self.anomaly_probability = anomaly_probability
        self._generation_count = 0

        logger.info(
            "HeartbeatGenerator initialized with %d customers, "
            "anomaly_probability=%.2f",
            len(self.customers),
            self.anomaly_probability,
        )

    def _generate_normal_heart_rate(self, customer: CustomerProfile) -> int:
        """
        Generate a normal heart rate based on customer's resting HR.

        Uses a Gaussian distribution centered on the resting heart rate
        with standard deviation based on age (older = slightly more variable).

        Args:
            customer: The customer profile to generate data for.

        Returns:
            A realistic heart rate value in the normal range.
        """
        # Standard deviation increases slightly with age
        std_dev = 8 + (customer.age / 20)
        heart_rate = int(random.gauss(customer.resting_heart_rate, std_dev))

        # Clamp to configured normal range
        return max(
            config.generator.normal_hr_min,
            min(config.generator.normal_hr_max, heart_rate),
        )

    def _generate_anomalous_heart_rate(self) -> int:
        """
        Generate an anomalous heart rate (either very high or very low).

        Returns:
            An anomalous heart rate value outside normal thresholds.
        """
        if random.random() < 0.5:
            # Low anomaly: 35–49 bpm
            return random.randint(
                config.generator.heart_rate_min,
                config.anomaly.low_threshold - 1,
            )
        else:
            # High anomaly: 151–200 bpm
            return random.randint(
                config.anomaly.high_threshold + 1,
                config.generator.heart_rate_max,
            )

    def generate_single_reading(
        self, customer: CustomerProfile | None = None
    ) -> HeartbeatReading:
        """
        Generate a single heartbeat reading for a customer.

        Args:
            customer: Specific customer profile. If None, picks random customer.

        Returns:
            A HeartbeatReading instance.
        """
        if customer is None:
            customer = random.choice(self.customers)

        # Decide if this reading should be anomalous
        is_anomaly = random.random() < self.anomaly_probability

        if is_anomaly:
            heart_rate = self._generate_anomalous_heart_rate()
        else:
            heart_rate = self._generate_normal_heart_rate(customer)

        timestamp = datetime.now(timezone.utc).isoformat()

        reading = HeartbeatReading(
            customer_id=customer.customer_id,
            heart_rate=heart_rate,
            timestamp=timestamp,
        )

        self._generation_count += 1
        return reading

    def generate_batch(self, batch_size: int | None = None) -> List[HeartbeatReading]:
        """
        Generate a batch of readings, one per customer by default.

        Args:
            batch_size: Number of readings to generate. Defaults to one per customer.

        Returns:
            List of HeartbeatReading instances.
        """
        if batch_size is None:
            # One reading per customer
            return [self.generate_single_reading(c) for c in self.customers]
        else:
            return [self.generate_single_reading() for _ in range(batch_size)]

    def stream_readings(
        self, interval: float | None = None
    ) -> Generator[HeartbeatReading, None, None]:
        """
        Continuously generate heartbeat readings as a stream.

        Yields one reading per customer per interval cycle.

        Args:
            interval: Seconds between generation cycles. Defaults to config value.

        Yields:
            HeartbeatReading instances indefinitely.
        """
        interval = interval or config.generator.interval_sec
        logger.info("Starting heartbeat stream (interval=%.1fs)", interval)

        cycle = 0
        while True:
            cycle += 1
            batch = self.generate_batch()
            for reading in batch:
                yield reading

            if cycle % 100 == 0:
                logger.info(
                    "Generator cycle %d complete | Total readings: %d",
                    cycle,
                    self._generation_count,
                )

            time.sleep(interval)

    @property
    def total_generated(self) -> int:
        """Total number of readings generated."""
        return self._generation_count


def create_generator(**kwargs) -> HeartbeatGenerator:
    """Factory function to create a HeartbeatGenerator with default settings."""
    return HeartbeatGenerator(**kwargs)


# ─── Standalone execution for testing ──────────────────────────────────────
if __name__ == "__main__":
    from src.logger import setup_logging

    setup_logging()
    generator = create_generator()

    print("Generating 5 sample heartbeat readings:")
    print("-" * 60)
    for reading in generator.generate_batch(5):
        print(reading)
    print("-" * 60)
    print(f"Total generated: {generator.total_generated}")
