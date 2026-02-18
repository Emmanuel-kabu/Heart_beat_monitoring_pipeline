"""
Kafka Producer Module

Sends heartbeat readings to a Kafka topic for real-time streaming.
Implements serialization, error handling, delivery callbacks,
and graceful shutdown.
"""

from __future__ import annotations

import json
import time
from typing import Callable, Optional

from confluent_kafka import KafkaError, KafkaException, Producer

from src.config import config
from src.logger import get_logger
from src.models import HeartbeatReading

logger = get_logger("kafka.producer")


class HeartbeatProducer:
    """
    Kafka producer for streaming heartbeat readings.

    Wraps the confluent_kafka Producer with heartbeat-specific
    serialization, delivery callbacks, and health monitoring.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        additional_config: dict | None = None,
    ):
        """
        Initialize the Kafka producer.

        Args:
            bootstrap_servers: Kafka broker addresses. Defaults to config.
            topic: Target Kafka topic. Defaults to config.
            additional_config: Extra Kafka producer configuration.
        """
        self._bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self._topic = topic or config.kafka.topic
        self._producer: Optional[Producer] = None
        self._delivered_count = 0
        self._failed_count = 0

        # Producer configuration
        self._config = {
            "bootstrap.servers": self._bootstrap_servers,
            "client.id": "heartbeat-producer",
            "acks": "all",  # Wait for all replicas
            "retries": 3,
            "retry.backoff.ms": 500,
            "linger.ms": 10,  # Small batch window for throughput
            "batch.size": 16384,
            "compression.type": "gzip",
            "enable.idempotence": True,  # Exactly-once semantics
        }
        if additional_config:
            self._config.update(additional_config)

    def connect(self) -> None:
        """Create the Kafka producer instance."""
        try:
            self._producer = Producer(self._config)
            logger.info(
                "Kafka producer connected to %s (topic: %s)",
                self._bootstrap_servers,
                self._topic,
            )
        except KafkaException as e:
            logger.error("Failed to create Kafka producer: %s", e)
            raise

    def _delivery_callback(self, err, msg) -> None:
        """
        Callback invoked when a message is delivered (or fails).

        Args:
            err: Error object if delivery failed, None on success.
            msg: The message that was produced.
        """
        if err:
            self._failed_count += 1
            logger.error(
                "Message delivery failed: topic=%s, partition=%s, error=%s",
                msg.topic(),
                msg.partition(),
                err,
            )
        else:
            self._delivered_count += 1
            if self._delivered_count % 500 == 0:
                logger.info(
                    "Messages delivered: %d (topic=%s, partition=%s)",
                    self._delivered_count,
                    msg.topic(),
                    msg.partition(),
                )

    def send(
        self,
        reading: HeartbeatReading,
        topic: str | None = None,
        callback: Callable | None = None,
    ) -> None:
        """
        Send a single heartbeat reading to Kafka.

        Args:
            reading: HeartbeatReading to send.
            topic: Override topic. Defaults to configured topic.
            callback: Override delivery callback.
        """
        if not self._producer:
            raise RuntimeError("Producer not connected. Call connect() first.")

        target_topic = topic or self._topic
        message_key = reading.customer_id.encode("utf-8")
        message_value = reading.to_json().encode("utf-8")

        try:
            self._producer.produce(
                topic=target_topic,
                key=message_key,
                value=message_value,
                callback=callback or self._delivery_callback,
            )
            # Trigger delivery callbacks for already-delivered messages
            self._producer.poll(0)

        except BufferError:
            logger.warning("Producer buffer full, waiting for deliveries...")
            self._producer.flush(timeout=5)
            # Retry after flush
            self._producer.produce(
                topic=target_topic,
                key=message_key,
                value=message_value,
                callback=callback or self._delivery_callback,
            )
        except KafkaException as e:
            logger.error("Failed to produce message: %s", e)
            raise

    def send_batch(self, readings: list[HeartbeatReading]) -> int:
        """
        Send a batch of heartbeat readings to Kafka.

        Args:
            readings: List of HeartbeatReading instances.

        Returns:
            Number of messages queued for delivery.
        """
        queued = 0
        for reading in readings:
            self.send(reading)
            queued += 1

        # Ensure all messages in the batch are delivered
        self._producer.flush(timeout=10)

        logger.debug("Batch of %d messages queued for delivery", queued)
        return queued

    def flush(self, timeout: float = 30) -> int:
        """
        Wait for all outstanding messages to be delivered.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            Number of messages still in the queue.
        """
        if self._producer:
            remaining = self._producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning("%d messages still in queue after flush", remaining)
            return remaining
        return 0

    def close(self) -> None:
        """Gracefully shut down the producer."""
        if self._producer:
            remaining = self.flush(timeout=30)
            if remaining > 0:
                logger.warning(
                    "Producer closing with %d undelivered messages", remaining
                )
            logger.info(
                "Kafka producer closed (delivered=%d, failed=%d)",
                self._delivered_count,
                self._failed_count,
            )
            self._producer = None

    @property
    def stats(self) -> dict:
        """Return producer statistics."""
        return {
            "delivered": self._delivered_count,
            "failed": self._failed_count,
            "total_attempted": self._delivered_count + self._failed_count,
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def create_producer(**kwargs) -> HeartbeatProducer:
    """Factory function to create and connect a HeartbeatProducer."""
    producer = HeartbeatProducer(**kwargs)
    producer.connect()
    return producer
