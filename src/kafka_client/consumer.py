"""
Kafka Consumer Module

Consumes heartbeat readings from a Kafka topic, validates them,
and persists valid data to PostgreSQL. Supports graceful shutdown,
batch processing, and automatic offset management.
"""

from __future__ import annotations

import json
import signal
import time
from typing import Callable, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.config import config
from src.database.db_handler import DatabaseHandler
from src.logger import get_logger
from src.models import HeartbeatReading
from src.validation.validator import HeartbeatValidator

logger = get_logger("kafka.consumer")


class HeartbeatConsumer:
    """
    Kafka consumer for processing heartbeat readings.

    Reads messages from the configured Kafka topic, validates them,
    and inserts valid readings into PostgreSQL. Supports both
    single-message and batch processing modes.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str | None = None,
        db_handler: DatabaseHandler | None = None,
        validator: HeartbeatValidator | None = None,
        batch_size: int = 50,
        additional_config: dict | None = None,
    ):
        """
        Initialize the Kafka consumer.

        Args:
            bootstrap_servers: Kafka broker addresses. Defaults to config.
            topic: Kafka topic to subscribe to. Defaults to config.
            group_id: Consumer group ID. Defaults to config.
            db_handler: Database handler for persistence. Created if None.
            validator: Data validator. Created if None.
            batch_size: Number of messages to batch before DB insert.
            additional_config: Extra Kafka consumer configuration.
        """
        self._bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self._topic = topic or config.kafka.topic
        self._group_id = group_id or config.kafka.group_id
        self._consumer: Optional[Consumer] = None
        self._db_handler = db_handler
        self._validator = validator or HeartbeatValidator()
        self._batch_size = batch_size
        self._running = False
        self._consumed_count = 0
        self._processed_count = 0

        # Consumer configuration
        self._config = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": self._group_id,
            "auto.offset.reset": config.kafka.auto_offset_reset,
            "enable.auto.commit": False,  # Manual commit for exactly-once
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }
        if additional_config:
            self._config.update(additional_config)

    def connect(self) -> None:
        """Create the Kafka consumer and subscribe to the topic."""
        try:
            self._consumer = Consumer(self._config)
            self._consumer.subscribe(
                [self._topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            logger.info(
                "Kafka consumer connected to %s (topic: %s, group: %s)",
                self._bootstrap_servers,
                self._topic,
                self._group_id,
            )
        except KafkaException as e:
            logger.error("Failed to create Kafka consumer: %s", e)
            raise

    def _on_assign(self, consumer, partitions: List[TopicPartition]) -> None:
        """Callback when partitions are assigned to this consumer."""
        partition_list = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info("Partitions assigned: %s", ", ".join(partition_list))

    def _on_revoke(self, consumer, partitions: List[TopicPartition]) -> None:
        """Callback when partitions are revoked from this consumer."""
        partition_list = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info("Partitions revoked: %s", ", ".join(partition_list))

    def _deserialize_message(self, msg) -> Optional[HeartbeatReading]:
        """
        Deserialize a Kafka message into a HeartbeatReading.

        Args:
            msg: Kafka message object.

        Returns:
            HeartbeatReading instance or None if deserialization fails.
        """
        try:
            value = msg.value().decode("utf-8")
            data = json.loads(value)
            return HeartbeatReading.from_dict(data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(
                "Failed to deserialize message (offset=%s): %s",
                msg.offset(),
                e,
            )
            return None

    def _process_batch(self, batch: List[HeartbeatReading]) -> int:
        """
        Validate and persist a batch of readings.

        Args:
            batch: List of HeartbeatReading instances.

        Returns:
            Number of successfully persisted readings.
        """
        if not batch:
            return 0

        # Validate the batch
        valid_readings, rejected = self._validator.validate_batch(batch)

        if rejected:
            for reading, error in rejected:
                logger.warning("Rejected reading: %s — %s", reading.customer_id, error)

        # Persist valid readings
        if valid_readings and self._db_handler:
            try:
                inserted = self._db_handler.insert_readings_batch(valid_readings)
                self._processed_count += inserted
                return inserted
            except Exception as e:
                logger.error("Failed to persist batch: %s", e)
                return 0

        return len(valid_readings)

    def consume(
        self,
        max_messages: int | None = None,
        timeout: float = 1.0,
    ) -> None:
        """
        Start consuming messages from the Kafka topic.

        Processes messages in batches for efficiency. Supports graceful
        shutdown via SIGINT/SIGTERM signals.

        Args:
            max_messages: Maximum messages to consume (None = infinite).
            timeout: Poll timeout in seconds.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        self._running = True
        batch: List[HeartbeatReading] = []

        # Set up signal handlers for graceful shutdown
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def shutdown_handler(signum, frame):
            logger.info("Shutdown signal received, stopping consumer...")
            self._running = False

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        logger.info("Consumer started, listening for messages...")

        try:
            while self._running:
                msg = self._consumer.poll(timeout=timeout)

                if msg is None:
                    # No message available; flush any pending batch
                    if batch:
                        self._process_batch(batch)
                        self._consumer.commit(asynchronous=False)
                        batch.clear()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "End of partition: %s[%d] at offset %d",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                    else:
                        logger.error("Consumer error: %s", msg.error())
                    continue

                # Deserialize the message
                reading = self._deserialize_message(msg)
                if reading:
                    batch.append(reading)
                    self._consumed_count += 1

                # Process batch when it reaches the configured size
                if len(batch) >= self._batch_size:
                    self._process_batch(batch)
                    self._consumer.commit(asynchronous=False)
                    batch.clear()

                # Check if we've reached the message limit
                if max_messages and self._consumed_count >= max_messages:
                    logger.info("Reached max_messages limit (%d)", max_messages)
                    break

            # Process any remaining messages in the batch
            if batch:
                self._process_batch(batch)
                self._consumer.commit(asynchronous=False)

        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

            logger.info(
                "Consumer stopped (consumed=%d, processed=%d)",
                self._consumed_count,
                self._processed_count,
            )

    def close(self) -> None:
        """Gracefully shut down the consumer."""
        self._running = False
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer closed")
            self._consumer = None

    @property
    def stats(self) -> dict:
        """Return consumer statistics."""
        return {
            "consumed": self._consumed_count,
            "processed": self._processed_count,
            "validation_stats": self._validator.stats,
        }

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def create_consumer(**kwargs) -> HeartbeatConsumer:
    """Factory function to create and connect a HeartbeatConsumer."""
    consumer = HeartbeatConsumer(**kwargs)
    consumer.connect()
    return consumer
