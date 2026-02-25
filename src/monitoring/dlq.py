"""
Dead Letter Queue (DLQ) Module

Messages that fail validation, deserialization, or DB persistence
are routed to a DLQ Kafka topic for later inspection and retry.

Features:
    - Automatic routing of failed messages with failure metadata
    - Configurable retry with exponential backoff
    - DLQ consumer for reprocessing / inspection
    - Prometheus metrics integration
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from src.config import config
from src.logger import get_logger
from src.monitoring.metrics import DLQ_MESSAGES_TOTAL, DLQ_RETRIES_TOTAL, DLQ_RETRY_SUCCESS

logger = get_logger("kafka.dlq")

# Default DLQ topic name
DLQ_TOPIC = f"{config.kafka.topic}_dlq"


class DeadLetterQueueProducer:
    """
    Produces failed messages to the Dead Letter Queue topic.

    Enriches each message with failure metadata (reason, timestamp,
    original topic/partition/offset, retry count).
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        dlq_topic: str | None = None,
    ):
        self._bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self._dlq_topic = dlq_topic or DLQ_TOPIC
        self._producer: Optional[Producer] = None
        self._sent_count = 0

    def connect(self) -> None:
        """Create the DLQ Kafka producer."""
        try:
            self._producer = Producer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "client.id": "heartbeat-dlq-producer",
                    "acks": "all",
                    "retries": 5,
                    "compression.type": "gzip",
                }
            )
            logger.info("DLQ producer connected (topic: %s)", self._dlq_topic)
        except KafkaException as e:
            logger.error("Failed to create DLQ producer: %s", e)
            raise

    def send_to_dlq(
        self,
        original_value: bytes | str,
        reason: str,
        original_topic: str | None = None,
        original_partition: int | None = None,
        original_offset: int | None = None,
        original_key: bytes | None = None,
        retry_count: int = 0,
        extra_metadata: dict | None = None,
    ) -> None:
        """
        Send a failed message to the DLQ topic with enriched metadata.

        Args:
            original_value: The original message payload.
            reason: Human-readable failure reason.
            original_topic: Source topic.
            original_partition: Source partition.
            original_offset: Source offset.
            original_key: Source message key.
            retry_count: How many times this message has been retried.
            extra_metadata: Any additional context.
        """
        if not self._producer:
            raise RuntimeError("DLQ producer not connected. Call connect() first.")

        if isinstance(original_value, bytes):
            try:
                original_value_str = original_value.decode("utf-8")
            except UnicodeDecodeError:
                original_value_str = original_value.hex()
        else:
            original_value_str = original_value

        dlq_envelope = {
            "original_value": original_value_str,
            "failure_reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "original_topic": original_topic or config.kafka.topic,
            "original_partition": original_partition,
            "original_offset": original_offset,
            "retry_count": retry_count,
            "metadata": extra_metadata or {},
        }

        try:
            self._producer.produce(
                topic=self._dlq_topic,
                key=original_key,
                value=json.dumps(dlq_envelope).encode("utf-8"),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)

            DLQ_MESSAGES_TOTAL.labels(reason=reason).inc()
            self._sent_count += 1

            logger.warning(
                "Message sent to DLQ: reason='%s', topic=%s, partition=%s, offset=%s",
                reason,
                original_topic,
                original_partition,
                original_offset,
            )
        except (KafkaException, BufferError) as e:
            logger.error("Failed to send message to DLQ: %s", e)

    def _delivery_callback(self, err, msg) -> None:
        if err:
            logger.error("DLQ delivery failed: %s", err)
        else:
            logger.debug(
                "DLQ message delivered to %s[%d]@%d", msg.topic(), msg.partition(), msg.offset()
            )

    def flush(self, timeout: float = 10) -> None:
        if self._producer:
            self._producer.flush(timeout=timeout)

    def close(self) -> None:
        if self._producer:
            self.flush()
            self._producer = None
            logger.info("DLQ producer closed (sent=%d)", self._sent_count)

    @property
    def sent_count(self) -> int:
        return self._sent_count


class DeadLetterQueueConsumer:
    """
    Consumes messages from the DLQ topic for reprocessing or inspection.

    Supports:
        - Manual review via iterate()
        - Automatic retry with a configurable handler callback
        - Exponential backoff between retries
    """

    MAX_RETRIES = 3
    BACKOFF_BASE = 2  # seconds

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        dlq_topic: str | None = None,
        group_id: str = "heartbeat_dlq_consumer_group",
    ):
        self._bootstrap_servers = bootstrap_servers or config.kafka.bootstrap_servers
        self._dlq_topic = dlq_topic or DLQ_TOPIC
        self._group_id = group_id
        self._consumer: Optional[Consumer] = None
        self._running = False

    def connect(self) -> None:
        """Create and subscribe the DLQ consumer."""
        try:
            self._consumer = Consumer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "group.id": self._group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            self._consumer.subscribe([self._dlq_topic])
            logger.info("DLQ consumer connected (topic: %s)", self._dlq_topic)
        except KafkaException as e:
            logger.error("Failed to create DLQ consumer: %s", e)
            raise

    def iterate(self, max_messages: int = 100, timeout: float = 2.0):
        """
        Yield DLQ messages for manual inspection.

        Yields:
            dict: The DLQ envelope (original_value, failure_reason, etc.)
        """
        if not self._consumer:
            raise RuntimeError("DLQ consumer not connected.")

        count = 0
        while count < max_messages:
            msg = self._consumer.poll(timeout=timeout)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.error("DLQ consumer error: %s", msg.error())
                continue

            try:
                envelope = json.loads(msg.value().decode("utf-8"))
                yield envelope
                count += 1
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("Failed to decode DLQ message: %s", e)

        self._consumer.commit(asynchronous=False)

    def retry_with_handler(
        self,
        handler: Callable[[dict], bool],
        max_messages: int = 50,
        dlq_producer: DeadLetterQueueProducer | None = None,
    ) -> Dict[str, int]:
        """
        Attempt to reprocess DLQ messages using a handler function.

        The handler receives the *original_value* (parsed as dict)
        and must return True on success, False on failure.

        Messages that fail again (up to MAX_RETRIES) are re-sent
        to the DLQ with an incremented retry_count.

        Args:
            handler: Callable that processes the original message.
            max_messages: Max messages to process in one run.
            dlq_producer: DLQ producer for re-queuing failures.

        Returns:
            Dict with 'success', 'failed', 'exhausted' counts.
        """
        stats = {"success": 0, "failed": 0, "exhausted": 0}

        for envelope in self.iterate(max_messages=max_messages):
            retry_count = envelope.get("retry_count", 0)
            original_value_str = envelope.get("original_value", "")

            DLQ_RETRIES_TOTAL.inc()

            # Parse original value
            try:
                original_data = json.loads(original_value_str)
            except (json.JSONDecodeError, TypeError):
                original_data = {"raw": original_value_str}

            # Exponential backoff
            if retry_count > 0:
                backoff = min(self.BACKOFF_BASE**retry_count, 30)
                time.sleep(backoff)

            # Attempt reprocessing
            try:
                success = handler(original_data)
            except Exception as e:
                logger.error("DLQ retry handler error: %s", e)
                success = False

            if success:
                stats["success"] += 1
                DLQ_RETRY_SUCCESS.inc()
                logger.info("DLQ message reprocessed successfully (retry #%d)", retry_count + 1)
            else:
                if retry_count + 1 >= self.MAX_RETRIES:
                    stats["exhausted"] += 1
                    logger.error(
                        "DLQ message exhausted max retries (%d): %s",
                        self.MAX_RETRIES,
                        envelope.get("failure_reason"),
                    )
                else:
                    stats["failed"] += 1
                    if dlq_producer:
                        dlq_producer.send_to_dlq(
                            original_value=original_value_str,
                            reason=envelope.get("failure_reason", "retry_failed"),
                            original_topic=envelope.get("original_topic"),
                            original_partition=envelope.get("original_partition"),
                            original_offset=envelope.get("original_offset"),
                            retry_count=retry_count + 1,
                        )

        return stats

    def close(self) -> None:
        if self._consumer:
            self._consumer.close()
            logger.info("DLQ consumer closed")
            self._consumer = None
