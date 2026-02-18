"""
Main Pipeline Orchestrator

Coordinates all pipeline components: data generation, Kafka streaming,
validation, and database persistence. Supports running individual
components or the full pipeline.
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
from typing import Optional

from src.config import config
from src.database.db_handler import DatabaseHandler, create_db_handler
from src.generator.heartbeat_generator import HeartbeatGenerator, create_generator
from src.kafka_client.consumer import HeartbeatConsumer, create_consumer
from src.kafka_client.producer import HeartbeatProducer, create_producer
from src.logger import get_logger, setup_logging
from src.validation.validator import HeartbeatValidator

logger = get_logger("main")


class HeartbeatPipeline:
    """
    Orchestrates the full heartbeat monitoring pipeline.

    Components:
        1. Data Generator → Produces synthetic heartbeat data
        2. Kafka Producer → Streams data to Kafka topic
        3. Kafka Consumer → Reads from Kafka, validates, persists to DB
    """

    def __init__(self):
        """Initialize the pipeline with all components."""
        self._generator: Optional[HeartbeatGenerator] = None
        self._producer: Optional[HeartbeatProducer] = None
        self._consumer: Optional[HeartbeatConsumer] = None
        self._db_handler: Optional[DatabaseHandler] = None
        self._validator: Optional[HeartbeatValidator] = None
        self._running = False
        self._producer_thread: Optional[threading.Thread] = None
        self._consumer_thread: Optional[threading.Thread] = None

    def _setup_components(self) -> None:
        """Initialize all pipeline components."""
        logger.info("Setting up pipeline components...")

        # Database handler
        self._db_handler = DatabaseHandler()
        self._db_handler.connect()
        logger.info("Database handler ready")

        # Validator
        self._validator = HeartbeatValidator()
        logger.info("Validator ready")

        # Data generator
        self._generator = HeartbeatGenerator()
        logger.info("Data generator ready")

        # Kafka producer
        self._producer = HeartbeatProducer()
        self._producer.connect()
        logger.info("Kafka producer ready")

        # Kafka consumer
        self._consumer = HeartbeatConsumer(
            db_handler=self._db_handler,
            validator=self._validator,
        )
        self._consumer.connect()
        logger.info("Kafka consumer ready")

        logger.info("All pipeline components initialized successfully")

    def _run_producer(self) -> None:
        """
        Producer loop: generates data and sends to Kafka.

        Runs in a separate thread.
        """
        logger.info("Producer thread started")
        cycle = 0

        try:
            while self._running:
                cycle += 1
                # Generate a batch of readings (one per customer)
                batch = self._generator.generate_batch()

                # Send each reading to Kafka
                for reading in batch:
                    if not self._running:
                        break
                    self._producer.send(reading)

                # Log progress
                if cycle % 50 == 0:
                    stats = self._producer.stats
                    logger.info(
                        "Producer cycle %d | Delivered: %d | Failed: %d | Generated: %d",
                        cycle,
                        stats["delivered"],
                        stats["failed"],
                        self._generator.total_generated,
                    )

                time.sleep(config.generator.interval_sec)

        except Exception as e:
            logger.error("Producer thread error: %s", e)
        finally:
            if self._producer:
                self._producer.flush()
            logger.info("Producer thread stopped")

    def _run_consumer(self) -> None:
        """
        Consumer loop: reads from Kafka, validates, persists to DB.

        Runs in a separate thread.
        """
        logger.info("Consumer thread started")

        try:
            self._consumer.consume()
        except Exception as e:
            logger.error("Consumer thread error: %s", e)
        finally:
            logger.info("Consumer thread stopped")

    def start(self) -> None:
        """
        Start the full pipeline.

        Launches producer and consumer in separate threads and waits
        for shutdown signal.
        """
        logger.info("=" * 60)
        logger.info("Starting Heartbeat Monitoring Pipeline")
        logger.info("=" * 60)

        self._setup_components()
        self._running = True

        # Set up graceful shutdown
        def shutdown(signum, frame):
            logger.info("Shutdown signal received")
            self.stop()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        # Start producer and consumer threads
        self._producer_thread = threading.Thread(
            target=self._run_producer,
            name="producer-thread",
            daemon=True,
        )
        self._consumer_thread = threading.Thread(
            target=self._run_consumer,
            name="consumer-thread",
            daemon=True,
        )

        self._producer_thread.start()
        self._consumer_thread.start()

        logger.info("Pipeline is running. Press Ctrl+C to stop.")

        # Keep main thread alive
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Gracefully stop all pipeline components."""
        logger.info("Stopping pipeline...")
        self._running = False

        # Stop consumer
        if self._consumer:
            self._consumer.close()

        # Wait for threads to finish
        if self._producer_thread and self._producer_thread.is_alive():
            self._producer_thread.join(timeout=10)

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=10)

        # Close producer
        if self._producer:
            self._producer.close()

        # Close database
        if self._db_handler:
            self._db_handler.disconnect()

        self._print_summary()
        logger.info("Pipeline stopped successfully")

    def _print_summary(self) -> None:
        """Print pipeline execution summary."""
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("-" * 60)

        if self._generator:
            logger.info("Generated readings: %d", self._generator.total_generated)

        if self._producer:
            stats = self._producer.stats
            logger.info("Kafka delivered: %d", stats["delivered"])
            logger.info("Kafka failed: %d", stats["failed"])

        if self._consumer:
            stats = self._consumer.stats
            logger.info("Consumed messages: %d", stats["consumed"])
            logger.info("Processed to DB: %d", stats["processed"])
            val_stats = stats["validation_stats"]
            logger.info("Validation rejected: %d", val_stats["rejected"])
            logger.info("Anomalies detected: %d", val_stats["anomalies_detected"])

        if self._db_handler:
            logger.info("DB inserts (session): %d", self._db_handler.insert_count)

        logger.info("=" * 60)


def run_producer_only() -> None:
    """Run only the data generator + Kafka producer."""
    setup_logging()
    logger.info("Running producer-only mode")

    generator = create_generator()
    producer = create_producer()

    try:
        for reading in generator.stream_readings():
            producer.send(reading)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.close()


def run_consumer_only() -> None:
    """Run only the Kafka consumer + DB writer."""
    setup_logging()
    logger.info("Running consumer-only mode")

    db_handler = create_db_handler()
    validator = HeartbeatValidator()

    consumer = HeartbeatConsumer(
        db_handler=db_handler,
        validator=validator,
    )
    consumer.connect()

    try:
        consumer.consume()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
        db_handler.disconnect()


def main() -> None:
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Real-Time Customer Heartbeat Monitoring Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main                  # Run full pipeline
  python -m src.main --mode producer  # Run producer only
  python -m src.main --mode consumer  # Run consumer only
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "producer", "consumer"],
        default="full",
        help="Pipeline mode: 'full' (default), 'producer', or 'consumer'",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Override log level",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=args.log_level)

    logger.info("Pipeline mode: %s", args.mode)

    if args.mode == "producer":
        run_producer_only()
    elif args.mode == "consumer":
        run_consumer_only()
    else:
        pipeline = HeartbeatPipeline()
        pipeline.start()


if __name__ == "__main__":
    main()
