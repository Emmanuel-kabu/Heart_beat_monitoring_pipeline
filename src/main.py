"""
Main Pipeline Orchestrator

Coordinates all pipeline components: data generation, Kafka streaming,
validation, database persistence, Prometheus metrics, DLQ, edge-case
detection, data quality (Great Expectations), alerting (Slack/Email),
and scheduled reporting.
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
from typing import Optional

from src.config import config
from src.data_quality.quality_engine import DataQualityEngine
from src.data_quality.quality_reporter import DataQualityReporter
from src.data_quality.quality_store import DataQualityStore
from src.database.db_handler import DatabaseHandler, create_db_handler
from src.generator.heartbeat_generator import HeartbeatGenerator, create_generator
from src.kafka_client.consumer import HeartbeatConsumer, create_consumer
from src.kafka_client.producer import HeartbeatProducer, create_producer
from src.logger import get_logger, setup_logging
from src.models import HeartbeatReading
from src.monitoring.alerting import EmailAlerter, SlackAlerter
from src.monitoring.dlq import DeadLetterQueueProducer
from src.monitoring.edge_cases import EdgeCaseDetector
from src.monitoring.metrics import (
    DATABASE_CONNECTED,
    KAFKA_CONSUMER_CONNECTED,
    KAFKA_PRODUCER_CONNECTED,
    PIPELINE_UPTIME_SECONDS,
    READINGS_GENERATED,
    READINGS_PRODUCED,
    start_metrics_server,
)
from src.monitoring.reporting import PipelineReporter
from src.validation.validator import HeartbeatValidator

logger = get_logger("main")


class HeartbeatPipeline:
    """
    Orchestrates the full heartbeat monitoring pipeline.

    Components:
        1. Data Generator  → Produces synthetic heartbeat data
        2. Kafka Producer   → Streams data to Kafka topic
        3. Kafka Consumer   → Reads from Kafka, validates, persists to DB
        4. Prometheus       → Metrics collection at /metrics
        5. Edge-Case Engine → Detects delay, failure, spikes, quality
        6. DLQ Producer     → Routes failed messages to Dead Letter Queue
        7. DQ Engine        → Great Expectations data quality validation
        8. Reporter         → Sends daily/weekly summaries to Slack
    """

    def __init__(self):
        """Initialize the pipeline with all components."""
        self._generator: Optional[HeartbeatGenerator] = None
        self._producer: Optional[HeartbeatProducer] = None
        self._consumer: Optional[HeartbeatConsumer] = None
        self._db_handler: Optional[DatabaseHandler] = None
        self._validator: Optional[HeartbeatValidator] = None
        self._dlq_producer: Optional[DeadLetterQueueProducer] = None
        self._edge_detector: Optional[EdgeCaseDetector] = None
        self._reporter: Optional[PipelineReporter] = None
        self._slack: Optional[SlackAlerter] = None
        self._email: Optional[EmailAlerter] = None
        self._dq_engine: Optional[DataQualityEngine] = None
        self._dq_store: Optional[DataQualityStore] = None
        self._dq_reporter: Optional[DataQualityReporter] = None
        self._running = False
        self._start_time: Optional[float] = None
        self._producer_thread: Optional[threading.Thread] = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._uptime_thread: Optional[threading.Thread] = None

    def _setup_components(self) -> None:
        """Initialize all pipeline components."""
        logger.info("Setting up pipeline components...")

        # ── Prometheus metrics server ───────────────────────────────────
        start_metrics_server(port=config.monitoring.prometheus_port)
        logger.info("Prometheus metrics server ready on port %d", config.monitoring.prometheus_port)

        # ── Alerting ────────────────────────────────────────────────────
        self._slack = SlackAlerter()
        self._slack_daily = SlackAlerter(
            webhook_url=config.alerting.slack_daily_webhook_url or None,
            channel=config.alerting.slack_daily_channel or None,
        ) if config.alerting.slack_daily_webhook_url else self._slack
        self._slack_weekly = SlackAlerter(
            webhook_url=config.alerting.slack_weekly_webhook_url or None,
            channel=config.alerting.slack_weekly_channel or None,
        ) if config.alerting.slack_weekly_webhook_url else self._slack
        self._email = EmailAlerter()
        logger.info("Alerting (Slack + Email) ready")

        # Database handler
        self._db_handler = DatabaseHandler()
        self._db_handler.connect()
        DATABASE_CONNECTED.set(1)
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
        KAFKA_PRODUCER_CONNECTED.set(1)
        logger.info("Kafka producer ready")

        # DLQ producer
        self._dlq_producer = DeadLetterQueueProducer()
        self._dlq_producer.connect()
        logger.info("DLQ producer ready")

        # Kafka consumer (with DLQ + edge detector refs injected)
        self._edge_detector = EdgeCaseDetector(
            slack=self._slack, email=self._email,
        )
        self._reporter = PipelineReporter(
            slack=self._slack,
            slack_daily=self._slack_daily,
            slack_weekly=self._slack_weekly,
            email_monthly=self._email,
        )

        # ── Data Quality Engine ─────────────────────────────────────────
        if config.data_quality.enabled:
            self._dq_store = DataQualityStore()
            self._dq_engine = DataQualityEngine(
                dlq_producer=self._dlq_producer,
                quality_store=self._dq_store,
            )
            self._dq_reporter = DataQualityReporter(
                quality_store=self._dq_store,
                slack=self._slack,
                email=self._email,
                degradation_email_threshold=config.data_quality.degradation_email_threshold,
            )
            logger.info("Data Quality Engine ready (GE + DLQ + reporting)")
        else:
            logger.info("Data Quality Engine disabled via DQ_ENABLED=false")

        self._consumer = HeartbeatConsumer(
            db_handler=self._db_handler,
            validator=self._validator,
            dlq_producer=self._dlq_producer,
            edge_detector=self._edge_detector,
            reporter=self._reporter,
            dq_engine=self._dq_engine,
        )
        self._consumer.connect()
        KAFKA_CONSUMER_CONNECTED.set(1)
        logger.info("Kafka consumer ready")

        logger.info("All pipeline components initialized successfully")

    def _run_producer(self) -> None:
        """
        Producer loop: generates data and sends to Kafka.

        Runs in a separate thread. Updates Prometheus metrics per reading.
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
                    READINGS_GENERATED.labels(customer_id=reading.customer_id).inc()
                    self._producer.send(reading)
                    READINGS_PRODUCED.labels(customer_id=reading.customer_id).inc()

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
        self._start_time = time.time()

        # Set up graceful shutdown
        def shutdown(signum, frame):
            logger.info("Shutdown signal received")
            self.stop()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        # Start edge-case detector + reporter + DQ reporter
        if self._edge_detector:
            self._edge_detector.start()
        if self._reporter:
            self._reporter.start()
        if self._dq_reporter:
            self._dq_reporter.start()

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
        self._uptime_thread = threading.Thread(
            target=self._track_uptime,
            name="uptime-thread",
            daemon=True,
        )

        self._producer_thread.start()
        self._consumer_thread.start()
        self._uptime_thread.start()

        logger.info("Pipeline is running. Press Ctrl+C to stop.")

        # Keep main thread alive
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def _track_uptime(self) -> None:
        """Background loop to update the uptime gauge."""
        while self._running:
            if self._start_time:
                PIPELINE_UPTIME_SECONDS.set(time.time() - self._start_time)
            time.sleep(5)

    def stop(self) -> None:
        """Gracefully stop all pipeline components."""
        logger.info("Stopping pipeline...")
        self._running = False

        # Stop monitoring components
        if self._edge_detector:
            self._edge_detector.stop()
        if self._reporter:
            self._reporter.stop()
        if self._dq_reporter:
            self._dq_reporter.stop()

        # Stop consumer
        if self._consumer:
            self._consumer.close()
            KAFKA_CONSUMER_CONNECTED.set(0)

        # Wait for threads to finish
        if self._producer_thread and self._producer_thread.is_alive():
            self._producer_thread.join(timeout=10)

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=10)

        # Close producer
        if self._producer:
            self._producer.close()
            KAFKA_PRODUCER_CONNECTED.set(0)

        # Close DLQ producer
        if self._dlq_producer:
            self._dlq_producer.close()

        # Close database
        if self._db_handler:
            self._db_handler.disconnect()
            DATABASE_CONNECTED.set(0)

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

    start_metrics_server(port=config.monitoring.prometheus_port)

    db_handler = create_db_handler()
    validator = HeartbeatValidator()
    dlq_producer = DeadLetterQueueProducer()
    dlq_producer.connect()
    edge_detector = EdgeCaseDetector()
    reporter = PipelineReporter()

    # Data Quality
    dq_engine = None
    dq_store = None
    dq_reporter = None
    if config.data_quality.enabled:
        dq_store = DataQualityStore()
        dq_engine = DataQualityEngine(
            dlq_producer=dlq_producer,
            quality_store=dq_store,
        )
        dq_reporter = DataQualityReporter(
            quality_store=dq_store,
            degradation_email_threshold=config.data_quality.degradation_email_threshold,
        )

    consumer = HeartbeatConsumer(
        db_handler=db_handler,
        validator=validator,
        dlq_producer=dlq_producer,
        edge_detector=edge_detector,
        reporter=reporter,
        dq_engine=dq_engine,
    )
    consumer.connect()

    edge_detector.start()
    reporter.start()
    if dq_reporter:
        dq_reporter.start()

    try:
        consumer.consume()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        if dq_reporter:
            dq_reporter.stop()
        edge_detector.stop()
        reporter.stop()
        consumer.close()
        dlq_producer.close()
        db_handler.disconnect()


def run_dlq_retry() -> None:
    """Reprocess messages from the Dead Letter Queue."""
    setup_logging()
    logger.info("Running DLQ retry mode")

    from src.monitoring.dlq import DeadLetterQueueConsumer

    db_handler = create_db_handler()
    validator = HeartbeatValidator()
    dlq_producer = DeadLetterQueueProducer()
    dlq_producer.connect()
    dlq_consumer = DeadLetterQueueConsumer()
    dlq_consumer.connect()

    def handler(data: dict) -> bool:
        """Attempt to reprocess a DLQ message."""
        try:
            reading = HeartbeatReading.from_dict(data)
            is_valid, enriched, error = validator.validate_and_enrich(reading)
            if not is_valid:
                logger.warning("DLQ re-validation failed: %s", error)
                return False
            db_handler.insert_reading(enriched)
            return True
        except Exception as e:
            logger.error("DLQ reprocess error: %s", e)
            return False

    try:
        stats = dlq_consumer.retry_with_handler(
            handler=handler,
            max_messages=100,
            dlq_producer=dlq_producer,
        )
        logger.info("DLQ retry results: %s", stats)
    except KeyboardInterrupt:
        logger.info("DLQ retry stopped by user")
    finally:
        dlq_consumer.close()
        dlq_producer.close()
        db_handler.disconnect()


def run_dq_report() -> None:
    """Print an on-demand data quality status report."""
    setup_logging()
    logger.info("Running DQ report mode")

    dq_store = DataQualityStore()
    summary = dq_store.get_lifetime_summary()

    print("\n" + "=" * 60)
    print("DATA QUALITY STATUS REPORT")
    print("=" * 60)
    print(f"  Total batches validated : {summary.get('total_batches', 0)}")
    print(f"  Total rows validated    : {summary.get('total_rows', 0)}")
    print(f"  Rows passed             : {summary.get('total_passed', 0)}")
    print(f"  Rows failed (DLQ)       : {summary.get('total_failed', 0)}")
    print(f"  Pass rate               : {summary.get('pass_rate', 0):.1f}%")
    print("-" * 60)
    dim_scores = summary.get("avg_dimension_scores", {})
    for dim, score in dim_scores.items():
        print(f"  {dim:<25}: {score:.1f}%")
    print("-" * 60)
    top_rules = dq_store.get_top_failing_rules(5)
    if top_rules:
        print("  Top failing rules:")
        for rule, count in top_rules:
            print(f"    - {rule}: {count} failures")
    top_customers = dq_store.get_top_failing_customers(5)
    if top_customers:
        print("  Top failing customers:")
        for cust, count in top_customers:
            print(f"    - {cust}: {count} failures")
    trend = dq_store.get_quality_trend()
    print(f"\n  Quality trend: {trend.get('direction', 'N/A')} "
          f"(delta={trend.get('delta', 0):+.1f}%)")
    print("=" * 60 + "\n")

    # Also send to Slack if configured
    try:
        slack = SlackAlerter()
        dq_reporter = DataQualityReporter(quality_store=dq_store, slack=slack)
        dq_reporter.send_immediate_report()
        logger.info("DQ report also sent to Slack")
    except Exception as e:
        logger.debug("Slack DQ report skipped: %s", e)


def main() -> None:
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Real-Time Customer Heartbeat Monitoring Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main                      # Run full pipeline
  python -m src.main --mode producer      # Run producer only
  python -m src.main --mode consumer      # Run consumer only
  python -m src.main --mode dlq-retry     # Reprocess DLQ messages
  python -m src.main --mode dq-report     # Print on-demand DQ status report
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "producer", "consumer", "dlq-retry", "dq-report"],
        default="full",
        help="Pipeline mode: 'full' (default), 'producer', 'consumer', 'dlq-retry', or 'dq-report'",
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
    elif args.mode == "dlq-retry":
        run_dlq_retry()
    elif args.mode == "dq-report":
        run_dq_report()
    else:
        pipeline = HeartbeatPipeline()
        pipeline.start()


if __name__ == "__main__":
    main()
