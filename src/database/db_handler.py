"""
Database Handler Module

Manages PostgreSQL connections and provides methods for inserting
and querying heartbeat data. Uses connection pooling for efficiency
and implements proper error handling and resource cleanup.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

from src.config import config
from src.logger import get_logger
from src.models import HeartbeatReading

logger = get_logger("database")


class DatabaseHandler:
    """
    PostgreSQL database handler with connection pooling.

    Provides CRUD operations for heartbeat readings, anomaly logs,
    and pipeline metrics. Uses psycopg2 connection pool for
    efficient resource management.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        min_connections: int = 2,
        max_connections: int = 10,
    ):
        """
        Initialize the database handler with connection pool.

        Args:
            host: PostgreSQL host. Defaults to config value.
            port: PostgreSQL port. Defaults to config value.
            database: Database name. Defaults to config value.
            user: Database user. Defaults to config value.
            password: Database password. Defaults to config value.
            min_connections: Minimum pool size.
            max_connections: Maximum pool size.
        """
        self._host = host or config.postgres.host
        self._port = port or config.postgres.port
        self._database = database or config.postgres.database
        self._user = user or config.postgres.user
        self._password = password or config.postgres.password
        self._min_conn = min_connections
        self._max_conn = max_connections
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._insert_count = 0

    def connect(self) -> None:
        """Establish the connection pool."""
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self._min_conn,
                maxconn=self._max_conn,
                host=self._host,
                port=self._port,
                dbname=self._database,
                user=self._user,
                password=self._password,
            )
            logger.info(
                "Database connection pool established (%s:%d/%s)",
                self._host,
                self._port,
                self._database,
            )
        except psycopg2.Error as e:
            logger.error("Failed to create connection pool: %s", e)
            raise

    def disconnect(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")

    @contextmanager
    def get_connection(self) -> Generator:
        """
        Context manager that provides a database connection from the pool.

        Yields:
            psycopg2 connection object.
        """
        if not self._pool:
            raise RuntimeError("Database connection pool not initialized. Call connect() first.")

        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    @contextmanager
    def get_cursor(self, cursor_factory=None) -> Generator:
        """
        Context manager that provides a database cursor.

        Args:
            cursor_factory: Optional cursor factory (e.g., RealDictCursor).

        Yields:
            psycopg2 cursor object.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

    # ─── INSERT Operations ──────────────────────────────────────────────

    def insert_reading(self, reading: HeartbeatReading) -> int:
        """
        Insert a single heartbeat reading into the database.

        Uses the PostgreSQL function for automatic anomaly logging.

        Args:
            reading: HeartbeatReading to insert.

        Returns:
            The ID of the inserted reading.
        """
        query = """
            SELECT heartbeat.insert_reading(
                %s, %s, %s::TIMESTAMPTZ, %s, %s
            )
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        reading.customer_id,
                        reading.heart_rate,
                        reading.timestamp,
                        config.anomaly.low_threshold,
                        config.anomaly.high_threshold,
                    ),
                )
                result = cursor.fetchone()
                reading_id = result[0] if result else None
                self._insert_count += 1

                if self._insert_count % 500 == 0:
                    logger.info("Total readings inserted: %d", self._insert_count)

                return reading_id

        except psycopg2.Error as e:
            logger.error("Failed to insert reading %s: %s", reading, e)
            raise

    def insert_readings_batch(self, readings: List[HeartbeatReading]) -> int:
        """
        Insert multiple heartbeat readings in a single transaction.

        Uses batch execution for improved performance.

        Args:
            readings: List of HeartbeatReading instances.

        Returns:
            Number of successfully inserted readings.
        """
        if not readings:
            return 0

        query = """
            INSERT INTO heartbeat.heart_rate_readings
                (customer_id, heart_rate, timestamp, is_anomaly, anomaly_type)
            VALUES (%s, %s, %s::TIMESTAMPTZ, %s, %s)
        """
        anomaly_query = """
            INSERT INTO heartbeat.anomaly_log
                (reading_id, customer_id, heart_rate, anomaly_type, timestamp)
            VALUES (currval('heartbeat.heart_rate_readings_id_seq'), %s, %s, %s, %s::TIMESTAMPTZ)
        """

        inserted = 0
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for reading in readings:
                        cursor.execute(
                            query,
                            (
                                reading.customer_id,
                                reading.heart_rate,
                                reading.timestamp,
                                reading.is_anomaly,
                                reading.anomaly_type,
                            ),
                        )
                        # Log anomalies separately
                        if reading.is_anomaly:
                            cursor.execute(
                                anomaly_query,
                                (
                                    reading.customer_id,
                                    reading.heart_rate,
                                    reading.anomaly_type,
                                    reading.timestamp,
                                ),
                            )
                        inserted += 1

            self._insert_count += inserted
            logger.debug("Batch insert: %d readings inserted", inserted)
            return inserted

        except psycopg2.Error as e:
            logger.error("Batch insert failed: %s", e)
            raise

    # ─── QUERY Operations ───────────────────────────────────────────────

    def get_latest_readings(self, limit: int = 10) -> List[Dict]:
        """
        Get the most recent heart rate readings.

        Args:
            limit: Maximum number of readings to return.

        Returns:
            List of reading dictionaries.
        """
        query = """
            SELECT customer_id, heart_rate, timestamp, is_anomaly, anomaly_type
            FROM heartbeat.heart_rate_readings
            ORDER BY timestamp DESC
            LIMIT %s
        """
        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_customer_readings(
        self,
        customer_id: str,
        limit: int = 100,
        since: Optional[datetime] = None,
    ) -> List[Dict]:
        """
        Get readings for a specific customer.

        Args:
            customer_id: Customer identifier.
            limit: Maximum number of readings.
            since: Optional timestamp to filter readings after.

        Returns:
            List of reading dictionaries.
        """
        if since:
            query = """
                SELECT customer_id, heart_rate, timestamp, is_anomaly, anomaly_type
                FROM heartbeat.heart_rate_readings
                WHERE customer_id = %s AND timestamp >= %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            params = (customer_id, since, limit)
        else:
            query = """
                SELECT customer_id, heart_rate, timestamp, is_anomaly, anomaly_type
                FROM heartbeat.heart_rate_readings
                WHERE customer_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            params = (customer_id, limit)

        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_anomalies(self, limit: int = 50) -> List[Dict]:
        """
        Get recent anomalous readings.

        Args:
            limit: Maximum number of anomalies to return.

        Returns:
            List of anomaly dictionaries.
        """
        query = """
            SELECT al.customer_id, c.customer_name, al.heart_rate,
                   al.anomaly_type, al.timestamp, al.detected_at
            FROM heartbeat.anomaly_log al
            JOIN heartbeat.customers c ON al.customer_id = c.customer_id
            ORDER BY al.timestamp DESC
            LIMIT %s
        """
        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_customer_summary(self) -> List[Dict]:
        """
        Get summary statistics for all customers.

        Returns:
            List of customer summary dictionaries.
        """
        query = "SELECT * FROM heartbeat.customer_summary"
        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

    def get_hourly_averages(self, customer_id: str | None = None, hours: int = 24) -> List[Dict]:
        """
        Get hourly average heart rates.

        Args:
            customer_id: Optional filter by customer.
            hours: Number of hours to look back.

        Returns:
            List of hourly average dictionaries.
        """
        if customer_id:
            query = """
                SELECT * FROM heartbeat.hourly_avg_heart_rate
                WHERE customer_id = %s
                ORDER BY hour DESC
                LIMIT %s
            """
            params = (customer_id, hours)
        else:
            query = """
                SELECT * FROM heartbeat.hourly_avg_heart_rate
                ORDER BY hour DESC
                LIMIT %s
            """
            params = (hours * 10,)  # Approximate: 10 customers * hours

        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def record_pipeline_metric(self, metric_name: str, metric_value: float) -> None:
        """
        Record a pipeline performance metric.

        Args:
            metric_name: Name of the metric.
            metric_value: Numeric value of the metric.
        """
        query = """
            INSERT INTO heartbeat.pipeline_metrics (metric_name, metric_value)
            VALUES (%s, %s)
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, (metric_name, metric_value))

    def get_total_readings_count(self) -> int:
        """Get total count of readings in the database."""
        query = "SELECT COUNT(*) FROM heartbeat.heart_rate_readings"
        with self.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else 0

    @property
    def insert_count(self) -> int:
        """Total number of readings inserted in this session."""
        return self._insert_count


def create_db_handler(**kwargs) -> DatabaseHandler:
    """Factory function to create and connect a DatabaseHandler."""
    handler = DatabaseHandler(**kwargs)
    handler.connect()
    return handler
