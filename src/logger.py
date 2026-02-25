"""
Logging Configuration Module

Sets up structured logging with both console and file handlers.
Provides a consistent logging interface across the entire pipeline.
"""

import logging
import logging.handlers
import sys
from pathlib import Path

from src.config import config


def setup_logging(
    name: str = "heartbeat_pipeline",
    level: str | None = None,
    log_file: str | None = None,
) -> logging.Logger:
    """
    Configure and return a logger with console and file handlers.

    Args:
        name: Logger name (typically module name).
        level: Override log level (defaults to config value).
        log_file: Override log file path (defaults to config value).

    Returns:
        Configured logging.Logger instance.
    """
    log_level = getattr(logging, (level or config.logging.level).upper(), logging.INFO)
    log_path = Path(log_file or config.logging.log_file)

    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    # ─── Formatter ──────────────────────────────────────────────────────
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ─── Console Handler ────────────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    console_handler.stream = open(sys.stdout.fileno(), mode='w', encoding='utf-8', errors='replace', closefd=False)
    logger.addHandler(console_handler)

    # ─── Rotating File Handler ──────────────────────────────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(log_path),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a child logger under the main pipeline logger.

    Args:
        name: Module or component name.

    Returns:
        Logger instance configured as child of the main pipeline logger.
    """
    # Ensure root pipeline logger is configured
    setup_logging()
    return logging.getLogger(f"heartbeat_pipeline.{name}")
