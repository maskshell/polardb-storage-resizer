"""
Logging setup for PolarDB Storage Resizer.

This module provides structured JSON logging with:
- Trace ID propagation across all log messages
- Environment-based log level configuration
- JSON format for production, human-readable for development
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any


# Module-scoped trace context for this execution
_trace_context: dict[str, str | None] = {"trace_id": None}


def set_trace_id(trace_id: str) -> None:
    """
    Set the trace ID for this execution.

    Args:
        trace_id: UUID4 trace ID to use in all log messages
    """
    _trace_context["trace_id"] = trace_id


def get_trace_id() -> str | None:
    """
    Get the current trace ID.

    Returns:
        Current trace ID or None if not set
    """
    return _trace_context["trace_id"]


class JsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as JSON objects with:
    - timestamp: ISO 8601 format
    - level: Log level name
    - logger: Logger name
    - message: Log message
    - trace_id: Current trace ID (if set)
    - Additional fields from extra
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add trace ID if set
        trace_id = get_trace_id()
        if trace_id:
            log_data["trace_id"] = trace_id

        # Add extra fields
        if hasattr(record, "cluster_id"):
            log_data["cluster_id"] = record.cluster_id
        if hasattr(record, "region"):
            log_data["region"] = record.region
        if hasattr(record, "action"):
            log_data["action"] = record.action
        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms
        if hasattr(record, "error_code"):
            log_data["error_code"] = record.error_code

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class HumanReadableFormatter(logging.Formatter):
    """
    Human-readable formatter for development.

    Includes trace ID in brackets if set.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for human reading."""
        # Build prefix with trace ID (full UUID for traceability)
        prefix_parts = [record.levelname]
        trace_id = get_trace_id()
        if trace_id:
            prefix_parts.append(f"[{trace_id}]")
        prefix = " ".join(prefix_parts)

        # Format message
        message = f"{prefix}: {record.getMessage()}"

        # Add exception if present
        if record.exc_info:
            message += f"\n{self.formatException(record.exc_info)}"

        return message


def setup_logging(
    level: str = "INFO",
    json_format: bool = False,
    force: bool = True,
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: If True, use JSON format; otherwise human-readable
        force: If True, replace existing handlers; if False, add to existing
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Only remove existing handlers if force=True
    # This allows pytest's log capture to work when force=False
    if force:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    # Set formatter based on mode
    formatter: logging.Formatter
    formatter = JsonFormatter() if json_format else HumanReadableFormatter()

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)
