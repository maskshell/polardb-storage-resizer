"""
Metrics collection for PolarDB Storage Resizer.

This module provides simple metrics collection for:
- Successful/failed changes
- Clusters scanned/skipped
- API call errors
- Execution timing

Metrics are output as structured logs for now, with the ability
to integrate with Prometheus or cloud monitoring later.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any


@dataclass
class Metrics:
    """
    Collection of execution metrics.

    Tracks various counters and timings for the resizer execution.
    """

    # Cluster metrics
    clusters_scanned: int = 0
    clusters_filtered: int = 0
    clusters_targeted: int = 0

    # Change metrics
    changes_planned: int = 0
    changes_applied: int = 0
    changes_succeeded: int = 0
    changes_failed: int = 0
    changes_skipped: int = 0

    # Error metrics by type
    transient_errors: int = 0
    permanent_errors: int = 0

    # Timing metrics (in milliseconds)
    total_duration_ms: float = 0.0
    api_call_duration_ms: float = 0.0

    # API call counts
    api_calls_total: int = 0
    api_calls_list_clusters: int = 0
    api_calls_get_detail: int = 0
    api_calls_modify_storage: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert metrics to dictionary for logging.

        Returns:
            Dictionary with all metric values
        """
        return {
            "clusters_scanned": self.clusters_scanned,
            "clusters_filtered": self.clusters_filtered,
            "clusters_targeted": self.clusters_targeted,
            "changes_planned": self.changes_planned,
            "changes_applied": self.changes_applied,
            "changes_succeeded": self.changes_succeeded,
            "changes_failed": self.changes_failed,
            "changes_skipped": self.changes_skipped,
            "transient_errors": self.transient_errors,
            "permanent_errors": self.permanent_errors,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "api_call_duration_ms": round(self.api_call_duration_ms, 2),
            "api_calls_total": self.api_calls_total,
            "api_calls_list_clusters": self.api_calls_list_clusters,
            "api_calls_get_detail": self.api_calls_get_detail,
            "api_calls_modify_storage": self.api_calls_modify_storage,
        }

    def increment_clusters_scanned(self, count: int = 1) -> None:
        """Increment clusters scanned counter."""
        self.clusters_scanned += count

    def increment_clusters_filtered(self, count: int = 1) -> None:
        """Increment clusters filtered counter."""
        self.clusters_filtered += count

    def increment_changes(self, succeeded: bool = True) -> None:
        """
        Increment change counters.

        Args:
            succeeded: Whether the change succeeded
        """
        self.changes_applied += 1
        if succeeded:
            self.changes_succeeded += 1
        else:
            self.changes_failed += 1

    def increment_error(self, is_transient: bool) -> None:
        """
        Increment error counter.

        Args:
            is_transient: Whether the error is transient
        """
        if is_transient:
            self.transient_errors += 1
        else:
            self.permanent_errors += 1

    def record_api_call(self, call_type: str, duration_ms: float) -> None:
        """
        Record an API call.

        Args:
            call_type: Type of API call (list_clusters, get_detail, modify_storage)
            duration_ms: Duration of the call in milliseconds
        """
        self.api_calls_total += 1
        self.api_call_duration_ms += duration_ms

        if call_type == "list_clusters":
            self.api_calls_list_clusters += 1
        elif call_type == "get_detail":
            self.api_calls_get_detail += 1
        elif call_type == "modify_storage":
            self.api_calls_modify_storage += 1


@lru_cache(maxsize=1)
def _metrics_instance() -> Metrics:
    """Create and cache the process-wide metrics instance."""
    return Metrics()


def get_metrics() -> Metrics:
    """
    Get the global metrics instance.

    Creates a new instance if one doesn't exist.

    Returns:
        Global Metrics instance
    """
    return _metrics_instance()


def reset_metrics() -> None:
    """Reset the global metrics instance."""
    _metrics_instance.cache_clear()
    _metrics_instance()


def log_metrics_summary(logger: logging.Logger) -> None:
    """
    Log a summary of collected metrics.

    Args:
        logger: Logger instance to use for output
    """
    metrics = get_metrics()
    logger.info("Execution metrics", extra=metrics.to_dict())
