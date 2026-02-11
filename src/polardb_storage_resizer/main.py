"""
Main entry point for PolarDB Storage Resizer.

This module provides:
- CLI entry point with signal handling
- Trace ID generation and propagation
- Exit code semantics (0=success, 1=partial failure, 2=config error, 3=signal)
- Main orchestration logic
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import uuid
from collections.abc import Callable
from types import FrameType, TracebackType
from typing import TYPE_CHECKING, Any

from polardb_storage_resizer.cloud_client import (
    create_rate_limited_client,
)
from polardb_storage_resizer.config import AppConfig
from polardb_storage_resizer.errors import ValidationError
from polardb_storage_resizer.executor import apply_changes, plan_changes
from polardb_storage_resizer.logging_setup import (
    get_logger,
    set_trace_id,
    setup_logging,
)
from polardb_storage_resizer.metrics import (
    get_metrics,
    log_metrics_summary,
    reset_metrics,
)
from polardb_storage_resizer.redaction import redact_error_message
from polardb_storage_resizer.strategy import select_target_clusters

if TYPE_CHECKING:
    from polardb_storage_resizer.cloud_client import PolarDBClient
    from polardb_storage_resizer.models import ClusterDetail, ExecutionReport


# Exit codes
EXIT_SUCCESS = 0  # All successful (including no changes needed)
EXIT_PARTIAL_FAILURE = 1  # At least one change failed
EXIT_CONFIG_ERROR = 2  # Configuration error or startup validation failed
EXIT_SIGNAL_INTERRUPT = 3  # Interrupted by signal (SIGTERM/SIGINT)


def generate_trace_id() -> str:
    """
    Generate a UUID4 trace ID for this execution.

    Returns:
        UUID4 string for tracing this execution
    """
    return str(uuid.uuid4())


class GracefulShutdown:
    """
    Context manager for graceful shutdown handling.

    Sets up signal handlers and provides a shutdown event that
    can be passed to the executor.
    """

    def __init__(self) -> None:
        """Initialize shutdown handler."""
        self.shutdown_event = threading.Event()
        self._original_sigterm: Callable[[int, FrameType | None], Any] | int | None = (
            None
        )
        self._original_sigint: Callable[[int, FrameType | None], Any] | int | None = (
            None
        )
        self._received_signal = False

    def _signal_handler(self, signum: int, _frame: FrameType | None) -> None:
        """Handle shutdown signal."""
        self._received_signal = True
        self.shutdown_event.set()
        logger = get_logger(__name__)
        logger.warning("Received signal %s, initiating graceful shutdown", signum)

    def __enter__(self) -> GracefulShutdown:
        """Set up signal handlers."""
        self._original_sigterm = signal.getsignal(signal.SIGTERM)
        self._original_sigint = signal.getsignal(signal.SIGINT)

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Restore original signal handlers."""
        if self._original_sigterm is not None:
            signal.signal(signal.SIGTERM, self._original_sigterm)
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)

    def was_interrupted(self) -> bool:
        """Check if a shutdown signal was received."""
        return self._received_signal


def discover_clusters(
    client: PolarDBClient,
    config: AppConfig,
    logger: logging.Logger,
) -> list[ClusterDetail]:
    """
    Discover clusters across all configured regions.

    Args:
        client: PolarDB client for API calls
        config: Application configuration
        logger: Logger instance

    Returns:
        List of cluster details for all discovered clusters
    """

    all_clusters: list[ClusterDetail] = []
    metrics = get_metrics()

    # If whitelist or tag filters are configured, use API-level filtering for efficiency
    # Otherwise, list all clusters and filter locally
    cluster_ids_filter = config.cluster_whitelist if config.cluster_whitelist else None
    tag_filters = config.cluster_tag_filters if config.cluster_tag_filters else None

    for region in config.regions:
        logger.info("Discovering clusters in region: %s", region)

        try:
            # List clusters in this region
            # Pass whitelist and tag filters for API-level filtering if configured
            summaries = client.list_clusters(region, cluster_ids_filter, tag_filters)
            metrics.increment_clusters_scanned(len(summaries))
            logger.info("Found %s clusters in %s", len(summaries), region)

            # Get details for each cluster
            for summary in summaries:
                try:
                    detail = client.get_cluster_detail(region, summary.cluster_id)
                    all_clusters.append(detail)
                except Exception as e:
                    logger.error(
                        "Failed to get details for cluster %s: %s",
                        summary.cluster_id,
                        redact_error_message(str(e)),
                    )

        except Exception as e:
            logger.error(
                "Failed to list clusters in %s: %s",
                region,
                redact_error_message(str(e)),
            )

    if not all_clusters and config.regions:
        logger.warning(
            "No clusters discovered across all regions. "
            "API may be unreachable or credentials may be invalid."
        )

    return all_clusters


def run(
    config: AppConfig,
    client: PolarDBClient,
    shutdown_event: threading.Event,
    logger: logging.Logger,
) -> tuple[int, ExecutionReport | None]:
    """
    Run the resizer logic.

    Args:
        config: Application configuration
        client: PolarDB client (rate-limited)
        shutdown_event: Event for graceful shutdown
        logger: Logger instance

    Returns:
        Tuple of (exit_code, execution_report)
    """
    metrics = get_metrics()

    # Discover clusters
    logger.info("Starting cluster discovery")
    all_clusters = discover_clusters(client, config, logger)
    logger.info("Discovered %s total clusters", len(all_clusters))

    # Filter to target clusters
    target_clusters = select_target_clusters(all_clusters, config)
    metrics.clusters_targeted = len(target_clusters)
    metrics.clusters_filtered = len(all_clusters) - len(target_clusters)

    logger.info(
        "Selected %d target clusters (filtered %d)",
        len(target_clusters),
        metrics.clusters_filtered,
    )

    # Check for shutdown before proceeding
    if shutdown_event.is_set():
        logger.warning("Shutdown requested, exiting early")
        return EXIT_SIGNAL_INTERRUPT, None

    # Generate change plans
    logger.info("Generating change plans")
    change_plans = plan_changes(target_clusters, config)
    metrics.changes_planned = len(change_plans)

    logger.info("Generated %s change plans", len(change_plans))

    if not change_plans:
        logger.info("No changes needed")
        return EXIT_SUCCESS, None

    # Apply changes
    logger.info(
        "Applying changes in %s mode",
        config.run_mode,
        extra={"run_mode": config.run_mode, "change_count": len(change_plans)},
    )

    report = apply_changes(change_plans, client, config, shutdown_event)

    # Update metrics from report
    metrics.changes_succeeded = report.total_successful
    metrics.changes_failed = report.total_failed
    metrics.changes_skipped = report.total_skipped

    # Log summary
    logger.info(
        "Execution complete",
        extra={
            "total_changes": report.total_changes,
            "successful": report.total_successful,
            "failed": report.total_failed,
            "skipped": report.total_skipped,
            "interrupted": report.interrupted,
        },
    )

    # Determine exit code
    if report.interrupted:
        return EXIT_SIGNAL_INTERRUPT, report
    elif report.total_failed > 0:
        return EXIT_PARTIAL_FAILURE, report
    else:
        return EXIT_SUCCESS, report


def main() -> int:
    """
    Main entry point for the application.

    Returns:
        Exit code:
        - 0: All successful (including no changes needed)
        - 1: At least one change failed
        - 2: Configuration error or startup validation failed
        - 3: Interrupted by signal (SIGTERM/SIGINT)
    """
    # Generate and set trace ID
    trace_id = generate_trace_id()
    set_trace_id(trace_id)

    # Load configuration
    try:
        config = AppConfig.from_env()
    except Exception as e:
        print(f"Failed to load configuration: {e}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    # Set up logging
    # In test envs, don't force handler replacement to allow
    # pytest's caplog to work

    in_test = "pytest" in sys.modules
    setup_logging(level=config.log_level, json_format=False, force=not in_test)
    logger = get_logger(__name__)

    logger.info(
        "Starting PolarDB Storage Resizer (trace_id=%s)",
        trace_id,
        extra={
            "run_mode": config.run_mode,
            "regions": config.regions,
            "trace_id": trace_id,
        },
    )

    # Validate configuration
    try:
        errors = config.validate()
        if errors:
            for error in errors:
                logger.error("Configuration error: %s", error)
            return EXIT_CONFIG_ERROR

        # Validate RSSA for apply mode
        is_valid, error_message = config.validate_rssa()
        if not is_valid:
            logger.error("RSSA validation failed: %s", error_message)
            return EXIT_CONFIG_ERROR

    except ValidationError as e:
        logger.error("Configuration validation failed: %s", e)
        return EXIT_CONFIG_ERROR

    # Reset metrics for this run
    reset_metrics()

    # Create client based on configuration
    # USE_FAKE_CLIENT=true: Use FakePolarDBClient (for testing)
    # USE_FAKE_CLIENT=false or not set: Use AliyunPolarDBClient (production)
    import os

    use_fake = os.environ.get("USE_FAKE_CLIENT", "").lower() in ("true", "1", "yes")

    if use_fake:
        # Import fake client for testing
        from polardb_storage_resizer.fake_client import FakePolarDBClient

        base_client: PolarDBClient = FakePolarDBClient()
        logger.info("Using FakePolarDBClient (testing mode)")
    else:
        # Use real Aliyun client
        from polardb_storage_resizer.aliyun_client import AliyunPolarDBClient

        base_client = AliyunPolarDBClient(
            connect_timeout=config.api_connect_timeout,
            read_timeout=config.api_read_timeout,
        )
        logger.info("Using AliyunPolarDBClient (production mode)")

    client = create_rate_limited_client(base_client, config)

    # Run with graceful shutdown handling
    with GracefulShutdown() as shutdown:
        exit_code, report = run(config, client, shutdown.shutdown_event, logger)

        # Log final metrics
        log_metrics_summary(logger)

        # Override exit code only if run() hadn't already detected interruption
        if shutdown.was_interrupted() and exit_code != EXIT_SIGNAL_INTERRUPT:
            exit_code = EXIT_SIGNAL_INTERRUPT

    logger.info("Exiting with code %s", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
