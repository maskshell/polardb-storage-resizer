"""
Executor for PolarDB Storage Resizer.

This module provides:
- plan_changes: Generate change plans from target clusters
- apply_changes: Execute changes with dry-run, retry, and graceful shutdown
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from polardb_storage_resizer.errors import (
    OPERATION_CONFLICT_HINTS,
    PermanentCloudAPIError,
    TransientCloudAPIError,
)
from polardb_storage_resizer.models import (
    ChangePlan,
    ChangeResult,
    ClusterDetail,
    ExecutionReport,
    ModifyResult,
)
from polardb_storage_resizer.strategy import (
    compute_target_storage,
    validate_storage_constraints,
)

if TYPE_CHECKING:
    from polardb_storage_resizer.cloud_client import PolarDBClient
    from polardb_storage_resizer.config import AppConfig


@dataclass
class _ExecutionContext:
    """Internal context for tracking execution state."""

    successful: list[ChangeResult] = field(default_factory=list)
    failed: list[ChangeResult] = field(default_factory=list)
    skipped: list[ChangeResult] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


def plan_changes(
    target_clusters: list[ClusterDetail],
    config: AppConfig,
) -> list[ChangePlan]:
    """
    Generate change plans for target clusters.

    This function combines the output of strategy functions to create
    a list of planned storage changes.

    Args:
        target_clusters: List of clusters to process (already filtered)
        config: Application configuration

    Returns:
        List of ChangePlan objects for clusters that need changes
    """
    import logging

    logger = logging.getLogger(__name__)
    plans: list[ChangePlan] = []

    for cluster in target_clusters:
        try:
            target_storage = compute_target_storage(cluster, config)
        except Exception as e:
            logger.error(
                "Unexpected error computing target for %s: %s",
                cluster.cluster_id,
                getattr(e, "message", str(e)),
            )
            continue

        if target_storage is None:
            # No change needed
            logger.debug(
                "No change needed for %s: used=%.1fGB, prov=%dGB",
                cluster.cluster_id,
                cluster.used_storage_gb,
                cluster.provisioned_storage_gb,
            )
            continue

        # Validate and adjust for API constraints
        validated_target = validate_storage_constraints(target_storage, cluster, config)

        # After alignment, target might be None (no change needed)
        if validated_target is None:
            logger.debug(
                "No change after alignment for %s: used=%.1fGB, prov=%dGB",
                cluster.cluster_id,
                cluster.used_storage_gb,
                cluster.provisioned_storage_gb,
            )
            continue

        # Create change plan
        plan = ChangePlan(
            cluster_id=cluster.cluster_id,
            region=cluster.region,
            current_storage_gb=cluster.provisioned_storage_gb,
            target_storage_gb=validated_target,
            reason=f"Used: {cluster.used_storage_gb}GB, Target: {validated_target}GB",
        )

        # Log the plan
        change_type = (
            "expand" if validated_target > cluster.provisioned_storage_gb else "shrink"
        )
        change_gb = abs(validated_target - cluster.provisioned_storage_gb)
        logger.info(
            "Plan: %s %s %dGB (%dGB -> %dGB) [used=%.1fGB]",
            cluster.cluster_id,
            change_type,
            change_gb,
            cluster.provisioned_storage_gb,
            validated_target,
            cluster.used_storage_gb,
        )
        plans.append(plan)

    return plans


def apply_changes(
    change_plans: list[ChangePlan],
    client: PolarDBClient,
    config: AppConfig,
    shutdown_event: threading.Event | None = None,
) -> ExecutionReport:
    """
    Apply change plans using the provided client.

    Args:
        change_plans: List of changes to apply
        client: PolarDB client for making API calls
        config: Application configuration
        shutdown_event: Event to signal graceful shutdown

    Returns:
        ExecutionReport with results of all changes
    """
    import logging

    logger = logging.getLogger(__name__)
    context = _ExecutionContext()

    if config.run_mode == "dry-run":
        # In dry-run mode, just record plans without calling API
        for plan in change_plans:
            result = ChangeResult(
                plan=plan,
                success=True,
                attempts=0,
                final_result=ModifyResult(
                    success=True,
                    cluster_id=plan.cluster_id,
                    old_storage_gb=plan.current_storage_gb,
                    new_storage_gb=plan.target_storage_gb,
                ),
            )
            context.skipped.append(result)

        return ExecutionReport(
            total_clusters=len(change_plans),
            total_changes=len(change_plans),
            total_successful=0,
            total_failed=0,
            total_skipped=len(change_plans),
            interrupted=False,
            successful=context.successful,
            failed=context.failed,
            skipped=context.skipped,
        )

    # Apply mode: execute changes
    interrupted = False

    # Use ThreadPoolExecutor for concurrent execution.
    # IMPORTANT: Each task runs independently - errors in one task
    # do NOT affect other tasks. This ensures that if one instance
    # fails (e.g., due to upgrade in progress), other instances
    # will still be processed.
    with ThreadPoolExecutor(max_workers=config.max_parallel_requests) as executor:
        futures: dict[Any, ChangePlan] = {}

        for plan in change_plans:
            # Check for shutdown before submitting
            if shutdown_event is not None and shutdown_event.is_set():
                interrupted = True
                # Mark remaining as skipped
                result = ChangeResult(
                    plan=plan,
                    success=False,
                    attempts=0,
                    error=Exception("Shutdown signal received"),
                )
                context.skipped.append(result)
                continue

            future = executor.submit(
                _execute_single_change,
                plan,
                client,
                config,
                shutdown_event,
            )
            futures[future] = plan

        # Collect results
        for future in as_completed(futures):
            plan = futures[future]
            try:
                result = future.result()
                with context.lock:
                    if result.success:
                        logger.info(
                            "Change succeeded: %s (%s -> %sGB)",
                            plan.cluster_id,
                            plan.current_storage_gb,
                            plan.target_storage_gb,
                        )
                        context.successful.append(result)
                    else:
                        error_msg = (
                            str(result.error) if result.error else "Unknown error"
                        )
                        logger.error(
                            "Change failed: %s - %s", plan.cluster_id, error_msg
                        )
                        context.failed.append(result)
            except Exception as e:
                result = ChangeResult(
                    plan=plan,
                    success=False,
                    attempts=1,
                    error=e,
                )
                logger.error("Change exception: %s - %s", plan.cluster_id, e)
                with context.lock:
                    context.failed.append(result)

    return ExecutionReport(
        total_clusters=len(change_plans),
        total_changes=len(change_plans),
        total_successful=len(context.successful),
        total_failed=len(context.failed),
        total_skipped=len(context.skipped),
        interrupted=interrupted
        or (shutdown_event is not None and shutdown_event.is_set()),
        successful=context.successful,
        failed=context.failed,
        skipped=context.skipped,
    )


def _execute_single_change(
    plan: ChangePlan,
    client: PolarDBClient,
    config: AppConfig,
    shutdown_event: threading.Event | None,
) -> ChangeResult:
    """
    Execute a single change with retry logic.

    Handles transient errors (network, throttling, instance operations) with
    exponential backoff retry. Permanent errors fail immediately.

    IMPORTANT: This function MUST always return a ChangeResult and never
    raise an exception. This ensures error isolation - one instance's
    failure does not affect other instances being processed concurrently.

    Args:
        plan: Change plan to execute
        client: PolarDB client
        config: Application configuration
        shutdown_event: Shutdown signal

    Returns:
        ChangeResult with execution outcome (never raises)
    """
    import logging

    logger = logging.getLogger(__name__)
    attempts = 0
    max_attempts = config.retry_max_attempts
    backoff_base = config.retry_backoff_base
    backoff_max = config.retry_backoff_max

    last_error: Exception | None = None

    while attempts < max_attempts:
        # Check for shutdown
        if shutdown_event is not None and shutdown_event.is_set():
            return ChangeResult(
                plan=plan,
                success=False,
                attempts=attempts,
                error=Exception("Shutdown signal received"),
            )

        attempts += 1

        try:
            result = client.modify_storage(
                plan.region, plan.cluster_id, plan.target_storage_gb
            )

            if result.success:
                return ChangeResult(
                    plan=plan,
                    success=True,
                    attempts=attempts,
                    final_result=result,
                )

            # Check error message for type
            error_msg = result.error_message or "Unknown error"
            error_msg_lower = error_msg.lower()

            # Detect operation conflict errors (instance upgrading, etc.)
            is_operation_conflict = any(
                hint in error_msg_lower for hint in OPERATION_CONFLICT_HINTS
            )

            if (
                is_operation_conflict
                or "transient" in error_msg_lower
                or "timeout" in error_msg_lower
            ):
                # Treat as transient, will retry
                error_code = (
                    "OperationConflict" if is_operation_conflict else "ModifyFailed"
                )
                last_error = TransientCloudAPIError(
                    message=error_msg,
                    error_code=error_code,
                )
                logger.warning(
                    "Transient error on %s (attempt %d/%d): %s",
                    plan.cluster_id,
                    attempts,
                    max_attempts,
                    error_msg[:100],
                )
            else:
                # Treat as permanent, don't retry
                return ChangeResult(
                    plan=plan,
                    success=False,
                    attempts=attempts,
                    final_result=result,
                    error=PermanentCloudAPIError(
                        message=error_msg,
                        error_code="ModifyFailed",
                    ),
                )

        except TransientCloudAPIError as e:
            last_error = e
            logger.warning(
                "Transient error on %s (attempt %d/%d): %s",
                plan.cluster_id,
                attempts,
                max_attempts,
                getattr(e, "message", str(e))[:100],
            )
            # Will retry

        except PermanentCloudAPIError as e:
            # Don't retry permanent errors
            logger.error(
                "Permanent error on %s: %s",
                plan.cluster_id,
                getattr(e, "message", str(e))[:100],
            )
            return ChangeResult(
                plan=plan,
                success=False,
                attempts=attempts,
                error=e,
            )

        except Exception as e:
            # Unknown error - treat as permanent for safety
            logger.error("Unexpected error on %s: %s", plan.cluster_id, e)
            return ChangeResult(
                plan=plan,
                success=False,
                attempts=attempts,
                error=PermanentCloudAPIError(
                    message=str(e),
                    original_error=e,
                ),
            )

        # If we get here and haven't exceeded max attempts, wait before retry
        if attempts < max_attempts:
            # Exponential backoff
            backoff = min(backoff_base * (2 ** (attempts - 1)), backoff_max)

            # For operation conflicts (like instance upgrading), use longer backoff
            if (
                last_error
                and isinstance(last_error, TransientCloudAPIError)
                and last_error.error_code == "OperationConflict"
            ):
                # Use at least 30 seconds for operation conflicts
                backoff = max(backoff, 30.0)
                logger.info(
                    "Operation conflict on %s, waiting %.1fs before retry",
                    plan.cluster_id,
                    backoff,
                )

            logger.info(
                "Retrying %s in %.1fs (attempt %d/%d)",
                plan.cluster_id,
                backoff,
                attempts + 1,
                max_attempts,
            )

            # Check for shutdown during backoff
            if shutdown_event is not None:
                # Sleep in small increments to check shutdown
                elapsed = 0.0
                while elapsed < backoff:
                    sleep_time = min(0.1, backoff - elapsed)
                    if shutdown_event.is_set():
                        return ChangeResult(
                            plan=plan,
                            success=False,
                            attempts=attempts,
                            error=Exception("Shutdown signal received"),
                        )
                    time.sleep(sleep_time)
                    elapsed += sleep_time
            else:
                time.sleep(backoff)

    # Exhausted retries
    error_detail = (
        getattr(last_error, "message", str(last_error))
        if last_error
        else "Unknown error"
    )
    logger.error(
        "Exhausted retries for %s after %d attempts: %s",
        plan.cluster_id,
        attempts,
        error_detail,
    )
    return ChangeResult(
        plan=plan,
        success=False,
        attempts=attempts,
        error=last_error or Exception("Max retries exceeded"),
    )
