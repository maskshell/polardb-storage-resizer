"""
Cloud API abstraction layer for PolarDB Storage Resizer.

This module provides:
- PolarDBClient Protocol: Interface for PolarDB operations
- Rate-limited client wrapper
- SDK exception mapping to Transient/Permanent errors
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from polardb_storage_resizer.errors import (
    OPERATION_CONFLICT_HINTS,
    PermanentCloudAPIError,
    TransientCloudAPIError,
)
from polardb_storage_resizer.redaction import redact_sdk_error

if TYPE_CHECKING:
    from polardb_storage_resizer.config import AppConfig
    from polardb_storage_resizer.models import (
        ClusterDetail,
        ClusterSummary,
        ModifyResult,
    )


# Error codes that indicate transient (retryable) errors
# Reference: https://help.aliyun.com/zh/polardb/api-polardb-2017-08-01-errorcodes
TRANSIENT_ERROR_CODES = {
    # Network/Service errors
    "RequestTimeout",
    "InternalError",
    "ServiceUnavailable",
    "Throttling",
    "Throttling.User",
    "Throttling.System",
    "ServiceBusy",
    # Instance operation conflicts (transient - retry after operation completes)
    "IncorrectDBClusterState",  # Instance in wrong state (upgrading, modifying, etc.)
    "TaskConflict",  # Another task is running on the instance
    "InvalidTaskOperation",  # Task operation conflict (e.g., cooling period)
    "ClusterOperationInProgress",  # Cluster operation in progress
    "LockTimeout",  # Could not acquire lock on resource
}

# Error codes that indicate permanent (non-retryable) errors
PERMANENT_ERROR_CODES = {
    "UnauthorizedOperation",
    "InvalidParameter",
    "InvalidDBClusterId.NotFound",
    "InvalidDBClusterId.Malformed",
    "QuotaExceeded",
    "Forbidden",
    "AccessDenied",
    # Cluster version doesn't support storage expansion
    "OperationDenied.ModifyStorageSpace",
    "InvalidStorageSpace.Value",  # Invalid storage space value
}

# Error code patterns that indicate transient errors (prefix matching)
TRANSIENT_ERROR_PREFIXES = (
    "Throttling",
    "Service",
    "Internal",
    "Incorrect",  # IncorrectDBClusterState, IncorrectInstanceStatus, etc.
    "Task",  # TaskConflict, TaskInProgress, etc.
)


@runtime_checkable
class PolarDBClient(Protocol):
    """
    Protocol defining the PolarDB client interface.

    This protocol allows for different implementations:
    - AliyunPolarDBClient: Production implementation using Aliyun SDK
    - FakePolarDBClient: Test implementation with in-memory storage
    """

    def list_clusters(
        self,
        region: str,
        cluster_ids: list[str] | None = None,
        tag_filters: dict[str, str] | None = None,
    ) -> list[ClusterSummary]:
        """
        List all clusters in a region.

        Args:
            region: Region to list clusters from
            cluster_ids: Optional list of cluster IDs to filter (API-level filtering)
            tag_filters: Optional tag key-value pairs to filter clusters

        Returns:
            List of cluster summaries
        """
        ...

    def get_cluster_detail(self, region: str, cluster_id: str) -> ClusterDetail:
        """
        Get detailed information for a cluster.

        Args:
            region: Region where the cluster is located
            cluster_id: Cluster identifier

        Returns:
            Detailed cluster information
        """
        ...

    def modify_storage(
        self, region: str, cluster_id: str, new_size_gb: int
    ) -> ModifyResult:
        """
        Modify storage for a cluster.

        Args:
            region: Region where the cluster is located
            cluster_id: Cluster identifier
            new_size_gb: New storage size in GB

        Returns:
            Result of the modification operation
        """
        ...


class RateLimitedClient:
    """
    Rate-limited wrapper for PolarDBClient.

    Implements simple rate limiting using token bucket algorithm
    to enforce max_qps limit.
    """

    def __init__(self, client: PolarDBClient, max_qps: int) -> None:
        """
        Initialize rate-limited client.

        Args:
            client: Underlying PolarDBClient implementation
            max_qps: Maximum queries per second allowed
        """
        self._client = client
        self._max_qps = max_qps
        self._lock = threading.Lock()
        self._last_request_time: float = 0.0
        self._min_interval = 1.0 / max_qps if max_qps > 0 else 0

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to enforce rate limit.

        Uses reserve-then-sleep pattern: reserves a time slot under lock,
        then sleeps outside the lock so other threads can reserve their
        own slots concurrently.
        """
        if self._max_qps <= 0:
            return

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            sleep_duration = max(0.0, self._min_interval - elapsed)
            self._last_request_time = now + sleep_duration

        if sleep_duration > 0:
            time.sleep(sleep_duration)

    def list_clusters(
        self,
        region: str,
        cluster_ids: list[str] | None = None,
        tag_filters: dict[str, str] | None = None,
    ) -> list[ClusterSummary]:
        """List clusters with rate limiting."""
        self._wait_for_rate_limit()
        return self._client.list_clusters(region, cluster_ids, tag_filters)

    def get_cluster_detail(self, region: str, cluster_id: str) -> ClusterDetail:
        """Get cluster detail with rate limiting."""
        self._wait_for_rate_limit()
        return self._client.get_cluster_detail(region, cluster_id)

    def modify_storage(
        self, region: str, cluster_id: str, new_size_gb: int
    ) -> ModifyResult:
        """Modify storage with rate limiting."""
        self._wait_for_rate_limit()
        return self._client.modify_storage(region, cluster_id, new_size_gb)


def classify_sdk_error(
    error: Exception,
    error_code: str | None = None,
) -> TransientCloudAPIError | PermanentCloudAPIError:
    """
    Classify an SDK error as transient or permanent.

    Transient errors (should retry):
    - Network/timeout errors
    - Service throttling
    - Instance operation conflicts (upgrading, modifying, etc.)
    - Temporary service unavailability

    Permanent errors (should not retry):
    - Authentication/authorization failures
    - Invalid parameters
    - Resource not found
    - Quota exceeded

    Args:
        error: Original SDK exception
        error_code: Error code from the SDK response

    Returns:
        Either TransientCloudAPIError or PermanentCloudAPIError
    """
    # Get error code from parameter or try to extract from error
    code = error_code
    if code is None and hasattr(error, "error_code"):
        code = error.error_code
    if code is None and hasattr(error, "code"):
        code = error.code

    # Redact the error message
    redacted_message = redact_sdk_error(error)

    # Check if error code indicates transient error
    if code:
        # Exact match for transient codes
        if code in TRANSIENT_ERROR_CODES:
            return TransientCloudAPIError(
                message=redacted_message,
                error_code=code,
                original_error=error,
            )

        # Exact match for permanent codes
        if code in PERMANENT_ERROR_CODES:
            return PermanentCloudAPIError(
                message=redacted_message,
                error_code=code,
                original_error=error,
            )

        # Check for transient error code prefixes
        for prefix in TRANSIENT_ERROR_PREFIXES:
            if code.startswith(prefix):
                return TransientCloudAPIError(
                    message=redacted_message,
                    error_code=code,
                    original_error=error,
                )

    # Default: check error message for hints
    error_str = str(error).lower()

    # Instance operation conflict hints (transient)
    if any(hint in error_str for hint in OPERATION_CONFLICT_HINTS):
        return TransientCloudAPIError(
            message=redacted_message,
            error_code=code or "OperationConflict",
            original_error=error,
        )

    # Network/timeout errors are transient
    if any(
        hint in error_str
        for hint in ("timeout", "connection", "network", "unavailable", "retry")
    ):
        return TransientCloudAPIError(
            message=redacted_message,
            error_code=code,
            original_error=error,
        )

    # Auth/permission errors are permanent
    if any(
        hint in error_str
        for hint in ("unauthorized", "forbidden", "denied", "not found")
    ):
        return PermanentCloudAPIError(
            message=redacted_message,
            error_code=code,
            original_error=error,
        )

    # Default to permanent for safety (don't retry unknown errors)
    return PermanentCloudAPIError(
        message=redacted_message,
        error_code=code,
        original_error=error,
    )


def create_rate_limited_client(
    client: PolarDBClient,
    config: AppConfig,
) -> RateLimitedClient:
    """
    Create a rate-limited client wrapper.

    Args:
        client: Underlying PolarDBClient implementation
        config: Application configuration with max_qps setting

    Returns:
        RateLimitedClient instance
    """
    return RateLimitedClient(client, config.max_qps)
