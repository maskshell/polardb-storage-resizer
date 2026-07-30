"""
Data models for PolarDB Storage Resizer.

This module defines the core data structures used throughout the application:
- ClusterSummary: Basic cluster information
- ClusterDetail: Detailed cluster information with storage data
- ChangePlan: A planned storage change
- ModifyResult: Result of a storage modification
- ExecutionReport: Summary of execution results

All models use dataclasses for simplicity and type safety.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ClusterSummary:
    """
    Summary information for a PolarDB cluster.

    Used when listing clusters to get basic information before
    fetching detailed data.

    Attributes:
        cluster_id: Unique cluster identifier (e.g., "pc-xxxxxxxxx")
        region: Region where the cluster is located
        cluster_name: Human-readable cluster name
        status: Current cluster status (e.g., "Running", "Stopped")
        pay_type: Payment type ("Prepaid" or "Postpaid")
    """

    cluster_id: str
    region: str
    cluster_name: str
    status: str
    pay_type: str  # "Prepaid" or "Postpaid"


@dataclass
class ClusterDetail:
    """
    Detailed information for a PolarDB cluster.

    Contains all information needed for storage adjustment decisions,
    including current usage and provisioned storage.

    Attributes:
        cluster_id: Unique cluster identifier
        region: Region where the cluster is located
        cluster_name: Human-readable cluster name
        status: Current cluster status
        pay_type: Payment type ("Prepaid" or "Postpaid")
        storage_type: Storage type from DescribeDBClusterAttribute. Real values
            observed: "HighPerformance" (Enterprise PSL), "Standard" (Enterprise
            compressed/通用云盘), "essdpl1" (Standard Edition ESSD PL1). Used for
            min/max lookups (with static-table fallback) and as the SECONDARY
            Standard-Edition signal. NOT a reliable edition discriminator on its own.
        used_storage_gb: Current used storage in GB (A in formula)
            - For compressed clusters: this is the compressed (billing) size
            - For non-compressed clusters: this is the actual data size
        provisioned_storage_gb: Current provisioned storage in GB (B in formula)
        storage_max_gb: Per-cluster storage ceiling from API StorageMax
            (byte->GB). Authoritative max; when None the strategy falls back to
            the static STORAGE_TYPE_MAX_GB table. Observed ~102400GB (100TB) for
            Enterprise, ~65500GB for Standard Edition.
        category: Product series from API — the PRIMARY edition discriminator:
            - "Normal": Enterprise (企业版/集群版) — primary target, supports BOTH
              expand and shrink (regardless of serverless_type).
            - "NormalMultimaster": Multi-master (多主集群) — expand only (shrink
              blocked).
            - "SENormal": Standard Edition (标准版) — excluded entirely at select
              (PolarDB built-in auto-expand handles ESSD). NOT "Serverless".
            - "Basic", "Archive": other editions.
        serverless_type: Serverless compute spec — "AgileServerless",
            "SteadyServerless", or None. ORTHOGONAL to edition and to shrink
            capability: a serverless Enterprise cluster on PSL storage can still
            shrink. Never read by any resize decision (logged only).
        compress_storage_mode: Storage compression mode
            (e.g., "ON", "OFF", empty if not supported)
        raw_used_storage_gb: Raw (uncompressed) used storage in GB,
            only set when compression is enabled
        create_time: Cluster creation time (optional)
        last_modify_time: Last modification time (optional)
    """

    cluster_id: str
    region: str
    cluster_name: str
    status: str
    pay_type: str
    storage_type: str
    used_storage_gb: float  # A: Current used storage (compressed if applicable)
    provisioned_storage_gb: int  # B: Current provisioned storage
    storage_max_gb: int | None = (
        None  # Per-cluster max from API StorageMax (byte->GB); None when absent
    )
    category: str | None = None  # "Normal", "NormalMultimaster", "SENormal", etc.
    serverless_type: str | None = None  # "AgileServerless", "SteadyServerless", or None
    compress_storage_mode: str | None = None  # "ON", "OFF", or None
    raw_used_storage_gb: float | None = (
        None  # Uncompressed size (only when compression enabled)
    )
    create_time: str | None = None
    last_modify_time: str | None = None


@dataclass
class ChangePlan:
    """
    A planned storage change for a cluster.

    Represents a single storage modification that should be applied.

    Attributes:
        cluster_id: Target cluster identifier
        region: Region where the cluster is located
        current_storage_gb: Current provisioned storage
        target_storage_gb: Target storage size after modification
        reason: Human-readable reason for the change
    """

    cluster_id: str
    region: str
    current_storage_gb: int
    target_storage_gb: int
    reason: str = ""


@dataclass
class ModifyResult:
    """
    Result of a storage modification operation.

    Captures the outcome of a single modify_storage call.

    Attributes:
        success: Whether the modification succeeded
        cluster_id: Target cluster identifier
        old_storage_gb: Previous storage size
        new_storage_gb: New storage size (may equal old if failed)
        error_message: Error message if failed, None if succeeded
        request_id: API request ID for tracking
    """

    success: bool
    cluster_id: str
    old_storage_gb: int
    new_storage_gb: int
    error_message: str | None = None
    request_id: str | None = None


@dataclass
class ChangeResult:
    """
    Detailed result of a single change attempt.

    Includes information about retries and timing.

    Attributes:
        plan: Original change plan
        success: Whether the change ultimately succeeded
        attempts: Number of attempts made
        final_result: Final ModifyResult (or None if not attempted)
        error: Exception if failed
    """

    plan: ChangePlan
    success: bool
    attempts: int = 1
    final_result: ModifyResult | None = None
    error: Exception | None = None


@dataclass
class ExecutionReport:
    """
    Report of execution results.

    Aggregates results from all change attempts in a single execution.

    Attributes:
        total_clusters: Total number of clusters considered
        total_changes: Total number of changes planned
        total_successful: Number of successful changes
        total_failed: Number of failed changes
        total_skipped: Number of skipped changes
        interrupted: Whether execution was interrupted by signal
        successful: List of successful change results
        failed: List of failed change results
        skipped: List of skipped change results
    """

    total_clusters: int
    total_changes: int
    total_successful: int
    total_failed: int
    total_skipped: int
    interrupted: bool
    successful: list[ChangeResult] = field(default_factory=list)
    failed: list[ChangeResult] = field(default_factory=list)
    skipped: list[ChangeResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize report to dictionary for logging."""
        return {
            "total_clusters": self.total_clusters,
            "total_changes": self.total_changes,
            "total_successful": self.total_successful,
            "total_failed": self.total_failed,
            "total_skipped": self.total_skipped,
            "interrupted": self.interrupted,
            "successful_count": len(self.successful),
            "failed_count": len(self.failed),
            "skipped_count": len(self.skipped),
        }
