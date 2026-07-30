"""
Storage adjustment strategy for PolarDB Storage Resizer.

This module implements:
- select_target_clusters: Filter clusters by pay type, status, whitelist
- compute_target_storage: Calculate B_target = ceil(A * buffer_percent/100)
- validate_storage_constraints: API constraints (step size, min storage, etc.)
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from polardb_storage_resizer.config import AppConfig
    from polardb_storage_resizer.models import ClusterDetail


# Standard Edition ESSD storage types — the SECONDARY Standard-Edition signal.
# The PRIMARY signal is category == "SENormal" (see _is_standard_edition). Either
# excludes the cluster. Real DescribeDBClusterAttribute StorageType values seen:
# "HighPerformance" (Enterprise PSL), "Standard" (Enterprise compressed),
# "essdpl1" (Standard Edition ESSD PL1) — only the essd* values land in this set.
# Reference: https://help.aliyun.com/zh/polardb/api-polardb-2017-08-01-describedbclusterattribute
STANDARD_EDITION_STORAGE_TYPES = frozenset(
    {
        "essdpl0",
        "essdpl1",
        "essdpl2",
        "essdpl3",
        "essdautopl",
        "essd_pl0",
        "essd_pl1",
        "essd_pl2",
        "essd_pl3",
        "essd_autopl",
    }
)


def _is_standard_edition(cluster: ClusterDetail) -> bool:
    """Check if a cluster is Standard Edition (excluded from all operations).

    Standard Edition uses ESSD storage and is excluded entirely — PolarDB
    built-in auto-expand handles it. Detection uses TWO complementary signals
    from DescribeDBClusterAttribute:
      - PRIMARY:   category == "SENormal" (docs: SENormal = 标准版)
      - SECONDARY: storage_type in STANDARD_EDITION_STORAGE_TYPES (e.g. "essdpl1")
    Either signal excludes the cluster; the dual check is robust to either
    field being absent or labelled differently.

    Args:
        cluster: Cluster to check

    Returns:
        True if cluster is Standard Edition
    """
    if cluster.category == "SENormal":
        return True
    if not cluster.storage_type:
        return False
    return cluster.storage_type.lower().strip() in STANDARD_EDITION_STORAGE_TYPES


def _is_expand_only(cluster: ClusterDetail) -> bool:
    """Check if a cluster only supports expansion (no shrink).

    Multi-master (NormalMultimaster) clusters cannot shrink storage. Standard
    Edition (SENormal) is excluded upstream by _is_standard_edition, so it is
    intentionally NOT treated as expand-only here.

    serverless_type is NOT consulted: a serverless Enterprise cluster on PSL
    storage can still shrink, so serverless never implies expand-only.

    Args:
        cluster: Cluster to check

    Returns:
        True if cluster only supports expansion
    """
    return cluster.category == "NormalMultimaster"


# API constraints
# Reference: https://api.aliyun.com/document/polardb/2017-08-01/ModifyDBClusterStorageSpace
STORAGE_STEP_GB = 10  # Storage must be aligned to 10GB

# Minimum/maximum storage by storage type (in GB)
# Min storage source:
#   https://api.aliyun.com/document/polardb/2017-08-01/ModifyDBClusterStoragePerformance
# Max storage source (edition selection guide, last-modified 2025-06-27):
#   https://help.aliyun.com/zh/polardb/polardb-for-mysql/polardb-mysql-edition-selection-guide
#   - Standard Edition ESSD PL0: max 32TB (32000GB)
#   - Standard Edition ESSD PL1/PL2/PL3/通用云盘: max 64TB (64000GB)
#   - Enterprise Edition PSL4/PSL5: max 500TB (500000GB)
#   - Enterprise actual max depends on node spec; API rejects if exceeded
# Note: StorageType from DescribeDBClusterAttribute API is typically lowercase
STORAGE_TYPE_MIN_GB: dict[str, int] = {
    # Enterprise edition — real API StorageType labels (PSL/compressed)
    "highperformance": 10,
    "standard": 10,
    # Enterprise edition (PSL) — legacy/granular labels
    "psl5": 10,
    "psl4": 10,
    # Standard edition (ESSD)
    "essdpl0": 20,
    "essdpl1": 20,
    "essdpl2": 470,
    "essdpl3": 1270,
    "essdautopl": 40,
    # Aliases with underscores (for robustness)
    "essd_pl0": 20,
    "essd_pl1": 20,
    "essd_pl2": 470,
    "essd_pl3": 1270,
    "essd_autopl": 40,
}

# Default minimum storage when storage type is unknown or legacy
DEFAULT_MIN_STORAGE_GB = 20

# Maximum storage by storage type (in GB)
# Exceeding these limits causes ModifyDBClusterStorageSpace to fail permanently.
# Note: Enterprise edition actual max depends on node spec (100TB~500TB);
# using product-level max (500TB) here — API rejects if node spec is lower.
STORAGE_TYPE_MAX_GB: dict[str, int] = {
    # Enterprise edition — real API StorageType labels. Observed per-spec
    # StorageMax ~102400GB (100TB); 100000 used as a conservative table fallback
    # (the live StorageMax field is authoritative — see effective_max_storage_gb).
    "highperformance": 100000,
    "standard": 100000,
    # Enterprise edition (PSL): up to 500000GB (500TB)
    "psl5": 500000,
    "psl4": 500000,
    # Standard edition (ESSD): PL0 up to 32000GB (32TB),
    #   PL1/PL2/PL3/通用云盘 up to 64000GB (64TB)
    "essdpl0": 32000,
    "essdpl1": 64000,
    "essdpl2": 64000,
    "essdpl3": 64000,
    "essdautopl": 64000,
    # Aliases with underscores (for robustness)
    "essd_pl0": 32000,
    "essd_pl1": 64000,
    "essd_pl2": 64000,
    "essd_pl3": 64000,
    "essd_autopl": 64000,
}

# Default maximum storage when storage type is unknown
# Use the more restrictive Standard Edition limit as safe default
DEFAULT_MAX_STORAGE_GB = 32000


def get_min_storage_gb(storage_type: str | None) -> int:
    """
    Get the minimum storage limit based on storage type.

    Args:
        storage_type: Storage type string from API (e.g., "psl4", "essdpl1")
                     Typically lowercase from DescribeDBClusterAttribute

    Returns:
        Minimum storage in GB for the given storage type
    """
    if not storage_type:
        return DEFAULT_MIN_STORAGE_GB

    # Normalize to lowercase for lookup
    normalized = storage_type.lower().strip()
    return STORAGE_TYPE_MIN_GB.get(normalized, DEFAULT_MIN_STORAGE_GB)


def get_max_storage_gb(storage_type: str | None) -> int:
    """
    Get the maximum storage limit based on storage type.

    Args:
        storage_type: Storage type string from API (e.g., "psl4", "essdpl1")

    Returns:
        Maximum storage in GB for the given storage type
    """
    if not storage_type:
        return DEFAULT_MAX_STORAGE_GB

    normalized = storage_type.lower().strip()
    return STORAGE_TYPE_MAX_GB.get(normalized, DEFAULT_MAX_STORAGE_GB)


def effective_max_storage_gb(detail: ClusterDetail) -> int:
    """Per-cluster storage ceiling, preferring the live API value.

    The authoritative max is the per-cluster ``StorageMax`` from
    DescribeDBClusterAttribute (observed ~102400GB / 100TB for Enterprise,
    ~65500GB for Standard Edition). When the API did not return it
    (``detail.storage_max_gb`` is falsy), fall back to the static
    STORAGE_TYPE_MAX_GB table keyed by storage_type. Used in BOTH
    compute_target_storage and validate_storage_constraints so the cap is
    enforced symmetrically (previously only compute applied it).
    """
    if detail.storage_max_gb:
        return detail.storage_max_gb
    return get_max_storage_gb(detail.storage_type)


def select_target_clusters(
    clusters: list[ClusterDetail],
    config: AppConfig,
) -> list[ClusterDetail]:
    """
    Filter clusters to only those that should be processed.

    Filters:
    - Pay type must be Prepaid
    - Status must be Running
    - Must be in configured regions
    - Must match whitelist if configured
    - Must NOT be in blacklist (blacklist takes priority over whitelist)
    - Standard Edition ESSD (storage_type ∈ ESSD set) excluded entirely.
      Note: Category="Normal" is Enterprise (企业版), the primary target for
      expand+shrink — NOT Standard Edition.

    Args:
        clusters: List of cluster details to filter
        config: Application configuration

    Returns:
        Filtered list of clusters that should be processed
    """
    result: list[ClusterDetail] = []

    for cluster in clusters:
        # Filter by pay type
        if cluster.pay_type != "Prepaid":
            continue

        # Filter by status
        if cluster.status != "Running":
            continue

        # Filter by region
        if cluster.region not in config.regions:
            continue

        # Standard Edition excluded — PolarDB built-in auto-expand handles them.
        # Detection is dual-signal (category=SENormal OR storage_type ∈ ESSD).
        if _is_standard_edition(cluster):
            logger.debug(
                "Skipping Standard Edition cluster %s (category=%s, storage_type=%s)",
                cluster.cluster_id,
                cluster.category,
                cluster.storage_type,
            )
            continue

        # Filter by blacklist (takes priority - exclude if in blacklist)
        if config.cluster_blacklist and cluster.cluster_id in config.cluster_blacklist:
            continue

        # Filter by whitelist if configured
        if (
            config.cluster_whitelist
            and cluster.cluster_id not in config.cluster_whitelist
        ):
            continue

        result.append(cluster)

    return result


def compute_target_storage(
    detail: ClusterDetail,
    config: AppConfig,
) -> int | None:
    """
    Calculate target storage using B_target = ceil(A * buffer_percent/100).

    Safety thresholds are enforced by capping (not skipping):
    - Expansion exceeding max_expand_ratio is capped to current * ratio
    - Shrinkage below max_shrink_ratio is capped to current * ratio
    - Changes exceeding max_single_change_gb are capped to that limit

    Args:
        detail: Cluster detail with current storage information
        config: Application configuration

    Returns:
        Target storage in GB, or None if no change needed.
    """
    # Calculate raw target: B_target = ceil(A * buffer_percent/100)
    raw_target = math.ceil(detail.used_storage_gb * (config.buffer_percent / 100))

    # Ensure minimum storage based on storage type
    min_storage_gb = get_min_storage_gb(detail.storage_type)
    target = max(raw_target, min_storage_gb)

    current = detail.provisioned_storage_gb

    # Guard against zero or negative provisioned storage (data anomaly)
    if current <= 0:
        return None

    # Block shrink for expand-only cluster types (Multi-master only; a serverless
    # Enterprise cluster on PSL storage can still shrink — see _is_expand_only).
    if target < current and _is_expand_only(detail):
        logger.debug(
            "Skipping shrink for expand-only cluster %s "
            "(category=%s, serverless_type=%s)",
            detail.cluster_id,
            detail.category,
            detail.serverless_type,
        )
        return None

    # If target equals current, no change needed (check early for clarity)
    if target == current:
        return None

    # Safety checks — cap instead of skip to enable progressive convergence
    if target > current:
        # Expansion: cap at max_expand_ratio (ceil to not under-cut ratio)
        ratio = target / current
        if ratio > config.max_expand_ratio:
            target = math.ceil(current * config.max_expand_ratio)

        # Cap at max_single_change_gb
        change_gb = target - current
        if change_gb > config.max_single_change_gb:
            target = current + config.max_single_change_gb

        # Warn when expansion cap leaves cluster in overage billing
        if target < detail.used_storage_gb:
            logger.warning(
                "Cluster %s: target %dGB is below used %dGB — "
                "overage billing will persist after resize",
                detail.cluster_id,
                target,
                detail.used_storage_gb,
            )
    else:
        # Shrinkage: cap at max_shrink_ratio (floor to not under-cut ratio)
        ratio = target / current
        if ratio < config.max_shrink_ratio:
            target = math.floor(current * config.max_shrink_ratio)

        # Cap at max_single_change_gb
        change_gb = current - target
        if change_gb > config.max_single_change_gb:
            target = current - config.max_single_change_gb

    # Enforce per-cluster maximum storage (live StorageMax preferred, else table)
    target = min(target, effective_max_storage_gb(detail))

    # After all caps applied, check if change meets threshold
    change_gb = abs(target - current)
    if change_gb < config.min_change_threshold_gb:
        return None

    return target


def validate_storage_constraints(
    target_gb: int,
    detail: ClusterDetail,
    config: AppConfig,  # noqa: ARG001
) -> int | None:
    """
    Validate and adjust target storage to meet API constraints.

    Constraints:
    - Minimum storage limit (based on storage type)
    - Step size alignment (must be multiple of 10GB)
    - Single change limit (already validated in compute_target_storage)

    Args:
        target_gb: Proposed target storage in GB
        detail: Cluster detail (for context and storage type)
        config: Application configuration

    Returns:
        Adjusted target storage in GB, aligned to constraints.
        Returns None if no change is needed after alignment.
    """
    # Apply minimum storage based on storage type
    min_storage_gb = get_min_storage_gb(detail.storage_type)
    result = max(target_gb, min_storage_gb)

    # If min storage enforcement would flip direction (shrink → expand), abort
    current = detail.provisioned_storage_gb
    if target_gb < current and min_storage_gb >= current:
        return None

    # Align to step size: round toward current value to preserve direction.
    # Expansion: round up to avoid under-sizing.
    # Shrinkage: round down to avoid flipping direction (shrink → expand).
    if result % STORAGE_STEP_GB != 0:
        if result >= current:
            result = ((result // STORAGE_STEP_GB) + 1) * STORAGE_STEP_GB
        else:
            result = (result // STORAGE_STEP_GB) * STORAGE_STEP_GB

    # Re-check minimum after round-down alignment
    if result < min_storage_gb:
        return None

    # Cap single change if needed (defensive - should already be handled)
    max_change = config.max_single_change_gb

    if result > current:
        # Expansion
        change = result - current
        if change > max_change:
            result = current + max_change
            # Re-align to step (round up for expansion to avoid under-sizing)
            if result % STORAGE_STEP_GB != 0:
                result = ((result // STORAGE_STEP_GB) + 1) * STORAGE_STEP_GB
    elif result < current:
        # Shrinkage
        change = current - result
        if change > max_change:
            result = current - max_change
            # Re-align to step (round down for shrinkage to stay below current)
            if result % STORAGE_STEP_GB != 0:
                result = (result // STORAGE_STEP_GB) * STORAGE_STEP_GB
            # After round-down, verify cap still holds
            if current - result > max_change:
                return None

    # Enforce per-cluster maximum storage (live StorageMax preferred, else table).
    # Placed BEFORE the no-op / threshold checks so a cap that pulls the result
    # down to current (or under threshold) still exits with None rather than
    # returning a no-op target.
    result = min(result, effective_max_storage_gb(detail))

    # After alignment, check if there's actually a change needed
    # This handles cases where alignment rounds up to current value
    if result == current:
        return None

    # Also check minimum change threshold after alignment
    final_change = abs(result - current)
    if final_change < config.min_change_threshold_gb:
        return None

    return result
