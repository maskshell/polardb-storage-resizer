"""
Fake PolarDB client for testing.

This module provides FakePolarDBClient, an in-memory implementation
of the PolarDBClient interface for use in tests and dry-run scenarios.

Moved from tests/conftest.py to src/ to avoid production→test imports.
"""

from __future__ import annotations

from typing import Any

from polardb_storage_resizer.models import (
    ClusterDetail,
    ClusterSummary,
    ModifyResult,
)


class FakePolarDBClient:
    """
    A fake PolarDB client for testing purposes.

    This implementation stores clusters in memory and allows
    configurable behavior for testing different scenarios.
    """

    def __init__(
        self,
        clusters: list[ClusterDetail] | None = None,
        modify_should_fail: bool = False,
        fail_on_cluster_ids: set[str] | None = None,
        transient_fail_count: int = 0,
    ):
        """
        Initialize the fake client.

        Args:
            clusters: Initial list of clusters
            modify_should_fail: If True, all modify operations fail
            fail_on_cluster_ids: Set of cluster IDs that should fail modify
            transient_fail_count: Number of transient failures before success
        """
        self._clusters: dict[str, ClusterDetail] = {}
        self._modify_should_fail = modify_should_fail
        self._fail_on_cluster_ids = fail_on_cluster_ids or set()
        self._transient_fail_count = transient_fail_count
        self._transient_fail_remaining = transient_fail_count

        # Track calls for verification
        self.list_clusters_calls: list[str] = []
        self.get_cluster_detail_calls: list[tuple[str, str]] = []
        self.modify_storage_calls: list[tuple[str, str, int]] = []
        # Track last list_clusters call parameters for test verification
        self._last_list_call: dict[str, Any] = {}

        if clusters:
            for cluster in clusters:
                self._clusters[cluster.cluster_id] = cluster

    def add_cluster(self, cluster: ClusterDetail) -> None:
        """Add a cluster to the fake client."""
        self._clusters[cluster.cluster_id] = cluster

    def list_clusters(
        self,
        region: str,
        cluster_ids: list[str] | None = None,
        tag_filters: dict[str, str] | None = None,
    ) -> list[ClusterSummary]:
        """List all clusters in a region.

        Args:
            region: Region to list clusters from
            cluster_ids: Optional list of cluster IDs to filter
            tag_filters: Optional tag key-value pairs to filter clusters

        Returns:
            List of cluster summaries
        """
        self.list_clusters_calls.append(region)
        # Store call parameters for test verification
        self._last_list_call = {
            "region": region,
            "cluster_ids": cluster_ids,
            "tag_filters": tag_filters,
        }

        summaries = []
        for cluster in self._clusters.values():
            if cluster.region == region:
                # Filter by cluster_ids if provided
                if cluster_ids and cluster.cluster_id not in cluster_ids:
                    continue
                summaries.append(
                    ClusterSummary(
                        cluster_id=cluster.cluster_id,
                        region=cluster.region,
                        cluster_name=cluster.cluster_name,
                        status=cluster.status,
                        pay_type=cluster.pay_type,
                    )
                )
        return summaries

    def get_cluster_detail(self, region: str, cluster_id: str) -> ClusterDetail:
        """Get detailed information for a cluster."""
        self.get_cluster_detail_calls.append((region, cluster_id))
        if cluster_id not in self._clusters:
            raise ValueError(f"Cluster {cluster_id} not found")
        return self._clusters[cluster_id]

    def modify_storage(
        self, region: str, cluster_id: str, new_size_gb: int
    ) -> ModifyResult:
        """Modify storage for a cluster."""
        self.modify_storage_calls.append((region, cluster_id, new_size_gb))

        if cluster_id not in self._clusters:
            return ModifyResult(
                success=False,
                cluster_id=cluster_id,
                old_storage_gb=0,
                new_storage_gb=new_size_gb,
                error_message=f"Cluster {cluster_id} not found",
            )

        # Handle transient failures
        if self._transient_fail_remaining > 0:
            self._transient_fail_remaining -= 1
            return ModifyResult(
                success=False,
                cluster_id=cluster_id,
                old_storage_gb=self._clusters[cluster_id].provisioned_storage_gb,
                new_storage_gb=new_size_gb,
                error_message="Transient error: timeout",
            )

        # Check if should fail
        if self._modify_should_fail or cluster_id in self._fail_on_cluster_ids:
            return ModifyResult(
                success=False,
                cluster_id=cluster_id,
                old_storage_gb=self._clusters[cluster_id].provisioned_storage_gb,
                new_storage_gb=new_size_gb,
                error_message="Permanent error: operation not allowed",
            )

        # Success: update storage
        old_size = self._clusters[cluster_id].provisioned_storage_gb
        self._clusters[cluster_id].provisioned_storage_gb = new_size_gb

        return ModifyResult(
            success=True,
            cluster_id=cluster_id,
            old_storage_gb=old_size,
            new_storage_gb=new_size_gb,
            request_id=f"req-{cluster_id}-{new_size_gb}",
        )

    def reset_call_tracking(self) -> None:
        """Reset all call tracking lists."""
        self.list_clusters_calls.clear()
        self.get_cluster_detail_calls.clear()
        self.modify_storage_calls.clear()
        self._transient_fail_remaining = self._transient_fail_count
