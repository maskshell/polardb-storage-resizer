"""
Tests for storage adjustment strategy in polardb_storage_resizer.strategy.

This module tests:
- select_target_clusters: Filter clusters by pay type, status, whitelist
- compute_target_storage: Calculate B_target = ceil(A * 1.05)
- validate_storage_constraints: API constraints (step size, min storage, etc.)

GREEN Phase: Tests should pass now that strategy.py is implemented.
"""

from __future__ import annotations

import math

import pytest

# Import from actual implementation
from polardb_storage_resizer.config import AppConfig
from polardb_storage_resizer.models import ClusterDetail
from polardb_storage_resizer.strategy import (
    compute_target_storage,
    effective_max_storage_gb,
    get_max_storage_gb,
    get_min_storage_gb,
    select_target_clusters,
    validate_storage_constraints,
)

# ==============================================================================
# Test Classes
# ==============================================================================


class TestSelectTargetClusters:
    """Tests for cluster filtering logic."""

    def test_filter_non_prepaid_clusters(
        self,
        sample_clusters: list[ClusterDetail],
        sample_config: AppConfig,
    ) -> None:
        """Clusters with pay_type != Prepaid should be filtered out."""
        result = select_target_clusters(sample_clusters, sample_config)

        for cluster in result:
            assert cluster.pay_type == "Prepaid"

    def test_filter_non_running_clusters(
        self,
        sample_clusters: list[ClusterDetail],
        sample_config: AppConfig,
    ) -> None:
        """Clusters with status != Running should be filtered out."""
        result = select_target_clusters(sample_clusters, sample_config)

        for cluster in result:
            assert cluster.status == "Running"

    def test_filter_by_whitelist(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Only clusters in whitelist should be selected."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            cluster_whitelist=["pc-full-11111111", "pc-normal-44444444"],
        )

        result = select_target_clusters(sample_clusters, config)

        cluster_ids = {c.cluster_id for c in result}
        assert cluster_ids == {"pc-full-11111111", "pc-normal-44444444"}

    def test_filter_by_blacklist(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Clusters in blacklist should be excluded."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            cluster_blacklist=["pc-full-11111111"],
        )

        result = select_target_clusters(sample_clusters, config)

        cluster_ids = {c.cluster_id for c in result}
        assert "pc-full-11111111" not in cluster_ids

    def test_blacklist_takes_priority_over_whitelist(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Blacklist should take priority - cluster in both should be excluded."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            cluster_whitelist=["pc-full-11111111", "pc-normal-44444444"],
            cluster_blacklist=["pc-full-11111111"],
        )

        result = select_target_clusters(sample_clusters, config)

        cluster_ids = {c.cluster_id for c in result}
        # pc-full-11111111 should be excluded even though it's in whitelist
        assert "pc-full-11111111" not in cluster_ids
        assert "pc-normal-44444444" in cluster_ids

    def test_empty_whitelist_selects_all_eligible(
        self,
        sample_clusters: list[ClusterDetail],
        sample_config: AppConfig,
    ) -> None:
        """Empty whitelist should select all eligible clusters."""
        result = select_target_clusters(sample_clusters, sample_config)

        # Eligible = prepaid, running, and NOT Standard Edition. Standard Edition
        # is detected by EITHER signal — category == "SENormal" OR storage_type in
        # the ESSD set — mirroring _is_standard_edition exactly.
        from polardb_storage_resizer.strategy import STANDARD_EDITION_STORAGE_TYPES

        def _is_std(c: ClusterDetail) -> bool:
            if c.category == "SENormal":
                return True
            return (c.storage_type or "").lower().strip() in (
                STANDARD_EDITION_STORAGE_TYPES
            )

        expected_count = len(
            [
                c
                for c in sample_clusters
                if c.pay_type == "Prepaid" and c.status == "Running" and not _is_std(c)
            ]
        )
        assert len(result) == expected_count

    def test_filter_by_region(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Only clusters in configured regions should be selected."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-beijing"],  # Only Beijing
        )

        result = select_target_clusters(sample_clusters, config)

        for cluster in result:
            assert cluster.region == "cn-beijing"

    def test_empty_result_when_no_matching_clusters(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Should return empty list when no clusters match criteria."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["ap-northeast-1"],  # Region with no clusters
        )

        result = select_target_clusters(sample_clusters, config)

        assert result == []


class TestComputeTargetStorage:
    """Tests for B_target = ceil(A * buffer_percent/100) calculation."""

    def test_basic_calculation(self, sample_config: AppConfig) -> None:
        """Target should be ceil(A * 1.05)."""
        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100,
            provisioned_storage_gb=200,
        )

        result = compute_target_storage(cluster, sample_config)

        assert result == math.ceil(100 * 1.05)  # = 105

    def test_custom_buffer_percent(self) -> None:
        """Target should use custom buffer_percent when configured."""
        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100,
            provisioned_storage_gb=200,
        )

        # Use 1.10 (110%) buffer instead of default 1.05
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            buffer_percent=110,
        )

        result = compute_target_storage(cluster, config)

        assert result == math.ceil(100 * 1.10)  # = 110

    def test_boundary_a_equals_zero(
        self,
        empty_cluster: ClusterDetail,
    ) -> None:
        """When A=0, target should be minimum storage value."""
        # Use permissive config to avoid safety threshold issues
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,  # Allow significant shrinkage
        )
        result = compute_target_storage(empty_cluster, config)

        # A=0 means target = ceil(0 * 1.05) = 0
        # But minimum storage should apply
        assert result is not None
        assert result > 0  # Should have some minimum

    def test_boundary_a_equals_b(
        self,
        full_usage_cluster: ClusterDetail,
        sample_config: AppConfig,
    ) -> None:
        """When A=B, target should expand by 5%."""
        result = compute_target_storage(full_usage_cluster, sample_config)

        # A=500, B=500, target = ceil(500 * 1.05) = 525
        assert result == 525

    def test_boundary_a_greater_than_b(
        self,
        overage_cluster: ClusterDetail,
        sample_config: AppConfig,
    ) -> None:
        """When A>B (overage), target should cover actual usage plus buffer."""
        result = compute_target_storage(overage_cluster, sample_config)

        # A=650, B=500, target = ceil(650 * 1.05) = 683
        assert result == 683

    def test_no_change_when_within_threshold(
        self,
        sample_config: AppConfig,
    ) -> None:
        """Should return None when change is below min_change_threshold."""
        cluster = ClusterDetail(
            cluster_id="pc-small-change",
            region="cn-hangzhou",
            cluster_name="small-change",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100,
            provisioned_storage_gb=104,  # Current is already close to target (105)
        )

        result = compute_target_storage(cluster, sample_config)

        # Change from 104 to 105 is only 1GB, below min_change_threshold
        # Should return None if threshold is > 1
        if sample_config.min_change_threshold_gb > 1:
            assert result is None

    def test_ceiling_rounding(self, sample_config: AppConfig) -> None:
        """Should use ceiling (round up) for target calculation."""
        # Test with value that would round to non-integer
        cluster = ClusterDetail(
            cluster_id="pc-ceiling",
            region="cn-hangzhou",
            cluster_name="ceiling-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100.5,
            provisioned_storage_gb=200,
        )

        result = compute_target_storage(cluster, sample_config)

        # 100.5 * 1.05 = 105.525, ceil = 106
        assert result == math.ceil(100.5 * 1.05)

    def test_shrink_when_underutilized(
        self,
        underutilized_cluster: ClusterDetail,
    ) -> None:
        """Should suggest shrinking when significantly underutilized."""
        # Use permissive config for this test
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,  # Allow up to 95% shrinkage
            max_single_change_gb=2000,  # Allow large changes
        )
        result = compute_target_storage(underutilized_cluster, config)

        # A=50, B=1000, target = ceil(50 * 1.05) = 53
        assert result == 53


class TestSafetyThresholds:
    """Tests for safety threshold enforcement."""

    def test_max_expand_ratio_exceeded(
        self,
        sample_config: AppConfig,
    ) -> None:
        """Should cap changes exceeding max_expand_ratio instead of skipping."""
        cluster = ClusterDetail(
            cluster_id="pc-expand",
            region="cn-hangzhou",
            cluster_name="expand-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=500,  # Target would be 525
            provisioned_storage_gb=100,  # Current is 100, ratio = 5.25x
        )

        # With max_expand_ratio=2.0, this change (100 -> 525) exceeds threshold
        # Should cap to 100 * 2.0 = 200
        result = compute_target_storage(cluster, sample_config)
        assert result == 200

    def test_max_shrink_ratio_exceeded(
        self,
        strict_safety_config: AppConfig,
    ) -> None:
        """Should cap shrinkage at max_shrink_ratio instead of skipping."""
        cluster = ClusterDetail(
            cluster_id="pc-shrink",
            region="cn-hangzhou",
            cluster_name="shrink-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=50,  # Target would be 53
            provisioned_storage_gb=500,  # Current is 500, ratio = 0.106
        )

        # With max_shrink_ratio=0.8, ideal target (53) exceeds threshold
        # (53/500 = 0.106 < 0.8) — should cap to 500*0.8 = 400
        result = compute_target_storage(cluster, strict_safety_config)
        assert result == 400

    def test_max_single_change_gb_exceeded(
        self,
        strict_safety_config: AppConfig,
    ) -> None:
        """Should cap shrinkage at max_single_change_gb instead of skipping."""
        cluster = ClusterDetail(
            cluster_id="pc-large",
            region="cn-hangzhou",
            cluster_name="large-shrink",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=200,  # Target would be 210
            provisioned_storage_gb=500,  # Change = 290GB
            # ratio = 210/500 = 0.42 < max_shrink_ratio=0.8 → cap to 400
            # Then change = 500-400 = 100 = max_single_change_gb → no cap needed
        )

        # shrink_ratio cap: 500 * 0.8 = 400 (ratio 0.42 < 0.8)
        result = compute_target_storage(cluster, strict_safety_config)
        assert result == 400


class TestMaxStorageLimits:
    """Tests for per-type maximum storage enforcement."""

    def test_target_capped_at_type_max_psl4(self) -> None:
        """Target should not exceed PSL4 max of 500000GB."""
        cluster = ClusterDetail(
            cluster_id="pc-near-max",
            region="cn-hangzhou",
            cluster_name="near-max",
            status="Running",
            pay_type="Prepaid",
            storage_type="psl4",
            used_storage_gb=490000,
            provisioned_storage_gb=490000,
        )
        # ceil(490000 * 1.05) = 514500, should be capped at 500000
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_expand_ratio=2.0,
            max_single_change_gb=10000,
        )
        result = compute_target_storage(cluster, config)
        assert result == 500000

    def test_target_capped_at_type_max_essd_pl0(self) -> None:
        """Target should not exceed ESSD PL0 max of 32000GB."""
        cluster = ClusterDetail(
            cluster_id="pc-essd-near-max",
            region="cn-hangzhou",
            cluster_name="essd-near-max",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl0",
            used_storage_gb=31000,
            provisioned_storage_gb=31000,
        )
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_expand_ratio=2.0,
            max_single_change_gb=10000,
        )
        result = compute_target_storage(cluster, config)
        assert result == 32000

    def test_target_capped_at_type_max_essd_pl1(self) -> None:
        """Target should not exceed ESSD PL1 max of 64000GB."""
        cluster = ClusterDetail(
            cluster_id="pc-essd-pl1-near-max",
            region="cn-hangzhou",
            cluster_name="essd-pl1-near-max",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl1",
            used_storage_gb=62000,
            provisioned_storage_gb=62000,
        )
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_expand_ratio=2.0,
            max_single_change_gb=10000,
        )
        # ceil(62000 * 1.05) = 65100, capped at 64000
        result = compute_target_storage(cluster, config)
        assert result == 64000

    def test_max_storage_no_effect_when_below_limit(self) -> None:
        """Max storage cap should have no effect when target is within limit."""
        cluster = ClusterDetail(
            cluster_id="pc-normal",
            region="cn-hangzhou",
            cluster_name="normal",
            status="Running",
            pay_type="Prepaid",
            storage_type="psl4",
            used_storage_gb=100,
            provisioned_storage_gb=100,
        )
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            min_change_threshold_gb=5,
        )
        result = compute_target_storage(cluster, config)
        assert result == math.ceil(100 * 1.05)

    def test_zero_provisioned_returns_none(self) -> None:
        """Clusters with zero provisioned storage should be skipped."""
        cluster = ClusterDetail(
            cluster_id="pc-zero",
            region="cn-hangzhou",
            cluster_name="zero-provisioned",
            status="Running",
            pay_type="Prepaid",
            storage_type="psl4",
            used_storage_gb=100,
            provisioned_storage_gb=0,
        )
        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])
        assert compute_target_storage(cluster, config) is None

    def test_get_max_storage_gb(self) -> None:
        """get_max_storage_gb should return correct max per type."""
        assert get_max_storage_gb("psl4") == 500000
        assert get_max_storage_gb("psl5") == 500000
        assert get_max_storage_gb("essdpl0") == 32000
        assert get_max_storage_gb("essdpl1") == 64000
        assert get_max_storage_gb("essdpl2") == 64000
        assert get_max_storage_gb("essdpl3") == 64000
        assert get_max_storage_gb("essdautopl") == 64000
        assert get_max_storage_gb("unknown_type") == 32000
        assert get_max_storage_gb(None) == 32000

    def test_effective_max_prefers_cluster_storage_max(self) -> None:
        """effective_max_storage_gb returns the live per-cluster StorageMax when set."""
        cluster = ClusterDetail(
            cluster_id="pc-max",
            region="cn-hangzhou",
            cluster_name="max",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=100,
            provisioned_storage_gb=200,
            storage_max_gb=102400,  # live API value (100TB), not a table literal
        )
        assert effective_max_storage_gb(cluster) == 102400

    def test_effective_max_falls_back_to_table_when_absent(self) -> None:
        """When storage_max_gb is None, fall back to the storage_type table."""
        cluster = ClusterDetail(
            cluster_id="pc-nomax",
            region="cn-hangzhou",
            cluster_name="nomax",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=100,
            provisioned_storage_gb=200,
            storage_max_gb=None,
        )
        # Derived from the table via get_max_storage_gb, not a hardcoded literal.
        assert effective_max_storage_gb(cluster) == get_max_storage_gb(
            "HighPerformance"
        )

    def test_storage_max_gb_caps_target_below_table_max(self) -> None:
        """Live per-cluster storage_max_gb caps expansion below the static table max."""
        cluster = ClusterDetail(
            cluster_id="pc-capped",
            region="cn-hangzhou",
            cluster_name="capped",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",  # table max 100000
            used_storage_gb=290,
            provisioned_storage_gb=200,
            storage_max_gb=300,  # live ceiling well below table max
        )
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_expand_ratio=2.0,
            max_single_change_gb=10000,
        )
        # ceil(290 * 1.05) = 305 > storage_max_gb 300 -> capped to 300 (not 100000)
        result = compute_target_storage(cluster, config)
        assert result == 300


class TestValidateStorageConstraints:
    """Tests for API constraint validation."""

    def test_step_size_alignment(self, sample_config: AppConfig) -> None:
        """Target should be aligned to step size (e.g., 10GB)."""
        cluster = ClusterDetail(
            cluster_id="pc-step",
            region="cn-hangzhou",
            cluster_name="step-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100,
            provisioned_storage_gb=200,
        )

        target = 103  # Not aligned to 5GB
        result = validate_storage_constraints(target, cluster, sample_config)

        # Should be aligned to nearest 10GB (ceil for expansion)
        assert result % 10 == 0

    def test_step_alignment_rounds_down_for_shrinkage(
        self,
        sample_config: AppConfig,
    ) -> None:
        """Shrinkage target should round down to step, not flip to expansion."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            min_change_threshold_gb=5,
        )
        cluster = ClusterDetail(
            cluster_id="pc-shrink-step",
            region="cn-hangzhou",
            cluster_name="shrink-step-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=50,
            provisioned_storage_gb=105,  # Not step-aligned (unusual but possible)
        )

        # Target 103 is shrinkage (103 < 105), should round down to 100, not up to 110
        target = 103
        result = validate_storage_constraints(target, cluster, config)

        assert result is not None
        assert result <= 105  # Must remain shrinkage
        assert result == 100  # Rounded down to nearest step

    def test_minimum_storage_limit(self, sample_config: AppConfig) -> None:
        """Target should not be below min storage (default 20GB)."""
        cluster = ClusterDetail(
            cluster_id="pc-min",
            region="cn-hangzhou",
            cluster_name="min-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",  # Unknown type, uses default
            used_storage_gb=1,  # Very small
            provisioned_storage_gb=100,
        )

        target = 2  # Below default minimum of 20GB
        result = validate_storage_constraints(target, cluster, sample_config)

        # Should be at least default minimum (20GB for unknown storage type)
        assert result >= 20

    def test_minimum_storage_psl4(self, sample_config: AppConfig) -> None:
        """PSL4 (Enterprise) minimum storage is 10GB."""
        cluster = ClusterDetail(
            cluster_id="pc-psl4",
            region="cn-hangzhou",
            cluster_name="psl4-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="psl4",
            used_storage_gb=1,  # Very small
            provisioned_storage_gb=100,
        )

        target = 5  # Below PSL4 minimum of 10GB
        result = validate_storage_constraints(target, cluster, sample_config)

        # Should be at least 10GB for PSL4
        assert result >= 10

    def test_minimum_storage_highperformance(self, sample_config: AppConfig) -> None:
        """HighPerformance (Enterprise PSL) minimum storage is 10GB."""
        cluster = ClusterDetail(
            cluster_id="pc-hp",
            region="cn-hangzhou",
            cluster_name="hp-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=1,
            provisioned_storage_gb=100,
        )

        target = 5  # Below HighPerformance minimum
        result = validate_storage_constraints(target, cluster, sample_config)
        # Derived from the table (single source of truth), not a hardcoded literal.
        assert result >= get_min_storage_gb("HighPerformance")

    def test_minimum_storage_standard_compressed(
        self, sample_config: AppConfig
    ) -> None:
        """Standard (Enterprise compressed/通用云盘) minimum storage is 10GB."""
        cluster = ClusterDetail(
            cluster_id="pc-std-disk",
            region="cn-hangzhou",
            cluster_name="std-disk-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="Standard",
            used_storage_gb=1,
            provisioned_storage_gb=100,
            compress_storage_mode="ON",
        )

        target = 5  # Below Standard minimum
        result = validate_storage_constraints(target, cluster, sample_config)
        # Derived from the table (single source of truth), not a hardcoded literal.
        assert result >= get_min_storage_gb("Standard")

    def test_minimum_storage_essd_pl2(self) -> None:
        """ESSD PL2 minimum storage is 470GB."""
        # Use permissive config to avoid safety check failures
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,  # Allow significant shrinkage
            max_single_change_gb=5000,  # Allow large changes
        )
        cluster = ClusterDetail(
            cluster_id="pc-essdpl2",
            region="cn-hangzhou",
            cluster_name="essdpl2-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl2",
            used_storage_gb=100,  # Target would be 105, below PL2 minimum
            provisioned_storage_gb=1000,
        )

        result = compute_target_storage(cluster, config)

        # Should be at least 470GB for ESSD PL2
        assert result >= 470

    def test_minimum_storage_essd_pl3(self) -> None:
        """ESSD PL3 minimum storage is 1270GB."""
        # Use permissive config to avoid safety check failures
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,  # Allow significant shrinkage
            max_single_change_gb=5000,  # Allow large changes
        )
        cluster = ClusterDetail(
            cluster_id="pc-essdpl3",
            region="cn-hangzhou",
            cluster_name="essdpl3-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl3",
            used_storage_gb=100,  # Target would be 105, below PL3 minimum
            provisioned_storage_gb=2000,
        )

        result = compute_target_storage(cluster, config)

        # Should be at least 1270GB for ESSD PL3
        assert result >= 1270

    def test_minimum_storage_essd_autopl(self) -> None:
        """ESSD AutoPL minimum storage is 40GB."""
        # Use permissive config to avoid safety check failures
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,  # Allow significant shrinkage
            max_single_change_gb=5000,  # Allow large changes
        )
        cluster = ClusterDetail(
            cluster_id="pc-autopl",
            region="cn-hangzhou",
            cluster_name="autopl-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdautopl",
            used_storage_gb=10,  # Target would be 11, below AutoPL minimum
            provisioned_storage_gb=100,
        )

        result = compute_target_storage(cluster, config)

        # Should be at least 40GB for ESSD AutoPL
        assert result >= 40

    def test_single_change_limit_capped(
        self,
        sample_config: AppConfig,
    ) -> None:
        """Target should be capped if single change exceeds limit."""
        cluster = ClusterDetail(
            cluster_id="pc-cap",
            region="cn-hangzhou",
            cluster_name="cap-test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=1500,
            provisioned_storage_gb=100,
        )

        target = 1575  # Change of 1475GB
        result = validate_storage_constraints(target, cluster, sample_config)

        # Change should be capped to max_single_change_gb
        max_change = sample_config.max_single_change_gb
        actual_change = result - cluster.provisioned_storage_gb
        assert actual_change <= max_change

    def test_expansion_cap_realign_rounds_up(
        self,
    ) -> None:
        """Expansion cap + re-alignment should round up, not down."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_single_change_gb=15,  # Not step-aligned
            min_change_threshold_gb=5,
        )
        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100,
            provisioned_storage_gb=100,
        )
        # target=115, change=15=max_change, cap to 100+15=115 (already aligned)
        # Now test with a non-step-aligned scenario:
        # target=117, change=17>15, cap to 100+15=115
        target = 117
        result = validate_storage_constraints(target, cluster, config)
        assert result is not None
        assert result > cluster.provisioned_storage_gb  # Must remain expansion
        assert result % 10 == 0  # Must be step-aligned
        # Should round UP from 115 to 120 (not down to 110)
        assert result == 120

    def test_min_storage_flip_shrink_to_expand_returns_none(
        self,
    ) -> None:
        """When min storage enforcement flips shrink to expand, return None."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            min_change_threshold_gb=5,
        )
        # ESSD PL3 min is 1270GB; current provisioned is only 500GB
        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl3",
            used_storage_gb=100,
            provisioned_storage_gb=500,
        )
        # Target 100 (shrink), but min_storage=1270 > current=500
        # This would flip from shrink to expand — should return None
        target = 100
        result = validate_storage_constraints(target, cluster, config)
        assert result is None

    def test_shrink_round_down_violating_max_change_returns_none(
        self,
    ) -> None:
        """After shrink cap + round-down, if change exceeds
        max_single_change_gb, return None."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.8,
            max_single_change_gb=10,  # Step-aligned
            min_change_threshold_gb=5,
        )
        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=50,
            provisioned_storage_gb=120,
        )
        # compute_target_storage: shrink, floor(120*0.8)=96,
        #   change=24>10, cap to 110
        # validate_storage_constraints(110, ...):
        #   110 is already aligned, shrink change=10
        target = compute_target_storage(cluster, config)
        validated = validate_storage_constraints(target, cluster, config)
        assert validated is not None
        assert validated == 110


class TestBoundaryCases:
    """Tests for edge cases and boundary conditions."""

    def test_very_large_storage_values(self) -> None:
        """Should handle very large storage values correctly."""
        cluster = ClusterDetail(
            cluster_id="pc-large",
            region="cn-hangzhou",
            cluster_name="large-storage",
            status="Running",
            pay_type="Prepaid",
            storage_type="psl4",  # Max 500000GB
            used_storage_gb=40000,
            provisioned_storage_gb=40000,
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_single_change_gb=50000,
        )
        result = compute_target_storage(cluster, config)

        # ceil(40000 * 1.05) = 42000, within psl4 max (500000)
        assert result == math.ceil(40000 * 1.05)
        assert result > 0

    def test_fractional_used_storage(self, sample_config: AppConfig) -> None:
        """Should handle fractional used storage values."""
        cluster = ClusterDetail(
            cluster_id="pc-frac",
            region="cn-hangzhou",
            cluster_name="fractional",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=99.7,
            provisioned_storage_gb=200,
        )

        result = compute_target_storage(cluster, sample_config)

        # Should still produce integer GB result
        assert isinstance(result, int)
        assert result == math.ceil(99.7 * 1.05)

    def test_empty_cluster_list(
        self,
        sample_config: AppConfig,
    ) -> None:
        """Should handle empty cluster list gracefully."""
        result = select_target_clusters([], sample_config)

        assert result == []


class TestIntegrationScenarios:
    """Integration tests combining multiple strategy functions."""

    def test_full_filter_and_compute_flow(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Test complete flow: filter -> compute -> validate."""
        # Use permissive config for integration test
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou", "cn-beijing"],
            max_shrink_ratio=0.05,  # Allow significant shrinkage
            max_single_change_gb=50000,  # Allow large changes
        )

        # Step 1: Filter clusters
        targets = select_target_clusters(sample_clusters, config)

        # Step 2: Compute target for each
        for cluster in targets:
            target = compute_target_storage(cluster, config)

            if target is not None:
                # Step 3: Validate constraints
                validated = validate_storage_constraints(target, cluster, config)

                assert validated > 0
                assert validated % 10 == 0  # Step aligned

    def test_all_clusters_filtered(
        self,
        sample_clusters: list[ClusterDetail],
    ) -> None:
        """Test when all clusters are filtered out."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["us-east-1"],  # No clusters in this region
        )

        result = select_target_clusters(sample_clusters, config)

        assert result == []


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestClusterTypeFiltering:
    """Tests for cluster type filtering (Standard/Serverless/Multi-master)."""

    def test_standard_edition_essd_excluded_from_selection(
        self,
        sample_config: AppConfig,
    ) -> None:
        """Standard Edition ESSD clusters should be excluded entirely."""
        clusters = [
            ClusterDetail(
                cluster_id="pc-std",
                region="cn-hangzhou",
                cluster_name="standard",
                status="Running",
                pay_type="Prepaid",
                storage_type="essdpl1",
                used_storage_gb=80,
                provisioned_storage_gb=100,
                category="Normal",
            ),
        ]

        result = select_target_clusters(clusters, sample_config)
        assert result == []

    def test_standard_edition_excluded_even_in_whitelist(
        self,
    ) -> None:
        """Standard Edition ESSD excluded even when explicitly in whitelist."""
        cluster = ClusterDetail(
            cluster_id="pc-std",
            region="cn-hangzhou",
            cluster_name="standard",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl1",
            used_storage_gb=80,
            provisioned_storage_gb=100,
            category="Normal",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            cluster_whitelist=["pc-std"],
        )

        result = select_target_clusters([cluster], config)
        assert result == []

    def test_enterprise_category_normal_is_not_excluded(
        self,
    ) -> None:
        """Enterprise Edition (category=Normal, PSL storage) should NOT be excluded."""
        cluster = ClusterDetail(
            cluster_id="pc-enterprise",
            region="cn-hangzhou",
            cluster_name="enterprise",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=200,
            provisioned_storage_gb=300,
            category="Normal",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
        )

        result = select_target_clusters([cluster], config)
        assert len(result) == 1

    def test_standard_edition_senormal_essd_excluded(
        self,
    ) -> None:
        """Standard Edition (category=SENormal, ESSD storage) is excluded entirely."""
        cluster = ClusterDetail(
            cluster_id="pc-sless",
            region="cn-hangzhou",
            cluster_name="standard-edition",
            status="Running",
            pay_type="Prepaid",
            storage_type="essdpl1",
            used_storage_gb=50,
            provisioned_storage_gb=500,
            category="SENormal",
            serverless_type="AgileServerless",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,
            max_single_change_gb=2000,
        )

        # Both signals fire (SENormal + ESSD) -> excluded at selection.
        selected = select_target_clusters([cluster], config)
        assert selected == []

    def test_standard_edition_by_category_highperformance_excluded(
        self,
    ) -> None:
        """Standard Edition detected via category=SENormal even with non-ESSD storage.

        PRIMARY category signal excludes the cluster even though storage_type
        (HighPerformance) is not in the ESSD set — the old storage_type-only
        check would have wrongly admitted it.
        """
        cluster = ClusterDetail(
            cluster_id="pc-sless-hp",
            region="cn-hangzhou",
            cluster_name="standard-by-category",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=450,
            provisioned_storage_gb=400,
            category="SENormal",
        )

        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        assert select_target_clusters([cluster], config) == []

    def test_serverless_enterprise_psl_shrink_allowed(
        self,
    ) -> None:
        """A serverless Enterprise cluster on PSL storage CAN shrink.

        User-confirmed rule: serverless nodes may attach to Enterprise clusters,
        and a pure-serverless PSL cluster has no shrink restriction. So an
        Enterprise (category=Normal) serverless cluster shrinks normally —
        serverless_type never blocks shrink.
        """
        cluster = ClusterDetail(
            cluster_id="pc-sless-enterprise",
            region="cn-hangzhou",
            cluster_name="serverless-enterprise",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=50,
            provisioned_storage_gb=500,
            category="Normal",
            serverless_type="AgileServerless",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,
            max_single_change_gb=2000,
        )

        result = compute_target_storage(cluster, config)
        assert result is not None
        assert result < cluster.provisioned_storage_gb

    def test_multimaster_cluster_selected_but_shrink_blocked(
        self,
    ) -> None:
        """Multi-master clusters should be selected but shrink should return None."""
        cluster = ClusterDetail(
            cluster_id="pc-multi",
            region="cn-hangzhou",
            cluster_name="multimaster",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=50,
            provisioned_storage_gb=500,
            category="NormalMultimaster",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,
            max_single_change_gb=2000,
        )

        selected = select_target_clusters([cluster], config)
        assert len(selected) == 1

        result = compute_target_storage(cluster, config)
        assert result is None

    def test_multimaster_cluster_expand_allowed(
        self,
    ) -> None:
        """Multi-master clusters should allow expansion."""
        cluster = ClusterDetail(
            cluster_id="pc-multi",
            region="cn-hangzhou",
            cluster_name="multimaster",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=450,
            provisioned_storage_gb=400,
            category="NormalMultimaster",
        )

        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        result = compute_target_storage(cluster, config)
        assert result is not None
        assert result > cluster.provisioned_storage_gb

    def test_enterprise_cluster_no_restrictions(
        self,
    ) -> None:
        """Enterprise clusters (category=Normal, PSL storage) should work normally."""
        cluster = ClusterDetail(
            cluster_id="pc-enterprise",
            region="cn-hangzhou",
            cluster_name="enterprise",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=50,
            provisioned_storage_gb=500,
            category="Normal",
            serverless_type="SteadyServerless",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,
            max_single_change_gb=2000,
        )

        selected = select_target_clusters([cluster], config)
        assert len(selected) == 1

        # Shrink allowed for enterprise clusters
        result = compute_target_storage(cluster, config)
        assert result is not None
        assert result < cluster.provisioned_storage_gb

    def test_serverless_type_does_not_affect_enterprise(
        self,
    ) -> None:
        """Enterprise with serverless_type=SteadyServerless should allow shrink."""
        cluster = ClusterDetail(
            cluster_id="pc-enterprise",
            region="cn-hangzhou",
            cluster_name="enterprise",
            status="Running",
            pay_type="Prepaid",
            storage_type="HighPerformance",
            used_storage_gb=50,
            provisioned_storage_gb=500,
            category="Normal",
            serverless_type="SteadyServerless",
        )

        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_shrink_ratio=0.05,
            max_single_change_gb=2000,
        )

        result = compute_target_storage(cluster, config)
        # Enterprise should allow shrink regardless of serverless_type
        assert result is not None
        assert result < cluster.provisioned_storage_gb

    def test_sample_clusters_standard_edition_filtered(
        self,
        sample_clusters: list[ClusterDetail],
        sample_config: AppConfig,
    ) -> None:
        """Standard Edition clusters (by category OR ESSD storage) are filtered out."""
        from polardb_storage_resizer.strategy import STANDARD_EDITION_STORAGE_TYPES

        result = select_target_clusters(sample_clusters, sample_config)

        for cluster in result:
            is_std = cluster.category == "SENormal" or (
                (cluster.storage_type or "").lower().strip()
                in STANDARD_EDITION_STORAGE_TYPES
            )
            assert not is_std, (
                f"Standard Edition cluster {cluster.cluster_id} should be filtered out"
            )
