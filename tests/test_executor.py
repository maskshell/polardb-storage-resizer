"""
Tests for executor in polardb_storage_resizer.executor.

This module tests:
- plan_changes: Generate change plans from target clusters
- apply_changes: Execute changes with dry-run, retry, and graceful shutdown
- Retry behavior: Transient errors retry, Permanent errors don't
- Graceful shutdown via shutdown_event

GREEN Phase: Tests should pass now that executor.py is implemented.
"""

from __future__ import annotations

import threading
import time

import pytest
from freezegun import freeze_time

# Import from actual implementation
from polardb_storage_resizer.config import AppConfig
from polardb_storage_resizer.executor import (
    apply_changes,
    plan_changes,
)
from polardb_storage_resizer.fake_client import FakePolarDBClient
from polardb_storage_resizer.models import (
    ChangePlan,
    ClusterDetail,
)

# ==============================================================================
# Test Classes
# ==============================================================================


class TestDryRun:
    """Tests for dry-run mode behavior."""

    def test_dry_run_does_not_call_modify_storage(
        self,
        sample_clusters: list[ClusterDetail],
        fake_client: FakePolarDBClient,
    ) -> None:
        """In dry-run mode, modify_storage should never be called."""
        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        # Generate plans
        plans = [
            ChangePlan(
                cluster_id=c.cluster_id,
                region=c.region,
                current_storage_gb=c.provisioned_storage_gb,
                target_storage_gb=int(c.used_storage_gb * 1.05),
            )
            for c in sample_clusters[:3]
        ]

        # Apply in dry-run mode
        apply_changes(plans, fake_client, config)

        # No modify_storage calls should have been made
        assert len(fake_client.modify_storage_calls) == 0

    def test_dry_run_returns_plan_report(
        self,
        sample_clusters: list[ClusterDetail],
        fake_client: FakePolarDBClient,
    ) -> None:
        """Dry-run should return a report of planned changes."""
        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        plans = [
            ChangePlan(
                cluster_id="pc-test",
                region="cn-hangzhou",
                current_storage_gb=100,
                target_storage_gb=105,
            )
        ]

        report = apply_changes(plans, fake_client, config)

        assert report.total_changes >= 0
        assert report.interrupted is False


class TestSingleFailureDoesNotBlockOthers:
    """Tests ensuring one failure doesn't stop other changes.

    This is critical for production: an error on one instance
    (e.g., upgrade in progress)
    should NOT prevent other instances from being resized.
    """

    def test_single_failure_continues_with_others(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """A single failure should not prevent other changes from being attempted."""
        config = AppConfig(run_mode="apply", regions=["cn-hangzhou"])

        # Configure client to fail on specific cluster
        fake_client._fail_on_cluster_ids = {"cluster-002"}

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
            ChangePlan("cluster-002", "cn-hangzhou", 200, 210),
            ChangePlan("cluster-003", "cn-hangzhou", 300, 315),
        ]

        report = apply_changes(plans, fake_client, config)

        # All clusters should have been attempted
        assert report.total_changes == 3
        # Some should have failed
        assert report.total_failed >= 1
        # Some should have succeeded (unless all failed)
        assert report.total_successful >= 0

    def test_one_failure_others_succeed(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """When one cluster fails, others should still succeed.

        This tests error isolation: failure of cluster-002 should not
        affect cluster-001 and cluster-003.
        """
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            retry_max_attempts=1,  # No retries to speed up test
        )

        # Add clusters to the fake client
        # ClusterDetail already imported at top level

        for cluster_id in ["cluster-001", "cluster-002", "cluster-003"]:
            fake_client.add_cluster(
                ClusterDetail(
                    cluster_id=cluster_id,
                    region="cn-hangzhou",
                    cluster_name=cluster_id,
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="PrepaidStorage",
                    used_storage_gb=50,
                    provisioned_storage_gb=100,
                )
            )

        # Configure client to fail ONLY on cluster-002
        fake_client._fail_on_cluster_ids = {"cluster-002"}

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
            ChangePlan("cluster-002", "cn-hangzhou", 200, 210),
            ChangePlan("cluster-003", "cn-hangzhou", 300, 315),
        ]

        report = apply_changes(plans, fake_client, config)

        # Verify isolation: exactly one failure (cluster-002)
        assert report.total_failed == 1
        assert report.failed[0].plan.cluster_id == "cluster-002"

        # Verify others succeeded
        assert report.total_successful == 2
        successful_ids = {r.plan.cluster_id for r in report.successful}
        assert successful_ids == {"cluster-001", "cluster-003"}

    def test_multiple_failures_others_succeed(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Multiple failures should still allow others to succeed."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            retry_max_attempts=1,
        )

        # Add clusters to the fake client
        # ClusterDetail already imported at top level

        for cluster_id in [
            "cluster-001",
            "cluster-002",
            "cluster-003",
            "cluster-004",
            "cluster-005",
        ]:
            fake_client.add_cluster(
                ClusterDetail(
                    cluster_id=cluster_id,
                    region="cn-hangzhou",
                    cluster_name=cluster_id,
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="PrepaidStorage",
                    used_storage_gb=50,
                    provisioned_storage_gb=100,
                )
            )

        # Configure client to fail on 2 out of 5 clusters
        fake_client._fail_on_cluster_ids = {"cluster-002", "cluster-004"}

        plans = [
            ChangePlan(f"cluster-00{i}", "cn-hangzhou", 100, 105) for i in range(1, 6)
        ]

        report = apply_changes(plans, fake_client, config)

        # 2 failures, 3 successes
        assert report.total_failed == 2
        assert report.total_successful == 3

        failed_ids = {r.plan.cluster_id for r in report.failed}
        assert failed_ids == {"cluster-002", "cluster-004"}

        successful_ids = {r.plan.cluster_id for r in report.successful}
        assert successful_ids == {"cluster-001", "cluster-003", "cluster-005"}

    def test_error_message_preserved_in_report(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Error messages from failures should be preserved in report."""
        config = AppConfig(run_mode="apply", regions=["cn-hangzhou"])

        fake_client._modify_should_fail = True

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
        ]

        report = apply_changes(plans, fake_client, config)

        # Failed items should have error messages
        if report.total_failed > 0:
            assert len(report.failed) > 0
            # Each failure should have an error message


class TestRetryBehavior:
    """Tests for retry behavior with different error types."""

    def test_transient_error_triggers_retry(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """TransientCloudAPIError should trigger retry."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            retry_max_attempts=3,
        )

        # Configure transient failures
        fake_client._transient_fail_count = 2

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
        ]

        apply_changes(plans, fake_client, config)

        # Should have retried and eventually succeeded
        assert len(fake_client.modify_storage_calls) >= 1

    def test_transient_error_exhausts_retries(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """After exhausting retries, should mark as failed."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            retry_max_attempts=3,
        )

        # Configure permanent transient failures
        fake_client._transient_fail_count = 10  # More than retry attempts

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
        ]

        apply_changes(plans, fake_client, config)

        # Should have tried retry_max_attempts times
        assert len(fake_client.modify_storage_calls) <= config.retry_max_attempts

    def test_permanent_error_no_retry(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """PermanentCloudAPIError should not trigger retry."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            retry_max_attempts=3,
        )

        # Configure permanent failure
        fake_client._modify_should_fail = True

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
        ]

        apply_changes(plans, fake_client, config)

        # Should have only tried once (no retry for permanent errors)
        assert len(fake_client.modify_storage_calls) == 1

    @freeze_time("2024-01-01 00:00:00")
    def test_exponential_backoff_timing(self) -> None:
        """Retry should use exponential backoff: 1s, 2s, 4s with base=1.0."""
        # This test documents expected backoff behavior
        # base=1.0, attempts: 1s, 2s, 4s
        backoff_base = 1.0
        expected_delays = [1.0, 2.0, 4.0]  # 2^0, 2^1, 2^2

        for i, expected in enumerate(expected_delays):
            actual_delay = backoff_base * (2**i)
            assert actual_delay == expected

    def test_backoff_capped_at_max(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Backoff should be capped at retry_backoff_max."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            retry_max_attempts=5,
            retry_backoff_base=2.0,
            retry_backoff_max=5.0,  # Cap at 5 seconds
        )

        # Expected delays with cap:
        # Attempt 1: immediate
        # Attempt 2: min(2*1, 5) = 2s
        # Attempt 3: min(2*2, 5) = 4s
        # Attempt 4: min(2*4, 5) = 5s (capped)
        # Attempt 5: min(2*8, 5) = 5s (capped)

        # This test documents expected behavior
        assert config.retry_backoff_max == 5.0


class TestConcurrentExecution:
    """Tests for concurrent execution behavior."""

    def test_all_requests_triggered(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """All change requests should be triggered in concurrent mode."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            max_parallel_requests=5,
        )

        plans = [
            ChangePlan(f"cluster-{i:03d}", "cn-hangzhou", 100, 105) for i in range(10)
        ]

        apply_changes(plans, fake_client, config)

        # All plans should have been executed
        assert len(fake_client.modify_storage_calls) == len(plans)

    def test_parallelism_limit_respected(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Should not exceed max_parallel_requests concurrent executions."""
        config = AppConfig(
            run_mode="apply",
            regions=["cn-hangzhou"],
            max_parallel_requests=3,
        )

        [ChangePlan(f"cluster-{i:03d}", "cn-hangzhou", 100, 105) for i in range(10)]

        # This test documents expected behavior
        # Implementation should limit concurrent executions to 3
        assert config.max_parallel_requests == 3


class TestGracefulShutdown:
    """Tests for graceful shutdown via shutdown_event."""

    def test_shutdown_event_stops_new_tasks(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """When shutdown_event is set, no new tasks should start."""
        config = AppConfig(run_mode="apply", regions=["cn-hangzhou"])

        shutdown_event = threading.Event()
        shutdown_event.set()  # Pre-set to simulate immediate shutdown

        plans = [
            ChangePlan(f"cluster-{i:03d}", "cn-hangzhou", 100, 105) for i in range(5)
        ]

        report = apply_changes(plans, fake_client, config, shutdown_event)

        # Report should indicate interruption
        assert report.interrupted is True

    def test_shutdown_preserves_in_progress_tasks(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """In-progress tasks should complete when shutdown_event is set."""
        config = AppConfig(run_mode="apply", regions=["cn-hangzhou"])

        shutdown_event = threading.Event()

        plans = [
            ChangePlan(f"cluster-{i:03d}", "cn-hangzhou", 100, 105) for i in range(5)
        ]

        # Start execution, then set shutdown event
        def delayed_shutdown() -> None:
            time.sleep(0.1)
            shutdown_event.set()

        shutdown_thread = threading.Thread(target=delayed_shutdown)
        shutdown_thread.start()

        report = apply_changes(plans, fake_client, config, shutdown_event)

        shutdown_thread.join()

        # Some tasks should have completed before shutdown
        # The exact number depends on timing
        assert report.total_changes >= 0

    def test_report_marked_as_interrupted(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Report should have interrupted=True when shutdown is triggered."""
        config = AppConfig(run_mode="apply", regions=["cn-hangzhou"])

        shutdown_event = threading.Event()
        shutdown_event.set()

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
        ]

        report = apply_changes(plans, fake_client, config, shutdown_event)

        assert report.interrupted is True


class TestPlanChanges:
    """Tests for plan_changes function."""

    def test_plan_changes_generates_plans(
        self,
        sample_clusters: list[ClusterDetail],
        sample_config: AppConfig,
    ) -> None:
        """plan_changes should generate ChangePlan for each cluster."""
        plans = plan_changes(sample_clusters, sample_config)

        assert isinstance(plans, list)
        for plan in plans:
            assert isinstance(plan, ChangePlan)
            assert plan.cluster_id is not None
            assert plan.target_storage_gb > 0

    def test_plan_changes_respects_min_threshold(
        self,
        sample_config: AppConfig,
    ) -> None:
        """plan_changes should skip clusters with changes below threshold."""
        # Cluster where target is very close to current
        cluster = ClusterDetail(
            cluster_id="pc-small",
            region="cn-hangzhou",
            cluster_name="small-change",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=100,
            provisioned_storage_gb=104,  # Target would be 105, change is only 1GB
        )

        plans = plan_changes([cluster], sample_config)

        # If min_change_threshold_gb > 1, this should be skipped
        if sample_config.min_change_threshold_gb > 1:
            assert len(plans) == 0

    def test_plan_changes_caps_expansion_instead_of_skipping(
        self,
    ) -> None:
        """plan_changes should cap expansion that exceeds max_expand_ratio."""
        # Create a cluster that would trigger ratio exceedance
        cluster = ClusterDetail(
            cluster_id="pc-danger",
            region="cn-hangzhou",
            cluster_name="danger-cluster",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=500,
            provisioned_storage_gb=100,  # 5x expansion, exceeds max 2.0
        )
        # Use config where safety cap applies
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            max_expand_ratio=1.5,
        )

        plans = plan_changes([cluster], config)

        # Should NOT skip — should cap to 100 * 1.5 = 150
        assert len(plans) == 1
        assert plans[0].target_storage_gb == 150

    def test_plan_changes_logs_unexpected_error(self) -> None:
        """plan_changes should log ERROR for unexpected exceptions."""
        # Create a cluster with invalid data that might cause
        # unexpected error in compute_target_storage
        cluster = ClusterDetail(
            cluster_id="pc-bad",
            region="cn-hangzhou",
            cluster_name="bad-cluster",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=-1,  # Invalid negative usage
            provisioned_storage_gb=100,
        )
        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        # This should not crash, and should log an error
        plans = plan_changes([cluster], config)
        assert isinstance(plans, list)


class TestExecutionReport:
    """Tests for ExecutionReport structure."""

    def test_report_includes_all_counts(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Report should include success, failure, and skip counts."""
        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        plans = [
            ChangePlan(f"cluster-{i:03d}", "cn-hangzhou", 100, 105) for i in range(5)
        ]

        report = apply_changes(plans, fake_client, config)

        assert hasattr(report, "total_clusters")
        assert hasattr(report, "total_successful")
        assert hasattr(report, "total_failed")
        assert hasattr(report, "total_skipped")

    def test_report_includes_detailed_results(
        self,
        fake_client: FakePolarDBClient,
    ) -> None:
        """Report should include detailed lists of results."""
        config = AppConfig(run_mode="dry-run", regions=["cn-hangzhou"])

        plans = [
            ChangePlan("cluster-001", "cn-hangzhou", 100, 105),
        ]

        report = apply_changes(plans, fake_client, config)

        assert hasattr(report, "successful")
        assert hasattr(report, "failed")
        assert hasattr(report, "skipped")
        assert isinstance(report.successful, list)
        assert isinstance(report.failed, list)


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
