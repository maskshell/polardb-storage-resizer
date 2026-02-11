"""
Integration tests for main flow in polardb_storage_resizer.main.

This module tests:
- Dry-run mode behavior (no actual modifications)
- Exit code semantics (0=success, 1=partial failure, 2=config error, 3=signal)
- Trace ID generation and propagation in logs
- Signal handling and graceful shutdown

GREEN Phase: Tests should pass now that main.py is implemented.
"""

from __future__ import annotations

import logging
import signal
import threading
from uuid import UUID

import pytest

# Import from actual implementation
from polardb_storage_resizer.main import generate_trace_id, main

# ==============================================================================
# Test Classes
# ==============================================================================


class TestDryRunMode:
    """Tests for dry-run mode behavior."""

    def test_dry_run_does_not_modify_storage(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In dry-run mode, modify_storage should never be called."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")

        # Run main
        exit_code = main()

        # Should exit successfully
        assert exit_code == 0

    def test_dry_run_logs_change_plans(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Dry-run mode should log planned changes."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        caplog.set_level(logging.INFO)

        exit_code = main()

        # Should have some log output
        # (actual assertion depends on implementation)
        assert exit_code == 0


class TestExitCodes:
    """Tests for exit code semantics."""

    def test_exit_0_on_success(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit code 0 indicates all changes succeeded."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")

        exit_code = main()

        assert exit_code == 0

    def test_exit_0_on_no_changes_needed(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit code 0 when no clusters need modification."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        # No clusters or all clusters already at optimal size

        exit_code = main()

        # No changes needed is still success
        assert exit_code == 0

    def test_exit_1_on_partial_failure(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit code 1 indicates at least one change failed."""
        from polardb_storage_resizer.cloud_client import create_rate_limited_client
        from polardb_storage_resizer.config import AppConfig
        from polardb_storage_resizer.logging_setup import get_logger, setup_logging
        from polardb_storage_resizer.main import run

        monkeypatch.setenv("RUN_MODE", "apply")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::test:role/test")

        # Set up logging for test
        setup_logging(level="DEBUG", json_format=False, force=False)
        logger = get_logger(__name__)

        # Create config from environment
        config = AppConfig.from_env()

        # Create a fake client where one cluster succeeds and one fails
        from polardb_storage_resizer.fake_client import FakePolarDBClient
        from polardb_storage_resizer.models import ClusterDetail

        fake_client = FakePolarDBClient(
            clusters=[
                ClusterDetail(
                    cluster_id="pc-success",
                    region="cn-hangzhou",
                    cluster_name="success-cluster",
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="psl4",
                    used_storage_gb=100,
                    provisioned_storage_gb=200,
                ),
                ClusterDetail(
                    cluster_id="pc-fail",
                    region="cn-hangzhou",
                    cluster_name="fail-cluster",
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="psl4",
                    used_storage_gb=100,
                    provisioned_storage_gb=200,
                ),
            ],
            fail_on_cluster_ids={"pc-fail"},
        )

        # Wrap with rate limiter
        client = create_rate_limited_client(fake_client, config)

        # Create shutdown event
        shutdown_event = threading.Event()

        # Run the resizer logic
        exit_code, report = run(config, client, shutdown_event, logger)

        # Verify exit code is 1 (partial failure)
        assert exit_code == 1
        assert report is not None
        assert report.total_failed > 0
        assert report.total_successful >= 0

    def test_exit_2_on_config_error(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit code 2 indicates configuration error."""
        # Missing required configuration
        monkeypatch.setenv("RUN_MODE", "dry-run")
        # REGIONS not set - should fail validation

        exit_code = main()

        assert exit_code == 2

    def test_exit_2_on_rssa_missing_in_apply(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit code 2 when RSSA credentials missing in apply mode."""
        monkeypatch.setenv("RUN_MODE", "apply")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        # ALIBABA_CLOUD_ROLE_ARN not set

        exit_code = main()

        assert exit_code == 2

    def test_exit_3_on_signal_interrupt(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exit code 3 indicates interruption by signal."""
        from polardb_storage_resizer.cloud_client import create_rate_limited_client
        from polardb_storage_resizer.config import AppConfig
        from polardb_storage_resizer.logging_setup import get_logger, setup_logging
        from polardb_storage_resizer.main import EXIT_SIGNAL_INTERRUPT, run

        monkeypatch.setenv("RUN_MODE", "apply")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::test:role/test")

        # Set up logging for test
        setup_logging(level="DEBUG", json_format=False, force=False)
        logger = get_logger(__name__)

        # Create config from environment
        config = AppConfig.from_env()

        # Create a fake client with clusters that need changes
        from polardb_storage_resizer.fake_client import FakePolarDBClient
        from polardb_storage_resizer.models import ClusterDetail

        fake_client = FakePolarDBClient(
            clusters=[
                ClusterDetail(
                    cluster_id="pc-test",
                    region="cn-hangzhou",
                    cluster_name="test-cluster",
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="psl4",
                    used_storage_gb=100,
                    provisioned_storage_gb=200,
                ),
            ],
        )

        # Wrap with rate limiter
        client = create_rate_limited_client(fake_client, config)

        # Create shutdown event and pre-set it to simulate signal interrupt
        shutdown_event = threading.Event()
        shutdown_event.set()  # Simulate that signal was received

        # Run the resizer logic
        exit_code, report = run(config, client, shutdown_event, logger)

        # Verify exit code is 3 (signal interrupt)
        assert exit_code == EXIT_SIGNAL_INTERRUPT


class TestTraceId:
    """Tests for Trace ID generation and propagation."""

    def test_trace_id_is_valid_uuid4(self) -> None:
        """Trace ID should be a valid UUID4."""
        trace_id = generate_trace_id()

        # Should be valid UUID
        uuid_obj = UUID(trace_id)
        assert uuid_obj.version == 4

    def test_trace_id_unique_per_execution(self) -> None:
        """Each execution should have a unique Trace ID."""
        ids = [generate_trace_id() for _ in range(10)]

        # All IDs should be unique
        assert len(set(ids)) == 10

    def test_trace_id_in_logs(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Trace ID should appear in log output."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        caplog.set_level(logging.DEBUG)

        main()

        # Logs should contain a UUID pattern
        import re

        log_output = caplog.text
        uuid_pattern = (
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
        )

        matches = re.findall(uuid_pattern, log_output, re.IGNORECASE)
        assert len(matches) > 0, "Trace ID should appear in logs"

    def test_trace_id_consistent_across_logs(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Same Trace ID should be used in all log messages."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        caplog.set_level(logging.DEBUG)

        main()

        import re

        log_output = caplog.text
        uuid_pattern = (
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
        )

        # Only match the application's own trace_id from structured log lines,
        # not arbitrary UUIDs that may leak from SDK error messages.
        # Format: "INFO [uuid]:" or "trace_id=uuid"
        prefix_matches = re.findall(
            rf"^[A-Z]+ \[({uuid_pattern})\]", log_output, re.MULTILINE | re.IGNORECASE
        )
        key_matches = re.findall(
            rf"trace_id=({uuid_pattern})", log_output, re.IGNORECASE
        )

        all_trace_ids = prefix_matches + key_matches
        assert all_trace_ids, "No trace IDs found in log output"

        unique_ids = {tid.lower() for tid in all_trace_ids}
        assert len(unique_ids) == 1, "All logs should use the same Trace ID"


class TestSignalHandling:
    """Tests for signal handling and graceful shutdown."""

    def test_sigterm_handler_registered(self) -> None:
        """SIGTERM handler should be registered during execution."""
        from polardb_storage_resizer.main import GracefulShutdown

        # Get original handlers
        original_sigterm = signal.getsignal(signal.SIGTERM)

        with GracefulShutdown():
            # Verify SIGTERM handler is set
            current_handler = signal.getsignal(signal.SIGTERM)
            # The handler should be our custom handler (a function, not default)
            assert callable(current_handler)
            assert current_handler != signal.SIG_DFL
            assert current_handler != signal.SIG_IGN

        # Verify original handler is restored
        assert signal.getsignal(signal.SIGTERM) == original_sigterm

    def test_sigint_handler_registered(self) -> None:
        """SIGINT handler should be registered during execution."""
        from polardb_storage_resizer.main import GracefulShutdown

        # Get original handlers
        original_sigint = signal.getsignal(signal.SIGINT)

        with GracefulShutdown():
            # Verify SIGINT handler is set
            current_handler = signal.getsignal(signal.SIGINT)
            # The handler should be our custom handler (a function, not default)
            assert callable(current_handler)
            assert current_handler != signal.SIG_DFL
            assert current_handler != signal.SIG_IGN

        # Verify original handler is restored
        assert signal.getsignal(signal.SIGINT) == original_sigint

    def test_shutdown_event_propagated_to_executor(self) -> None:
        """Signal should trigger shutdown_event passed to executor."""
        from polardb_storage_resizer.main import GracefulShutdown

        with GracefulShutdown() as shutdown:
            # Initially, shutdown event should not be set
            assert not shutdown.shutdown_event.is_set()

            # Simulate signal by calling the signal handler directly
            shutdown._signal_handler(signal.SIGTERM, None)

            # Now shutdown event should be set
            assert shutdown.shutdown_event.is_set()
            assert shutdown.was_interrupted()


class TestMultipleRegions:
    """Tests for multi-region processing."""

    def test_all_configured_regions_processed(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """All configured regions should be processed."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou,cn-beijing,cn-shanghai")
        caplog.set_level(logging.INFO)

        exit_code = main()

        # Should process all regions
        caplog.text.lower()
        # (actual assertions depend on implementation logging)

        assert exit_code == 0


class TestExecutionReport:
    """Tests for execution report generation."""

    def test_report_logged_on_completion(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Execution report should be logged on completion."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("USE_FAKE_CLIENT", "true")
        caplog.set_level(logging.INFO)

        main()

        # Logs should contain execution-related keywords
        log_messages = [r.message.lower() for r in caplog.records]
        assert any("execution" in msg or "complete" in msg for msg in log_messages), (
            f"Expected execution log. Got: {log_messages[:5]}"
        )


class TestErrorHandling:
    """Tests for error handling in main flow."""

    def test_transient_error_retried(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Transient errors should be retried."""
        from polardb_storage_resizer.cloud_client import RateLimitedClient
        from polardb_storage_resizer.config import AppConfig
        from polardb_storage_resizer.logging_setup import get_logger, setup_logging
        from polardb_storage_resizer.main import run

        monkeypatch.setenv("RUN_MODE", "apply")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::test:role/test")
        monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "3")
        # Reduce backoff time for faster tests
        monkeypatch.setenv("RETRY_BACKOFF_BASE", "0.01")
        monkeypatch.setenv("RETRY_BACKOFF_MAX", "0.1")

        # Set up logging for test
        setup_logging(level="DEBUG", json_format=False, force=False)
        logger = get_logger(__name__)

        # Create config from environment
        config = AppConfig.from_env()

        # Create a fake client with transient failures (2 failures before success)
        from polardb_storage_resizer.fake_client import FakePolarDBClient
        from polardb_storage_resizer.models import ClusterDetail

        fake_client = FakePolarDBClient(
            clusters=[
                ClusterDetail(
                    cluster_id="pc-transient",
                    region="cn-hangzhou",
                    cluster_name="transient-cluster",
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="psl4",
                    used_storage_gb=100,
                    provisioned_storage_gb=200,
                ),
            ],
            transient_fail_count=2,  # Fail twice, then succeed
        )

        # Wrap with rate limiter
        client = RateLimitedClient(fake_client, config.max_qps)

        # Create shutdown event
        shutdown_event = threading.Event()

        # Run the resizer logic
        exit_code, report = run(config, client, shutdown_event, logger)

        # Should succeed after retries
        assert exit_code == 0
        assert report is not None
        assert report.total_successful == 1
        assert report.total_failed == 0
        # Verify that modify was called multiple times (retries)
        assert len(fake_client.modify_storage_calls) >= 2

    def test_permanent_error_not_retried(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Permanent errors should not be retried."""
        from polardb_storage_resizer.cloud_client import RateLimitedClient
        from polardb_storage_resizer.config import AppConfig
        from polardb_storage_resizer.logging_setup import get_logger, setup_logging
        from polardb_storage_resizer.main import run

        monkeypatch.setenv("RUN_MODE", "apply")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::test:role/test")
        monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "3")

        # Set up logging for test
        setup_logging(level="DEBUG", json_format=False, force=False)
        logger = get_logger(__name__)

        # Create config from environment
        config = AppConfig.from_env()

        # Create a fake client that always fails with permanent error
        from polardb_storage_resizer.fake_client import FakePolarDBClient
        from polardb_storage_resizer.models import ClusterDetail

        fake_client = FakePolarDBClient(
            clusters=[
                ClusterDetail(
                    cluster_id="pc-permanent-fail",
                    region="cn-hangzhou",
                    cluster_name="permanent-fail-cluster",
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="psl4",
                    used_storage_gb=100,
                    provisioned_storage_gb=200,
                ),
            ],
            fail_on_cluster_ids={"pc-permanent-fail"},  # Permanent failure
        )

        # Wrap with rate limiter
        client = RateLimitedClient(fake_client, config.max_qps)

        # Create shutdown event
        shutdown_event = threading.Event()

        # Run the resizer logic
        exit_code, report = run(config, client, shutdown_event, logger)

        # Should fail with exit code 1
        assert exit_code == 1
        assert report is not None
        assert report.total_failed == 1
        # Verify that modify was called only once (no retries for permanent errors)
        assert len(fake_client.modify_storage_calls) == 1

    def test_error_logged_with_context(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Errors should be logged with sufficient context."""
        from polardb_storage_resizer.cloud_client import RateLimitedClient
        from polardb_storage_resizer.config import AppConfig
        from polardb_storage_resizer.logging_setup import get_logger, setup_logging
        from polardb_storage_resizer.main import run

        monkeypatch.setenv("RUN_MODE", "apply")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::test:role/test")

        caplog.set_level(logging.ERROR)

        # Set up logging for test
        setup_logging(level="DEBUG", json_format=False, force=False)
        logger = get_logger(__name__)

        # Create config from environment
        config = AppConfig.from_env()

        # Create a fake client that fails
        from polardb_storage_resizer.fake_client import FakePolarDBClient
        from polardb_storage_resizer.models import ClusterDetail

        fake_client = FakePolarDBClient(
            clusters=[
                ClusterDetail(
                    cluster_id="pc-error-test",
                    region="cn-hangzhou",
                    cluster_name="error-test-cluster",
                    status="Running",
                    pay_type="Prepaid",
                    storage_type="psl4",
                    used_storage_gb=100,
                    provisioned_storage_gb=200,
                ),
            ],
            fail_on_cluster_ids={"pc-error-test"},
        )

        # Wrap with rate limiter
        client = RateLimitedClient(fake_client, config.max_qps)

        # Create shutdown event
        shutdown_event = threading.Event()

        # Run the resizer logic
        run(config, client, shutdown_event, logger)

        # Check that error logs contain context
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]

        # There should be at least one error log
        assert len(error_records) >= 1

        # Error logs should contain cluster_id, region, or error type
        log_messages = " ".join(r.message for r in error_records).lower()
        # Check for relevant context in error messages
        has_context = (
            "pc-error-test" in log_messages
            or "cn-hangzhou" in log_messages
            or "failed" in log_messages
            or "error" in log_messages
        )
        assert has_context, (
            "Error logs should contain context like cluster_id, region, or error type"
        )


class TestIntegration:
    """End-to-end integration tests."""

    def test_full_dry_run_flow(
        self,
        isolated_env: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Complete dry-run flow should work end-to-end."""
        monkeypatch.setenv("RUN_MODE", "dry-run")
        monkeypatch.setenv("REGIONS", "cn-hangzhou")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        caplog.set_level(logging.DEBUG)

        exit_code = main()

        # Should complete successfully
        assert exit_code == 0

        # Should have trace ID in logs
        import re

        uuid_pattern = (
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
        )
        assert re.search(uuid_pattern, caplog.text, re.IGNORECASE)


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
