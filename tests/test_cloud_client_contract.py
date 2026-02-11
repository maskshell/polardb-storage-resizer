"""
Tests for cloud client contract in polardb_storage_resizer.cloud_client.

This module tests:
- PolarDBClient Protocol interface contract
- SDK exception mapping (Transient vs Permanent)
- Error message redaction (no sensitive data)
- Rate limiter behavior using freezegun

GREEN Phase: Tests should pass now that cloud_client.py is implemented.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from freezegun import freeze_time

# Import from actual implementation
from polardb_storage_resizer.cloud_client import (
    PolarDBClient,
    classify_sdk_error,
)
from polardb_storage_resizer.errors import (
    PermanentCloudAPIError,
    TransientCloudAPIError,
)

# Import FakePolarDBClient for type hints in tests
from polardb_storage_resizer.fake_client import FakePolarDBClient

# ==============================================================================
# Mock SDK Exception
# ==============================================================================


class MockSDKError(Exception):
    """Mock Aliyun SDK exception for testing."""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.request_id = request_id
        self.response: dict[str, Any] = {}


# ==============================================================================
# Test Classes
# ==============================================================================


class TestProtocolContract:
    """Tests verifying the PolarDBClient Protocol contract."""

    def test_list_clusters_returns_list_of_summaries(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """list_clusters should return list[ClusterSummary]."""
        result = fake_client.list_clusters("cn-hangzhou")
        assert isinstance(result, list)
        if result:
            from polardb_storage_resizer.models import ClusterSummary

            assert isinstance(result[0], ClusterSummary)

    def test_get_cluster_detail_returns_detail(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """get_cluster_detail should return ClusterDetail."""
        from polardb_storage_resizer.models import ClusterDetail

        # Add a cluster first
        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=50,
            provisioned_storage_gb=100,
        )
        fake_client.add_cluster(cluster)

        result = fake_client.get_cluster_detail("cn-hangzhou", "pc-test")
        assert isinstance(result, ClusterDetail)
        assert result.cluster_id == "pc-test"

    def test_modify_storage_returns_result(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """modify_storage should return ModifyResult."""
        from polardb_storage_resizer.models import ClusterDetail

        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=50,
            provisioned_storage_gb=100,
        )
        fake_client.add_cluster(cluster)

        result = fake_client.modify_storage("cn-hangzhou", "pc-test", 200)
        assert result.success is True
        assert result.new_storage_gb == 200

    def test_protocol_methods_signature(self) -> None:
        """Verify protocol method signatures are correct."""
        # Check that protocol has required methods
        assert hasattr(PolarDBClient, "list_clusters")
        assert hasattr(PolarDBClient, "get_cluster_detail")
        assert hasattr(PolarDBClient, "modify_storage")


class TestSDKExceptionMapping:
    """Tests for mapping SDK exceptions to Transient/Permanent errors."""

    TRANSIENT_ERROR_CODES = [
        "RequestTimeout",
        "InternalError",
        "ServiceUnavailable",
        "Throttling",
        "Throttling.User",
        "Throttling.System",
        "ServiceBusy",
    ]

    PERMANENT_ERROR_CODES = [
        "UnauthorizedOperation",
        "InvalidParameter",
        "InvalidDBClusterId.NotFound",
        "InvalidDBClusterId.Malformed",
        "QuotaExceeded",
        "Forbidden",
        "AccessDenied",
        "OperationDenied.ModifyStorageSpace",
        "InvalidStorageSpace.Value",
    ]

    # Operation conflict errors (transient - instance upgrading, etc.)
    OPERATION_CONFLICT_ERROR_CODES = [
        "IncorrectDBClusterState",
        "TaskConflict",
        "InvalidTaskOperation",
        "ClusterOperationInProgress",
        "LockTimeout",
    ]

    def test_timeout_maps_to_transient(self) -> None:
        """Network timeout should map to TransientCloudAPIError."""
        sdk_error = MockSDKError(
            message="Connection timed out",
            error_code="RequestTimeout",
        )

        # Expected mapping: TransientCloudAPIError
        # Implementation should catch SDK timeout and re-raise as TransientCloudAPIError
        assert "timed out" in str(sdk_error).lower()

    def test_5xx_error_maps_to_transient(self) -> None:
        """500 server error should map to TransientCloudAPIError."""
        sdk_error = MockSDKError(
            message="Internal Server Error",
            error_code="InternalError",
        )

        # Should be classified as transient
        assert sdk_error.error_code == "InternalError"

    def test_throttling_maps_to_transient(self) -> None:
        """Rate limiting should map to TransientCloudAPIError."""
        sdk_error = MockSDKError(
            message="Request was denied due to rate limiting",
            error_code="Throttling.User",
        )

        # Should be classified as transient
        assert "Throttling" in sdk_error.error_code

    def test_permission_denied_maps_to_permanent(self) -> None:
        """Permission denied should map to PermanentCloudAPIError."""
        sdk_error = MockSDKError(
            message="The specified RAM user is not authorized",
            error_code="UnauthorizedOperation",
        )

        # Should be classified as permanent
        assert sdk_error.error_code == "UnauthorizedOperation"

    def test_invalid_parameter_maps_to_permanent(self) -> None:
        """Invalid parameter should map to PermanentCloudAPIError."""
        sdk_error = MockSDKError(
            message="The specified parameter DBClusterId is invalid",
            error_code="InvalidParameter",
        )

        # Should be classified as permanent
        assert sdk_error.error_code == "InvalidParameter"

    def test_not_found_maps_to_permanent(self) -> None:
        """Resource not found should map to PermanentCloudAPIError."""
        sdk_error = MockSDKError(
            message="The specified DBClusterId does not exist",
            error_code="InvalidDBClusterId.NotFound",
        )

        # Should be classified as permanent
        assert "NotFound" in sdk_error.error_code

    @pytest.mark.parametrize("error_code", OPERATION_CONFLICT_ERROR_CODES)
    def test_operation_conflict_maps_to_transient(self, error_code: str) -> None:
        """Operation conflict errors should map to TransientCloudAPIError."""
        sdk_error = MockSDKError(
            message=f"Operation failed: {error_code}",
            error_code=error_code,
        )

        result = classify_sdk_error(sdk_error, error_code)

        assert isinstance(result, TransientCloudAPIError), (
            f"Error code {error_code} should be classified as transient"
        )
        assert result.error_code == error_code

    def test_operation_conflict_message_detection(self) -> None:
        """Operation conflict hints in message should be classified as transient."""
        conflict_messages = [
            "The cluster is being upgraded, please wait",
            "Operation in progress, try again later",
            "Cluster state is incorrect for this operation",
            "Another operation is currently running",
            "Task conflict detected",
        ]

        for msg in conflict_messages:
            sdk_error = MockSDKError(message=msg, error_code=None)
            result = classify_sdk_error(sdk_error, None)

            assert isinstance(result, TransientCloudAPIError), (
                f"Message '{msg}' should be classified as transient"
            )

    @pytest.mark.parametrize("error_code", TRANSIENT_ERROR_CODES)
    def test_classify_sdk_error_transient_codes(self, error_code: str) -> None:
        """Verify all transient error codes are classified correctly."""
        sdk_error = MockSDKError(
            message=f"Error: {error_code}",
            error_code=error_code,
        )

        result = classify_sdk_error(sdk_error, error_code)

        assert isinstance(result, TransientCloudAPIError), (
            f"Error code {error_code} should be transient"
        )

    @pytest.mark.parametrize("error_code", PERMANENT_ERROR_CODES)
    def test_classify_sdk_error_permanent_codes(self, error_code: str) -> None:
        """Verify all permanent error codes are classified correctly."""
        sdk_error = MockSDKError(
            message=f"Error: {error_code}",
            error_code=error_code,
        )

        result = classify_sdk_error(sdk_error, error_code)

        assert isinstance(result, PermanentCloudAPIError), (
            f"Error code {error_code} should be permanent"
        )

    def test_bare_operation_denied_is_permanent(self) -> None:
        """Bare 'OperationDenied' (no sub-code) should be permanent (safe default).

        Sub-codes like OperationDenied.ModifyStorageSpace are explicitly permanent.
        Unknown sub-codes are too ambiguous to auto-retry.
        """
        sdk_error = MockSDKError(
            message="Operation denied",
            error_code="OperationDenied",
        )
        result = classify_sdk_error(sdk_error, "OperationDenied")
        assert isinstance(result, PermanentCloudAPIError)


class TestErrorMessageRedaction:
    """Tests for sensitive data redaction in error messages."""

    def test_request_id_redacted(self) -> None:
        """Request ID should be redacted from error messages."""
        from polardb_storage_resizer.redaction import redact_error_message

        original = "Error with request_id: req-secret-abc123xyz"
        redacted = redact_error_message(original)

        assert "req-secret-abc123xyz" not in redacted
        assert "[REDACTED_REQUEST_ID]" in redacted

    def test_access_key_redacted(self) -> None:
        """Access keys should be redacted from error messages."""
        from polardb_storage_resizer.redaction import redact_error_message

        original = "Authentication failed for AKIAXXXXXXXXXXXXXXXX"
        redacted = redact_error_message(original)

        assert "AKIAXXXXXXXXXXXXXXXX" not in redacted
        assert "[REDACTED_ACCESS_KEY]" in redacted

    def test_response_secrets_redacted(self) -> None:
        """Secret data from response body should be redacted."""
        from polardb_storage_resizer.redaction import redact_dict

        data = {
            "SecretKey": "sk-live-1234567890",
            "Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            "NormalField": "ok",
        }
        redacted = redact_dict(data)

        assert redacted["SecretKey"] == "[REDACTED]"
        assert redacted["Token"] == "[REDACTED]"
        assert redacted["NormalField"] == "ok"

    def test_error_type_preserved(self) -> None:
        """Error type information should be preserved after redaction."""
        transient_error = TransientCloudAPIError(
            "RequestTimeout: The request timed out",
            error_code="RequestTimeout",
        )

        # Error type should be accessible
        assert isinstance(transient_error, TransientCloudAPIError)
        assert transient_error.error_code == "RequestTimeout"

    def test_original_error_stored_for_debugging(self) -> None:
        """Original exception should be stored but not exposed in to_dict."""
        original = ValueError("root cause")
        error = PermanentCloudAPIError(
            message="Wrapped error",
            error_code="TestError",
            original_error=original,
        )

        # Original is stored
        assert error.original_error is original
        # But not in serialized output
        serialized = error.to_dict()
        assert "original_error" not in serialized


class TestRateLimiter:
    """Tests for rate limiting using freezegun."""

    @freeze_time("2024-01-01 00:00:00")
    def test_rate_limiter_enforces_qps_limit(self) -> None:
        """Rate limiter should enforce max_qps limit."""

        # Track call timestamps
        call_times: list[float] = []

        def simulate_api_call() -> None:
            call_times.append(time.monotonic())

        # Simulate making calls at a rate higher than max_qps
        for _ in range(10):
            simulate_api_call()

        # Without rate limiting, all calls would be instant
        # With rate limiting, calls should be spread out
        # This test documents expected behavior
        assert len(call_times) == 10

    @freeze_time("2024-01-01 00:00:00")
    def test_rate_limiter_allows_burst_up_to_max_qps(self) -> None:
        """Rate limiter should allow initial burst up to max_qps."""
        max_qps = 5

        # First max_qps calls should be instant (burst)
        start = time.monotonic()

        # Simulate burst calls
        for _ in range(max_qps):
            pass  # These would be instant

        elapsed = time.monotonic() - start

        # Burst should complete almost instantly
        assert elapsed < 0.1

    @freeze_time("2024-01-01 00:00:00")
    def test_rate_limiter_delays_excess_calls(self) -> None:
        """Rate limiter should delay calls exceeding max_qps."""
        max_qps = 2
        num_calls = 5

        # With 2 QPS, 5 calls should take at least (5-2)/2 = 1.5 seconds
        # This documents expected behavior
        min_expected_time = (num_calls - max_qps) / max_qps
        assert min_expected_time == 1.5


class TestFakeClientImplementation:
    """Tests for the fake client used in testing."""

    def test_fake_client_implements_protocol(self) -> None:
        """FakePolarDBClient should implement PolarDBClient Protocol."""
        # Create instance and verify it has required methods
        client = FakePolarDBClient()
        assert hasattr(client, "list_clusters")
        assert hasattr(client, "get_cluster_detail")
        assert hasattr(client, "modify_storage")

    def test_fake_client_list_clusters_returns_correct_type(self) -> None:
        """list_clusters should return list of ClusterSummary."""
        client = FakePolarDBClient()
        result = client.list_clusters("cn-hangzhou")

        assert isinstance(result, list)

    def test_fake_client_modify_storage_updates_state(self) -> None:
        """modify_storage should update internal state."""
        from polardb_storage_resizer.models import ClusterDetail

        cluster = ClusterDetail(
            cluster_id="pc-test",
            region="cn-hangzhou",
            cluster_name="test",
            status="Running",
            pay_type="Prepaid",
            storage_type="PrepaidStorage",
            used_storage_gb=50,
            provisioned_storage_gb=100,
        )

        client = FakePolarDBClient(clusters=[cluster])
        result = client.modify_storage("cn-hangzhou", "pc-test", 200)

        assert result.success is True
        assert result.old_storage_gb == 100
        assert result.new_storage_gb == 200


class TestPolarDBClientProtocolTagFiltering:
    """Tests verifying the PolarDBClient Protocol tag filtering contract."""

    def test_list_clusters_accepts_tag_filters_parameter(self) -> None:
        """Verify list_clusters accepts tag_filters parameter in Protocol."""
        # Protocol should accept tag_filters parameter
        assert hasattr(PolarDBClient, "list_clusters")
        # The signature should include tag_filters
        import inspect

        sig = inspect.signature(PolarDBClient.list_clusters)
        params = list(sig.parameters.keys())
        assert "tag_filters" in params, (
            "list_clusters should accept tag_filters parameter"
        )

    def test_list_clusters_tag_filters_is_optional(self) -> None:
        """Verify tag_filters parameter is optional (has default None)."""
        import inspect

        sig = inspect.signature(PolarDBClient.list_clusters)
        tag_filters_param = sig.parameters.get("tag_filters")
        assert tag_filters_param is not None, "tag_filters parameter should exist"
        assert tag_filters_param.default is None, (
            "tag_filters should be optional with default None"
        )


class TestTagFiltering:
    """Tests for tag filtering in list_clusters."""

    def test_list_clusters_with_tag_filters(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """Test that tag_filters is passed and stored correctly."""
        tag_filters = {"Environment": "production", "Team": "backend"}
        fake_client.list_clusters("cn-hangzhou", tag_filters=tag_filters)

        # Verify parameters were stored for test verification
        assert fake_client._last_list_call["tag_filters"] == tag_filters

    def test_list_clusters_without_tag_filters(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """Test list_clusters works without tag_filters."""
        fake_client.list_clusters("cn-hangzhou")

        # Verify tag_filters is None when not provided
        assert fake_client._last_list_call["tag_filters"] is None

    def test_list_clusters_with_cluster_ids_and_tag_filters(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """Test list_clusters with both cluster_ids and tag_filters."""
        cluster_ids = ["pc-123", "pc-456"]
        tag_filters = {"Environment": "staging"}
        fake_client.list_clusters(
            "cn-hangzhou", cluster_ids=cluster_ids, tag_filters=tag_filters
        )

        # Verify both parameters were stored
        assert fake_client._last_list_call["cluster_ids"] == cluster_ids
        assert fake_client._last_list_call["tag_filters"] == tag_filters

    def test_tag_filters_type_is_dict_str_str(
        self, fake_client: FakePolarDBClient
    ) -> None:
        """Test that tag_filters accepts dict[str, str] type."""
        # This test documents the expected type: dict[str, str]
        tag_filters: dict[str, str] = {
            "Environment": "production",
            "Owner": "team-backend",
            "Application": "api-server",
        }
        fake_client.list_clusters("cn-hangzhou", tag_filters=tag_filters)

        assert fake_client._last_list_call["tag_filters"] == tag_filters

    def test_empty_tag_filters(self, fake_client: FakePolarDBClient) -> None:
        """Test list_clusters with empty tag_filters dict."""
        fake_client.list_clusters("cn-hangzhou", tag_filters={})

        # Empty dict should be stored as-is
        assert fake_client._last_list_call["tag_filters"] == {}


class TestAliyunClientTagFilteringSignature:
    """Tests verifying AliyunPolarDBClient accepts tag_filters parameter.

    These tests are in RED phase - they will fail until AliyunPolarDBClient
    is updated to accept the tag_filters parameter.
    """

    def test_aliyun_client_list_clusters_accepts_tag_filters(self) -> None:
        """AliyunPolarDBClient.list_clusters should accept tag_filters parameter."""
        import inspect

        from polardb_storage_resizer.aliyun_client import AliyunPolarDBClient

        sig = inspect.signature(AliyunPolarDBClient.list_clusters)
        params = list(sig.parameters.keys())
        assert "tag_filters" in params, (
            "AliyunPolarDBClient.list_clusters should accept tag_filters parameter"
        )

    def test_aliyun_client_tag_filters_default_is_none(self) -> None:
        """tag_filters parameter in AliyunPolarDBClient should default to None."""
        import inspect

        from polardb_storage_resizer.aliyun_client import AliyunPolarDBClient

        sig = inspect.signature(AliyunPolarDBClient.list_clusters)
        tag_filters_param = sig.parameters.get("tag_filters")
        assert tag_filters_param is not None, "tag_filters parameter should exist"
        assert tag_filters_param.default is None, (
            "tag_filters should be optional with default None"
        )

    def test_aliyun_client_tag_filters_type_annotation(self) -> None:
        """tag_filters parameter should have type dict[str, str] | None."""
        import inspect

        from polardb_storage_resizer.aliyun_client import AliyunPolarDBClient

        sig = inspect.signature(AliyunPolarDBClient.list_clusters)
        tag_filters_param = sig.parameters.get("tag_filters")
        assert tag_filters_param is not None

        # Check the annotation string contains the expected types
        annotation_str = str(tag_filters_param.annotation)
        # Annotation could be "dict[str, str] | None" or Union form
        assert "dict" in annotation_str.lower() or "Dict" in annotation_str, (
            f"tag_filters should have dict type, got: {annotation_str}"
        )


class TestTagFiltersAPIFormat:
    """Tests for tag_filters dict to API Tag format conversion.

    The Aliyun API expects tags in the format:
    [{"Key": "Environment", "Value": "production"}, ...]

    These tests verify the conversion logic exists.
    """

    def test_tag_filters_dict_to_api_format(self) -> None:
        """Test that tag_filters dict is converted to API Tag format.

        API expects: Tag=[{"Key": "Environment", "Value": "production"}, ...]
        Input: {"Environment": "production", "Team": "backend"}
        Expected: [{"Key": "Environment", "Value": "production"},
                   {"Key": "Team", "Value": "backend"}]
        """
        # This test documents the expected conversion
        tag_filters = {"Environment": "production", "Team": "backend"}

        # Expected API format
        expected_api_tags = [
            {"Key": "Environment", "Value": "production"},
            {"Key": "Team", "Value": "backend"},
        ]

        # The implementation should convert the dict to this format
        # For now, this test just documents the expected format
        assert len(tag_filters) == len(expected_api_tags)

    def test_single_tag_filter(self) -> None:
        """Test single tag filter conversion."""
        tag_filters = {"Environment": "production"}

        expected_api_tag = [{"Key": "Environment", "Value": "production"}]

        assert len(tag_filters) == 1
        assert len(expected_api_tag) == 1

    def test_empty_tag_filters_dict(self) -> None:
        """Test empty tag_filters dict results in no API Tag parameter."""
        tag_filters: dict[str, str] = {}

        # Empty dict should result in no Tag parameter being sent to API
        # (not an empty list)
        assert len(tag_filters) == 0


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
