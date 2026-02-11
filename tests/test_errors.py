"""
Tests for error types in polardb_storage_resizer.errors.

This module tests:
- Error type serialization/deserialization
- TransientCloudAPIError vs PermanentCloudAPIError distinction
- Error message redaction (no sensitive data in serialized errors)

GREEN Phase: Tests should pass now that errors.py is implemented.
"""

from __future__ import annotations

import json

import pytest

# Import from actual implementation
from polardb_storage_resizer.errors import (
    CloudAPIError,
    ConcurrentExecutionError,
    PermanentCloudAPIError,
    ResizerError,
    SafetyCheckError,
    TransientCloudAPIError,
    ValidationError,
)

# ==============================================================================
# Test Classes
# ==============================================================================


class TestErrorHierarchy:
    """Tests for error type hierarchy and inheritance."""

    def test_resizer_error_is_base_class(self) -> None:
        """ResizerError should be the base class for all custom errors."""
        assert issubclass(CloudAPIError, ResizerError)
        assert issubclass(ValidationError, ResizerError)
        assert issubclass(SafetyCheckError, ResizerError)
        assert issubclass(ConcurrentExecutionError, ResizerError)

    def test_cloud_api_error_hierarchy(self) -> None:
        """CloudAPIError should have Transient and Permanent subclasses."""
        assert issubclass(TransientCloudAPIError, CloudAPIError)
        assert issubclass(PermanentCloudAPIError, CloudAPIError)

    def test_transient_cloud_api_error_is_transient(self) -> None:
        """TransientCloudAPIError.is_transient() should return True."""
        error = TransientCloudAPIError("Temporary failure")
        assert error.is_transient() is True

    def test_permanent_cloud_api_error_is_not_transient(self) -> None:
        """PermanentCloudAPIError.is_transient() should return False."""
        error = PermanentCloudAPIError("Permanent failure")
        assert error.is_transient() is False


class TestErrorSerialization:
    """Tests for error serialization to dictionary format."""

    def test_resizer_error_to_dict(self) -> None:
        """ResizerError should serialize to dict with all relevant fields."""
        error = ResizerError("Test error message", extra_field="extra_value")

        result = error.to_dict()

        assert isinstance(result, dict)
        assert result["message"] == "Test error message"
        assert result["error_type"] == "ResizerError"

    def test_cloud_api_error_to_dict(self) -> None:
        """CloudAPIError should include error_code and request_id (redacted)."""
        error = TransientCloudAPIError(
            message="API timeout",
            error_code="RequestTimeout",
            request_id="req-123456",
        )

        result = error.to_dict()

        assert result["message"] == "API timeout"
        assert result["error_code"] == "RequestTimeout"
        # Request ID is always redacted for security
        assert result["request_id"] == "req-****"
        assert result["error_type"] == "TransientCloudAPIError"

    def test_validation_error_to_dict(self) -> None:
        """ValidationError should include the field name."""
        error = ValidationError("Invalid value", field="max_qps")

        result = error.to_dict()

        assert result["message"] == "Invalid value"
        assert result["field"] == "max_qps"
        assert result["error_type"] == "ValidationError"

    def test_safety_check_error_to_dict(self) -> None:
        """SafetyCheckError should include threshold details."""
        error = SafetyCheckError(
            message="Change exceeds maximum ratio",
            threshold_name="max_expand_ratio",
            actual_value=3.0,
            limit_value=2.0,
        )

        result = error.to_dict()

        assert result["threshold_name"] == "max_expand_ratio"
        assert result["actual_value"] == 3.0
        assert result["limit_value"] == 2.0

    def test_concurrent_execution_error_to_dict(self) -> None:
        """ConcurrentExecutionError should include conflicting process ID."""
        error = ConcurrentExecutionError(
            message="Another instance is running",
            conflicting_process_id="job-abc123",
        )

        result = error.to_dict()

        assert result["conflicting_process_id"] == "job-abc123"


class TestErrorDeserialization:
    """Tests for error deserialization from dictionary format."""

    def test_resizer_error_from_dict(self) -> None:
        """ResizerError should deserialize from dict correctly."""
        data = {
            "message": "Test error",
            "error_type": "ResizerError",
        }

        error = ResizerError.from_dict(data)

        assert isinstance(error, ResizerError)
        assert error.message == "Test error"

    def test_transient_error_from_dict(self) -> None:
        """TransientCloudAPIError should deserialize correctly."""
        data = {
            "message": "Timeout",
            "error_type": "TransientCloudAPIError",
            "error_code": "RequestTimeout",
        }

        error = ResizerError.from_dict(data)

        assert isinstance(error, TransientCloudAPIError)
        assert error.error_code == "RequestTimeout"

    def test_permanent_error_from_dict(self) -> None:
        """PermanentCloudAPIError should deserialize correctly."""
        data = {
            "message": "Access denied",
            "error_type": "PermanentCloudAPIError",
            "error_code": "UnauthorizedOperation",
        }

        error = ResizerError.from_dict(data)

        assert isinstance(error, PermanentCloudAPIError)

    def test_validation_error_from_dict(self) -> None:
        """ValidationError should deserialize with field info."""
        data = {
            "message": "Invalid",
            "error_type": "ValidationError",
            "field": "regions",
        }

        error = ResizerError.from_dict(data)

        assert isinstance(error, ValidationError)
        assert error.field == "regions"


class TestErrorRoundTrip:
    """Tests for serialization -> deserialization round trip."""

    def test_resizer_error_round_trip(self) -> None:
        """Error should survive serialization round trip."""
        original = ResizerError("Test message", key1="value1", key2="value2")

        serialized = original.to_dict()
        restored = ResizerError.from_dict(serialized)

        assert restored.message == original.message

    def test_cloud_api_error_round_trip(self) -> None:
        """CloudAPIError should survive round trip."""
        original = TransientCloudAPIError(
            message="API error",
            error_code="Throttling",
            request_id="req-xyz",
        )

        serialized = original.to_dict()
        restored = ResizerError.from_dict(serialized)

        assert restored.message == original.message
        assert isinstance(restored, TransientCloudAPIError)

    def test_safety_check_error_round_trip(self) -> None:
        """SafetyCheckError should preserve threshold values."""
        original = SafetyCheckError(
            message="Too large",
            threshold_name="max_single_change_gb",
            actual_value=5000,
            limit_value=1000,
        )

        serialized = original.to_dict()
        restored = ResizerError.from_dict(serialized)

        assert restored.threshold_name == original.threshold_name
        assert restored.actual_value == original.actual_value


class TestTransientVsPermanent:
    """Tests for distinguishing transient vs permanent errors."""

    def test_transient_errors_are_retryable(self) -> None:
        """All TransientCloudAPIError instances should be retryable."""
        transient_errors = [
            TransientCloudAPIError("Network timeout"),
            TransientCloudAPIError(
                "Service unavailable", error_code="ServiceUnavailable"
            ),
            TransientCloudAPIError("Rate limited", error_code="Throttling"),
            TransientCloudAPIError("5xx error", error_code="InternalError"),
        ]

        for error in transient_errors:
            assert error.is_transient() is True, f"{error} should be transient"

    def test_permanent_errors_are_not_retryable(self) -> None:
        """All PermanentCloudAPIError instances should not be retryable."""
        permanent_errors = [
            PermanentCloudAPIError("Permission denied"),
            PermanentCloudAPIError("Not found", error_code="NotFound"),
            PermanentCloudAPIError("Invalid parameter", error_code="InvalidParameter"),
            PermanentCloudAPIError("Quota exceeded", error_code="QuotaExceeded"),
        ]

        for error in permanent_errors:
            assert error.is_transient() is False, f"{error} should be permanent"

    def test_error_type_determines_retry_strategy(self) -> None:
        """Error type should determine retry behavior."""
        transient = TransientCloudAPIError("Retry me")
        permanent = PermanentCloudAPIError("Don't retry me")

        # Transient should be retried
        assert transient.is_transient() is True

        # Permanent should not be retried
        assert permanent.is_transient() is False


class TestErrorRedaction:
    """Tests for sensitive data redaction in error messages."""

    def test_redacted_error_excludes_request_id(self) -> None:
        """Redacted error should not contain raw request ID."""
        error = TransientCloudAPIError(
            message="API call failed with request req-secret-abc123",
            request_id="req-secret-abc123",
        )

        serialized = error.to_dict()

        # The serialized error should either:
        # 1. Not include request_id at all
        # 2. Include a redacted version
        if "request_id" in serialized:
            assert "secret-abc123" not in str(serialized["request_id"])

    def test_redacted_error_excludes_access_keys(self) -> None:
        """Redacted error should not contain access keys."""
        error = TransientCloudAPIError(
            message="Authentication failed for AKIAXXXXXXXXXXXXXXXX",
        )

        serialized = error.to_dict()

        # Access key pattern should be redacted
        assert "AKIAXXXXXXXXXXXXXXXX" not in str(serialized)

    def test_redacted_error_excludes_response_body_secrets(self) -> None:
        """Redacted error should not contain secrets from response body."""
        error = PermanentCloudAPIError(
            message='Error: {"secret_key": "sk-123456", "token": "abc"}',
        )

        serialized = error.to_dict()

        # Secrets should be redacted
        serialized_str = json.dumps(serialized)
        assert "sk-123456" not in serialized_str

    def test_redacted_error_preserves_error_type(self) -> None:
        """Redacted error should preserve error type information."""
        error = TransientCloudAPIError(
            message="RequestTimeout: The request timed out",
            error_code="RequestTimeout",
        )

        serialized = error.to_dict()

        assert serialized["error_type"] == "TransientCloudAPIError"
        assert serialized["error_code"] == "RequestTimeout"

    def test_original_error_not_exposed_in_serialization(self) -> None:
        """Original exception should not be exposed in serialized output."""
        original_exception = ValueError("Secret internal error with password123")
        error = TransientCloudAPIError(
            message="Wrapped error",
            original_error=original_exception,
        )

        serialized = error.to_dict()

        # Original exception should not leak into serialization
        assert "password123" not in str(serialized)
        assert "Secret internal error" not in str(serialized)


class TestErrorContext:
    """Tests for error context and debugging information."""

    def test_error_carries_context(self) -> None:
        """Errors should carry context for debugging."""
        error = CloudAPIError(
            message="Operation failed",
            error_code="OperationFailed",
            request_id="req-123",
            region="cn-hangzhou",
            cluster_id="pc-xxxxxxxxx",
        )

        serialized = error.to_dict()

        # Context should be available (possibly redacted)
        assert serialized.get("error_code") == "OperationFailed"

    def test_validation_error_carries_field_context(self) -> None:
        """ValidationError should indicate which field failed."""
        error = ValidationError(
            message="Value must be positive",
            field="max_qps",
            provided_value="-5",
        )

        serialized = error.to_dict()

        assert serialized["field"] == "max_qps"

    def test_safety_check_error_carries_threshold_context(self) -> None:
        """SafetyCheckError should explain the threshold violation."""
        error = SafetyCheckError(
            message="Change ratio too high",
            threshold_name="max_expand_ratio",
            actual_value=5.0,
            limit_value=2.0,
        )

        serialized = error.to_dict()

        assert serialized["threshold_name"] == "max_expand_ratio"
        assert serialized["limit_value"] == 2.0


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
