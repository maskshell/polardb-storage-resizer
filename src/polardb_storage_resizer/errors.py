"""
Error types for PolarDB Storage Resizer.

This module defines a hierarchical error type system:
- ResizerError: Base exception for all resizer errors
- CloudAPIError: Base for cloud API errors (with Transient/Permanent subtypes)
- ValidationError: Configuration or input validation failed
- SafetyCheckError: Safety threshold check failed
- ConcurrentExecutionError: Concurrent execution conflict detected

All errors support serialization/deserialization for logging and
automatic sensitive data redaction.
"""

from __future__ import annotations

from typing import Any

# Shared constants used by multiple modules
OPERATION_CONFLICT_HINTS: tuple[str, ...] = (
    "operation in progress",
    "task in progress",
    "being modified",
    "being upgraded",
    "is locked",
    "is busy",
    "cluster state",
    "incorrect state",
    "task conflict",
    "another operation",
    "please wait",
    "try again later",
    "cooling period",
)


class ResizerError(Exception):
    """
    Base exception for all resizer errors.

    All custom exceptions in this project should inherit from this class.
    Provides serialization/deserialization support for logging and debugging.

    Attributes:
        message: Custom attribute storing the error message
            (not the deprecated BaseException.message).
            Access via getattr(e, 'message', str(e)) per CLAUDE.md.
        details: Additional context as keyword arguments
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = kwargs

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize error to dictionary for logging.

        The output is safe for logging (sensitive data is redacted).

        Returns:
            Dictionary with error_type, message, and any additional details
        """
        from polardb_storage_resizer.redaction import (
            redact_dict,
            redact_error_message,
            redact_list,
        )

        result: dict[str, Any] = {
            "error_type": self.__class__.__name__,
            "message": redact_error_message(self.message),
        }
        # Add any additional details (redact them too)
        if self.details:
            for key, value in self.details.items():
                if isinstance(value, str):
                    result[key] = redact_error_message(value)
                elif isinstance(value, dict):
                    result[key] = redact_dict(value)
                elif isinstance(value, list):
                    result[key] = redact_list(value)
                else:
                    result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResizerError:
        """
        Deserialize error from dictionary.

        Args:
            data: Dictionary with error data (from to_dict())

        Returns:
            Appropriate ResizerError subclass instance
        """
        error_type = data.get("error_type", "ResizerError")
        message = data.get("message", "Unknown error")

        # Extract the error class
        error_classes: dict[str, type[ResizerError]] = {
            "ResizerError": ResizerError,
            "CloudAPIError": CloudAPIError,
            "TransientCloudAPIError": TransientCloudAPIError,
            "PermanentCloudAPIError": PermanentCloudAPIError,
            "ValidationError": ValidationError,
            "SafetyCheckError": SafetyCheckError,
            "ConcurrentExecutionError": ConcurrentExecutionError,
        }

        error_class = error_classes.get(error_type, ResizerError)

        # Build kwargs from data (exclude special fields)
        special_fields = {"error_type", "message"}
        kwargs = {k: v for k, v in data.items() if k not in special_fields}

        return error_class(message, **kwargs)


class CloudAPIError(ResizerError):
    """
    Base exception for cloud API errors.

    Cloud API errors are categorized as either Transient (retryable)
    or Permanent (non-retryable).

    Attributes:
        message: Human-readable error message
        error_code: Cloud API error code (e.g., "RequestTimeout")
        request_id: Request ID from the API call (will be redacted in logs)
        original_error: Original exception from the SDK (for debugging only)
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        request_id: str | None = None,
        original_error: Exception | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, **kwargs)
        self.error_code = error_code
        self.request_id = request_id
        self.original_error = original_error

    def is_transient(self) -> bool:
        """
        Check if this error is transient (retryable).

        Returns:
            True if the error is transient and should be retried
        """
        # Base class returns False; subclasses override this
        return False

    def to_dict(self) -> dict[str, Any]:
        """Serialize error to dictionary, with redaction applied."""
        result = super().to_dict()

        if self.error_code:
            result["error_code"] = self.error_code

        # Redact request_id if present
        if self.request_id:
            from polardb_storage_resizer.redaction import redact_request_id

            result["request_id"] = redact_request_id(self.request_id)

        # Note: original_error is intentionally NOT included in serialization
        # to prevent sensitive data leaks

        return result


class TransientCloudAPIError(CloudAPIError):
    """
    Transient error that can be retried.

    Examples of transient errors:
    - Network timeout
    - 5xx server errors
    - Rate limiting (Throttling)
    - Service temporarily unavailable

    These errors should be retried with exponential backoff.
    """

    def is_transient(self) -> bool:
        """Transient errors are retryable."""
        return True


class PermanentCloudAPIError(CloudAPIError):
    """
    Permanent error that should not be retried.

    Examples of permanent errors:
    - 4xx client errors
    - Permission denied (UnauthorizedOperation)
    - Resource not found
    - Invalid parameters
    - Quota exceeded

    These errors indicate a problem that won't be fixed by retrying.
    """

    def is_transient(self) -> bool:
        """Permanent errors are not retryable."""
        return False


class ValidationError(ResizerError):
    """
    Configuration or input validation failed.

    Used when configuration values or input parameters are invalid.

    Attributes:
        message: Human-readable error message
        field: Name of the field that failed validation
    """

    def __init__(self, message: str, field: str | None = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.field = field

    def to_dict(self) -> dict[str, Any]:
        """Serialize error to dictionary."""
        result = super().to_dict()
        if self.field:
            result["field"] = self.field
        return result


class SafetyCheckError(ResizerError):
    """
    Safety threshold check failed.

    Raised when a proposed change exceeds configured safety limits.

    Attributes:
        message: Human-readable error message
        threshold_name: Name of the threshold that was exceeded
        actual_value: The actual value that exceeded the threshold
        limit_value: The configured limit value
    """

    def __init__(
        self,
        message: str,
        threshold_name: str | None = None,
        actual_value: float | None = None,
        limit_value: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, **kwargs)
        self.threshold_name = threshold_name
        self.actual_value = actual_value
        self.limit_value = limit_value

    def to_dict(self) -> dict[str, Any]:
        """Serialize error to dictionary."""
        result = super().to_dict()
        if self.threshold_name:
            result["threshold_name"] = self.threshold_name
        if self.actual_value is not None:
            result["actual_value"] = self.actual_value
        if self.limit_value is not None:
            result["limit_value"] = self.limit_value
        return result


class ConcurrentExecutionError(ResizerError):
    """
    Concurrent execution conflict detected.

    Raised when another instance of the resizer is already running.
    K8s CronJob concurrencyPolicy: Forbid should prevent this,
    but this error provides additional safety.

    Attributes:
        message: Human-readable error message
        conflicting_process_id: ID of the conflicting process
    """

    def __init__(
        self,
        message: str,
        conflicting_process_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, **kwargs)
        self.conflicting_process_id = conflicting_process_id

    def to_dict(self) -> dict[str, Any]:
        """Serialize error to dictionary."""
        result = super().to_dict()
        if self.conflicting_process_id:
            result["conflicting_process_id"] = self.conflicting_process_id
        return result
