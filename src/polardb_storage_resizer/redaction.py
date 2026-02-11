"""
Sensitive data redaction utilities for PolarDB Storage Resizer.

This module provides functions to redact sensitive information from:
- Error messages (request IDs, access keys, secrets)
- Cluster IDs (partial redaction for debugging)
- API response data

All error serialization should use these functions to prevent
sensitive data from leaking into logs.
"""

from __future__ import annotations

import json
import re
from typing import Any

# Patterns for sensitive data detection
ACCESS_KEY_PATTERN = re.compile(r"(?<![A-Z0-9])(AKIA|LTAI)[A-Z0-9]{16}(?![A-Z0-9])")
SECRET_KEY_PATTERN = re.compile(r"(sk-|sk_live-|sk_test-)[a-zA-Z0-9]{24,}")
TOKEN_PATTERN = re.compile(r"eyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*")
REQUEST_ID_PATTERN = re.compile(r"req-[a-zA-Z0-9-]+")
PASSWORD_PATTERN = re.compile(
    r"(password|passwd|pwd)[\"']?\s*[:=]\s*[\"'][^\"']+[\"']", re.IGNORECASE
)
SECRET_VALUE_PATTERN = re.compile(
    r"(secret_key|secret|token|api_key|access_key|credential)"
    r"[\"']?\s*[:=]\s*[\"'][^\"']{8,}[\"']",
    re.IGNORECASE,
)

# Keys that should be fully redacted in dictionaries
_SENSITIVE_KEYS: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "secret",
        "secret_key",
        "secretkey",
        "token",
        "access_token",
        "refresh_token",
        "api_key",
        "apikey",
        "access_key",
        "accesskey",
        "access_key_id",
        "access_key_secret",
        "private_key",
        "privatekey",
        "credential",
        "credentials",
        "auth",
        "authorization",
    }
)


def redact_cluster_id(cluster_id: str) -> str:
    """
    Redact a cluster ID for logging.

    Preserves the first 8 characters for debugging purposes,
    replaces the rest with asterisks.

    Args:
        cluster_id: Full cluster ID (e.g., "pc-xxxxxxxxxxxxxxxx")

    Returns:
        Redacted cluster ID (e.g., "pc-xxxxxxxx****")

    Example:
        >>> redact_cluster_id("pc-abc123def456")
        'pc-abc12****'
    """
    if not cluster_id:
        return cluster_id

    if len(cluster_id) <= 8:
        # Too short to meaningfully redact
        return cluster_id[:4] + "****"

    return cluster_id[:8] + "****"


def redact_request_id(request_id: str) -> str:
    """
    Redact a request ID for logging.

    Preserves the prefix and first few characters,
    replaces the rest with asterisks.

    Args:
        request_id: Full request ID

    Returns:
        Redacted request ID

    Example:
        >>> redact_request_id("req-secret-abc123xyz")
        'req-****'
    """
    if not request_id:
        return request_id

    # For request IDs, just show the prefix
    if request_id.startswith("req-"):
        return "req-****"

    # For other IDs, show first 4 chars
    if len(request_id) <= 4:
        return "****"

    return request_id[:4] + "****"


def redact_error_message(message: str) -> str:
    """
    Redact sensitive information from an error message.

    Removes/replaces:
    - Access keys (AKIA..., LTAI...)
    - Secret keys (sk-..., sk_live-...)
    - JWT tokens
    - Request IDs
    - Passwords and secrets in key-value pairs

    Args:
        message: Original error message

    Returns:
        Redacted message safe for logging

    Example:
        >>> redact_error_message("Error for AKIAXXXXXXXXXXXXXXXX")
        'Error for [REDACTED_ACCESS_KEY]'
    """
    if not message:
        return message

    result = message

    # Redact access keys
    result = ACCESS_KEY_PATTERN.sub("[REDACTED_ACCESS_KEY]", result)

    # Redact secret keys
    result = SECRET_KEY_PATTERN.sub("[REDACTED_SECRET]", result)

    # Redact JWT tokens
    result = TOKEN_PATTERN.sub("[REDACTED_TOKEN]", result)

    # Redact request IDs
    result = REQUEST_ID_PATTERN.sub("[REDACTED_REQUEST_ID]", result)

    # Redact passwords
    result = PASSWORD_PATTERN.sub("[REDACTED_PASSWORD]", result)

    # Redact other secrets
    result = SECRET_VALUE_PATTERN.sub("[REDACTED_SECRET]", result)

    return result


def redact_dict(data: dict[str, Any]) -> dict[str, Any]:
    """
    Redact sensitive information from a dictionary.

    Recursively processes dictionaries and lists to remove
    sensitive keys and values.

    Args:
        data: Dictionary to redact

    Returns:
        Redacted dictionary safe for logging
    """
    result: dict[str, Any] = {}

    for key, value in data.items():
        key_lower = key.lower()

        # Check if key is sensitive
        if key_lower in _SENSITIVE_KEYS:
            result[key] = "[REDACTED]"
            continue

        # Recursively process nested structures
        if isinstance(value, dict):
            result[key] = redact_dict(value)
        elif isinstance(value, list):
            result[key] = redact_list(value)
        elif isinstance(value, str):
            # Redact sensitive patterns in string values
            result[key] = redact_error_message(value)
        else:
            result[key] = value

    return result


def redact_list(data: list[Any]) -> list[Any]:
    """
    Redact sensitive information from a list.

    Args:
        data: List to redact

    Returns:
        Redacted list safe for logging
    """
    result: list[Any] = []

    for item in data:
        if isinstance(item, dict):
            result.append(redact_dict(item))
        elif isinstance(item, list):
            result.append(redact_list(item))
        elif isinstance(item, str):
            result.append(redact_error_message(item))
        else:
            result.append(item)

    return result


def redact_json(json_str: str) -> str:
    """
    Redact sensitive information from a JSON string.

    Args:
        json_str: JSON string to redact

    Returns:
        Redacted JSON string safe for logging
    """
    try:
        data = json.loads(json_str)
        redacted = redact_dict(data) if isinstance(data, dict) else redact_list(data)
        return json.dumps(redacted)
    except (json.JSONDecodeError, TypeError):
        # If not valid JSON, just redact as string
        return redact_error_message(json_str)


def redact_sdk_error(error: Exception) -> str:
    """
    Redact sensitive information from an SDK exception.

    Handles common SDK error patterns including:
    - Error messages
    - Response bodies
    - Request IDs

    Args:
        error: SDK exception to redact

    Returns:
        Redacted error message safe for logging
    """
    # Get the basic error message
    message = str(error)

    # Redact the message
    result = redact_error_message(message)

    # Try to redact response body if present
    if hasattr(error, "response"):
        response = error.response
        if isinstance(response, dict):
            # Replace with redacted version for reliable matching
            redacted_response = str(redact_dict(response))
            result = result.replace(redacted_response, "[REDACTED_RESPONSE]")
            # If original str(response) differs (e.g. whitespace),
            # also try replacing the original
            try:
                original_response = str(response)
            except (TypeError, ValueError):
                original_response = None

            if original_response:
                result = result.replace(original_response, "[REDACTED_RESPONSE]")

    return result
