"""
Tests for redaction module in polardb_storage_resizer.redaction.

Tests cover all public functions with edge cases and adversarial inputs.
"""

from __future__ import annotations

import json

from polardb_storage_resizer.redaction import (
    ACCESS_KEY_PATTERN,
    SECRET_VALUE_PATTERN,
    redact_cluster_id,
    redact_dict,
    redact_error_message,
    redact_json,
    redact_list,
    redact_request_id,
    redact_sdk_error,
)

# ==============================================================================
# redact_cluster_id
# ==============================================================================


class TestRedactClusterId:
    def test_normal_cluster_id(self) -> None:
        assert redact_cluster_id("pc-abc123def456") == "pc-abc12****"

    def test_short_cluster_id(self) -> None:
        assert redact_cluster_id("pc-ab") == "pc-a****"

    def test_empty_string(self) -> None:
        assert redact_cluster_id("") == ""

    def test_exactly_8_chars(self) -> None:
        # len("pc-1234") = 7 <= 8, so short path: first 4 + ****
        assert redact_cluster_id("pc-1234") == "pc-1****"

    def test_exactly_8_chars_boundary(self) -> None:
        # len("pc-12345") = 8, still <= 8 path: first 4 + ****
        assert redact_cluster_id("pc-12345") == "pc-1****"

    def test_9_chars(self) -> None:
        # len > 8, so first 8 + ****
        assert redact_cluster_id("pc-123456") == "pc-12345****"

    def test_long_cluster_id(self) -> None:
        assert redact_cluster_id("pc-abcdefghij1234567890") == "pc-abcde****"


# ==============================================================================
# redact_request_id
# ==============================================================================


class TestRedactRequestId:
    def test_req_prefix(self) -> None:
        assert redact_request_id("req-secret-abc123xyz") == "req-****"

    def test_empty_string(self) -> None:
        assert redact_request_id("") == ""

    def test_short_id(self) -> None:
        assert redact_request_id("abc") == "****"

    def test_non_req_prefix(self) -> None:
        assert redact_request_id("other-prefix-id") == "othe****"


# ==============================================================================
# redact_error_message
# ==============================================================================


class TestRedactErrorMessage:
    def test_redacts_access_key(self) -> None:
        msg = "Error for AKIAXXXXXXXXXXXXXXXX in cluster"
        result = redact_error_message(msg)
        assert "[REDACTED_ACCESS_KEY]" in result
        assert "AKIA" not in result

    def test_redacts_ltai_access_key(self) -> None:
        # LTAI prefix + exactly 16 uppercase hex chars
        msg = "Failed with LTAI0123456789ABCDEF"
        result = redact_error_message(msg)
        assert "[REDACTED_ACCESS_KEY]" in result
        assert "LTAI" not in result

    def test_redacts_secret_key(self) -> None:
        msg = "Secret: sk-abcdefghijklmnopqrstuvwxyz123456"
        result = redact_error_message(msg)
        assert "[REDACTED_SECRET]" in result
        assert "sk-" not in result

    def test_redacts_jwt_token(self) -> None:
        token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123"
        result = redact_error_message(f"Auth failed: {token}")
        assert "[REDACTED_TOKEN]" in result
        assert "eyJ" not in result

    def test_redacts_request_id(self) -> None:
        msg = "Request req-abc123-xyz failed"
        result = redact_error_message(msg)
        assert "[REDACTED_REQUEST_ID]" in result
        assert "req-abc123-xyz" not in result

    def test_redacts_password(self) -> None:
        msg = 'Config: password="supersecret123"'
        result = redact_error_message(msg)
        assert "[REDACTED_PASSWORD]" in result
        assert "supersecret" not in result

    def test_redacts_credential_value(self) -> None:
        msg = 'credential="my_credential_value_12345"'
        result = redact_error_message(msg)
        assert "[REDACTED_SECRET]" in result

    def test_empty_string(self) -> None:
        assert redact_error_message("") == ""

    def test_no_sensitive_data(self) -> None:
        msg = "Simple error: timeout after 30 seconds"
        assert redact_error_message(msg) == msg

    def test_multiple_patterns(self) -> None:
        msg = (
            'AKIAXXXXXXXXXXXXXXXX with password="secret123" '
            'and token="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKx"'
        )
        result = redact_error_message(msg)
        assert "AKIA" not in result
        assert "secret123" not in result
        assert "eyJhbG" not in result
        assert "[REDACTED_ACCESS_KEY]" in result
        assert "[REDACTED_PASSWORD]" in result
        # token="..." matches SECRET_VALUE_PATTERN (key=value format)
        # so it's redacted as [REDACTED_SECRET] before TOKEN_PATTERN runs
        assert "[REDACTED_SECRET]" in result

    def test_redacts_key_value(self) -> None:
        msg = 'api_key="value_with_more_than_8_chars"'
        result = redact_error_message(msg)
        assert "[REDACTED_SECRET]" in result


# ==============================================================================
# redact_dict
# ==============================================================================


class TestRedactDict:
    def test_redacts_sensitive_keys(self) -> None:
        data = {"name": "test", "password": "secret123", "api_key": "key-xyz"}
        result = redact_dict(data)
        assert result["name"] == "test"
        assert result["password"] == "[REDACTED]"
        assert result["api_key"] == "[REDACTED]"

    def test_recursive_nesting(self) -> None:
        data = {
            "outer": {
                "inner": {
                    "secret": "hidden_value",
                    "safe": "visible",
                }
            }
        }
        result = redact_dict(data)
        assert result["outer"]["inner"]["secret"] == "[REDACTED]"
        assert result["outer"]["inner"]["safe"] == "visible"

    def test_redacts_strings_with_patterns(self) -> None:
        data = {"message": "Error for AKIAXXXXXXXXXXXXXXXX"}
        result = redact_dict(data)
        assert "[REDACTED_ACCESS_KEY]" in result["message"]

    def test_preserves_non_sensitive_values(self) -> None:
        data = {"count": 42, "enabled": True, "ratio": 3.14}
        result = redact_dict(data)
        assert result == data

    def test_empty_dict(self) -> None:
        assert redact_dict({}) == {}

    def test_list_values(self) -> None:
        data = {"items": [{"password": "secret_val"}]}
        result = redact_dict(data)
        assert result["items"][0]["password"] == "[REDACTED]"


# ==============================================================================
# redact_list
# ==============================================================================


class TestRedactList:
    def test_simple_list(self) -> None:
        data = ["hello", "world"]
        assert redact_list(data) == ["hello", "world"]

    def test_list_with_sensitive_strings(self) -> None:
        data = ["normal", "AKIAXXXXXXXXXXXXXXXX", "other"]
        result = redact_list(data)
        assert result[0] == "normal"
        assert "[REDACTED_ACCESS_KEY]" in result[1]
        assert result[2] == "other"

    def test_nested_list(self) -> None:
        data = [["AKIAXXXXXXXXXXXXXXXX"]]
        result = redact_list(data)
        assert "[REDACTED_ACCESS_KEY]" in result[0][0]

    def test_list_of_dicts(self) -> None:
        data = [{"password": "hidden"}, {"name": "visible"}]
        result = redact_list(data)
        assert result[0]["password"] == "[REDACTED]"
        assert result[1]["name"] == "visible"

    def test_empty_list(self) -> None:
        assert redact_list([]) == []

    def test_mixed_types(self) -> None:
        data = ["text", 42, 3.14, None, {"secret": "val"}]
        result = redact_list(data)
        assert result[0] == "text"
        assert result[1] == 42
        assert result[2] == 3.14
        assert result[3] is None
        assert result[4]["secret"] == "[REDACTED]"


# ==============================================================================
# redact_json
# ==============================================================================


class TestRedactJson:
    def test_valid_json_dict(self) -> None:
        json_str = '{"password": "secret123", "name": "test"}'
        result = redact_json(json_str)
        data = json.loads(result)
        assert data["password"] == "[REDACTED]"
        assert data["name"] == "test"

    def test_valid_json_list(self) -> None:
        json_str = '["normal", "AKIAXXXXXXXXXXXXXXXX"]'
        result = redact_json(json_str)
        data = json.loads(result)
        assert data[0] == "normal"
        assert "[REDACTED_ACCESS_KEY]" in data[1]

    def test_invalid_json(self) -> None:
        json_str = "not valid json with AKIAXXXXXXXXXXXXXXXX"
        result = redact_json(json_str)
        assert "[REDACTED_ACCESS_KEY]" in result

    def test_empty_json_string(self) -> None:
        result = redact_json("{}")
        assert result == "{}"

    def test_nested_json(self) -> None:
        json_str = '{"outer": {"password": "hidden"}}'
        result = redact_json(json_str)
        data = json.loads(result)
        assert data["outer"]["password"] == "[REDACTED]"


# ==============================================================================
# redact_sdk_error
# ==============================================================================


class TestRedactSdkError:
    def test_basic_exception(self) -> None:
        error = Exception("Error with AKIAXXXXXXXXXXXXXXXX")
        result = redact_sdk_error(error)
        assert "[REDACTED_ACCESS_KEY]" in result

    def test_exception_with_response_dict(self) -> None:
        class MockError(Exception):
            def __init__(self, msg: str) -> None:
                super().__init__(msg)
                self.response = {"secret": "value"}

        error = MockError("test error")
        result = redact_sdk_error(error)
        assert "test error" in result
        # Response is processed but not included in output directly

    def test_exception_with_response_non_dict(self) -> None:
        class MockError(Exception):
            def __init__(self, msg: str) -> None:
                super().__init__(msg)
                self.response = "string response"

        error = MockError("test error")
        result = redact_sdk_error(error)
        assert "test error" in result

    def test_exception_with_credential_message(self) -> None:
        error = Exception(
            'failed to refresh OAuth token, response: {"access_key": '
            '"LTAI0123456789ABCD"}'
        )
        result = redact_sdk_error(error)
        assert "LTAI" not in result

    def test_normal_exception_unchanged(self) -> None:
        error = ValueError("timeout after 30 seconds")
        result = redact_sdk_error(error)
        assert result == "timeout after 30 seconds"


# ==============================================================================
# Pattern Tests
# ==============================================================================


class TestPatterns:
    def test_access_key_pattern_matches_akia(self) -> None:
        assert ACCESS_KEY_PATTERN.search("AKIAXXXXXXXXXXXXXXXX")

    def test_access_key_pattern_matches_ltai(self) -> None:
        # LTAI + exactly 16 uppercase hex chars
        assert ACCESS_KEY_PATTERN.search("LTAI0123456789ABCDEF")

    def test_access_key_pattern_no_false_positive(self) -> None:
        assert not ACCESS_KEY_PATTERN.search("NOTAKIA123")

    def test_secret_value_pattern_catches_credential(self) -> None:
        assert SECRET_VALUE_PATTERN.search('credential="value_12345"')

    def test_secret_value_pattern_catches_secret(self) -> None:
        assert SECRET_VALUE_PATTERN.search('secret="value_12345"')

    def test_secret_value_pattern_catches_token(self) -> None:
        assert SECRET_VALUE_PATTERN.search('token="value_12345"')
