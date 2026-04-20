"""
Tests for configuration loading and validation in polardb_storage_resizer.config.

This module tests:
- Default configuration values
- Required field validation
- Environment variable override
- Configuration file loading
- RRSA fast-fail in apply mode
- RRSA warning in dry-run mode

GREEN Phase: Tests should pass now that config.py is implemented.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Import from actual implementation
from polardb_storage_resizer.config import AppConfig
from polardb_storage_resizer.errors import ValidationError

# ==============================================================================
# Test Classes
# ==============================================================================


class TestDefaultValues:
    """Tests for default configuration values."""

    def test_default_run_mode(self) -> None:
        """Default run_mode should be 'dry-run' for safety."""
        config = AppConfig()
        assert config.run_mode == "dry-run"

    def test_default_log_level(self) -> None:
        """Default log_level should be 'INFO'."""
        config = AppConfig()
        assert config.log_level == "INFO"

    def test_default_buffer_percent(self) -> None:
        """buffer_percent should default to 105 but be configurable."""
        config = AppConfig()
        assert config.buffer_percent == 105

    def test_default_safety_thresholds(self) -> None:
        """Safety thresholds should have sensible defaults."""
        config = AppConfig()

        assert config.max_expand_ratio == 2.0
        assert config.max_shrink_ratio == 0.5
        assert config.max_single_change_gb == 1000
        assert config.min_change_threshold_gb == 10

    def test_default_retry_settings(self) -> None:
        """Retry settings should have sensible defaults."""
        config = AppConfig()

        assert config.retry_max_attempts == 3
        assert config.retry_backoff_base == 1.0
        assert config.retry_backoff_max == 30.0

    def test_default_concurrency_settings(self) -> None:
        """Concurrency settings should have sensible defaults."""
        config = AppConfig()

        assert config.max_parallel_requests == 5
        assert config.max_qps == 10


class TestRequiredFields:
    """Tests for required field validation."""

    def test_regions_required(self) -> None:
        """regions should be a required field."""
        config = AppConfig(regions=[])

        errors = config.validate()

        assert any("regions" in str(e).lower() for e in errors)

    def test_run_mode_required(self) -> None:
        """run_mode should be a required field."""
        config = AppConfig(run_mode=None)  # type: ignore

        errors = config.validate()

        assert any("run_mode" in str(e).lower() for e in errors)

    def test_valid_run_modes(self) -> None:
        """run_mode must be either 'dry-run' or 'apply'."""
        valid_modes = ["dry-run", "apply"]

        for mode in valid_modes:
            config = AppConfig(run_mode=mode, regions=["cn-hangzhou"])
            errors = config.validate()
            run_mode_errors = [e for e in errors if "run_mode" in str(e).lower()]
            assert len(run_mode_errors) == 0, f"Mode '{mode}' should be valid"

    def test_invalid_run_mode_rejected(self) -> None:
        """Invalid run_mode values should be rejected."""
        config = AppConfig(run_mode="invalid-mode", regions=["cn-hangzhou"])

        errors = config.validate()

        assert any("run_mode" in str(e).lower() for e in errors)


class TestSafetyThresholdValidation:
    """Tests for safety threshold validation."""

    def test_max_expand_ratio_must_be_positive(self) -> None:
        """max_expand_ratio must be > 1."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_expand_ratio=0.5,  # Invalid: < 1
        )

        errors = config.validate()

        assert any("max_expand_ratio" in str(e).lower() for e in errors)

    def test_max_shrink_ratio_must_be_valid(self) -> None:
        """max_shrink_ratio must be between 0 and 1."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_shrink_ratio=1.5,  # Invalid: > 1
        )

        errors = config.validate()

        assert any("max_shrink_ratio" in str(e).lower() for e in errors)

    def test_max_single_change_gb_must_be_positive(self) -> None:
        """max_single_change_gb must be > 0."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_single_change_gb=-100,  # Invalid: negative
        )

        errors = config.validate()

        assert any("max_single_change_gb" in str(e).lower() for e in errors)

    def test_min_change_threshold_gb_non_negative(self) -> None:
        """min_change_threshold_gb must be >= 0."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            min_change_threshold_gb=-10,  # Invalid: negative
        )

        errors = config.validate()

        assert any("min_change_threshold" in str(e).lower() for e in errors)

    def test_min_change_threshold_ge_max_single_change_rejected(self) -> None:
        """min_change_threshold_gb >= max_single_change_gb should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            min_change_threshold_gb=500,
            max_single_change_gb=500,
        )

        errors = config.validate()

        assert any(
            "min_change_threshold_gb" in str(e) and "max_single_change_gb" in str(e)
            for e in errors
        )

    def test_max_single_change_below_step_size_rejected(self) -> None:
        """max_single_change_gb below STORAGE_STEP_GB (10) should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_single_change_gb=5,  # Below 10GB step
        )

        errors = config.validate()

        assert any("step size" in str(e).lower() for e in errors)

    def test_max_single_change_not_multiple_of_step_rejected(self) -> None:
        """max_single_change_gb not a multiple of 10 should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_single_change_gb=15,
        )

        errors = config.validate()

        assert any("multiple" in str(e).lower() for e in errors)

    def test_min_change_threshold_not_multiple_of_step_rejected(self) -> None:
        """min_change_threshold_gb > 0 and not a multiple of 10 should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            min_change_threshold_gb=7,
        )

        errors = config.validate()

        assert any("multiple" in str(e).lower() for e in errors)

    def test_buffer_percent_exceeds_max_rejected(self) -> None:
        """buffer_percent > 300 should be rejected as likely typo."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            buffer_percent=500,
        )

        errors = config.validate()

        assert any("maximum" in str(e).lower() for e in errors)

    def test_buffer_percent_300_accepted(self) -> None:
        """buffer_percent = 300 should be accepted (upper bound is exclusive)."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            buffer_percent=300,
        )

        errors = config.validate()

        assert not any("maximum" in str(e).lower() for e in errors)


class TestEnvironmentVariableLoading:
    """Tests for loading configuration from environment variables."""

    def test_load_run_mode_from_env(self, isolated_env: dict[str, str]) -> None:
        """run_mode should be loaded from RUN_MODE env var."""
        os.environ["RUN_MODE"] = "apply"

        config = AppConfig.from_env()

        assert config.run_mode == "apply"

    def test_load_regions_from_env(self, isolated_env: dict[str, str]) -> None:
        """regions should be loaded from REGIONS env var (comma-separated)."""
        os.environ["REGIONS"] = "cn-hangzhou,cn-beijing,cn-shanghai"

        config = AppConfig.from_env()

        assert config.regions == ["cn-hangzhou", "cn-beijing", "cn-shanghai"]

    def test_load_log_level_from_env(self, isolated_env: dict[str, str]) -> None:
        """log_level should be loaded from LOG_LEVEL env var."""
        os.environ["LOG_LEVEL"] = "DEBUG"

        config = AppConfig.from_env()

        assert config.log_level == "DEBUG"

    def test_load_max_qps_from_env(self, isolated_env: dict[str, str]) -> None:
        """max_qps should be loaded from MAX_QPS env var."""
        os.environ["MAX_QPS"] = "20"

        config = AppConfig.from_env()

        assert config.max_qps == 20

    def test_env_overrides_defaults(self, isolated_env: dict[str, str]) -> None:
        """Environment variables should override default values."""
        os.environ["RUN_MODE"] = "apply"
        os.environ["REGIONS"] = "cn-hangzhou"
        os.environ["MAX_PARALLEL_REQUESTS"] = "10"
        os.environ["MAX_EXPAND_RATIO"] = "1.5"

        config = AppConfig.from_env()

        assert config.run_mode == "apply"
        assert config.max_parallel_requests == 10
        assert config.max_expand_ratio == 1.5

    def test_load_buffer_percent_from_env(self, isolated_env: dict[str, str]) -> None:
        """buffer_percent should be loaded from BUFFER_PERCENT env var."""
        os.environ["BUFFER_PERCENT"] = "110"

        config = AppConfig.from_env()

        assert config.buffer_percent == 110

    def test_load_min_change_threshold_from_env(
        self, isolated_env: dict[str, str]
    ) -> None:
        """min_change_threshold_gb should be loaded from MIN_CHANGE_THRESHOLD_GB."""
        os.environ["MIN_CHANGE_THRESHOLD_GB"] = "20"

        config = AppConfig.from_env()

        assert config.min_change_threshold_gb == 20


class TestYAMLFileLoading:
    """Tests for loading configuration from YAML file."""

    def test_load_from_yaml_file(self) -> None:
        """Configuration should be loadable from YAML file."""
        config_path = Path(__file__).parent / "fixtures" / "sample_config.yaml"

        config = AppConfig.from_yaml(config_path)

        assert config.run_mode == "dry-run"
        assert "cn-hangzhou" in config.regions
        assert "cn-beijing" in config.regions

    def test_yaml_file_not_found(self) -> None:
        """Loading from non-existent file should raise error."""
        config_path = Path("/nonexistent/config.yaml")

        with pytest.raises(FileNotFoundError):
            AppConfig.from_yaml(config_path)

    def test_yaml_invalid_syntax(self, tmp_path: Path) -> None:
        """Loading from malformed YAML should raise ValidationError."""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("{ invalid yaml: [unclosed", encoding="utf-8")

        with pytest.raises(ValidationError):
            AppConfig.from_yaml(bad_yaml)

    def test_load_buffer_percent_from_yaml(self, tmp_path: Path) -> None:
        """buffer_percent should be loadable from YAML safety section."""
        yaml_content = """
regions:
  - cn-hangzhou
safety:
  buffer_percent: 120
"""
        yaml_file = tmp_path / "buffer_config.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")

        config = AppConfig.from_yaml(yaml_file)

        assert config.buffer_percent == 120

    def test_load_buffer_percent_from_yaml_toplevel(self, tmp_path: Path) -> None:
        """buffer_percent should be loadable from YAML top-level (backward compat)."""
        yaml_content = """
regions:
  - cn-hangzhou
buffer_percent: 115
"""
        yaml_file = tmp_path / "buffer_toplevel.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")

        config = AppConfig.from_yaml(yaml_file)

        assert config.buffer_percent == 115

    def test_yaml_null_safety_section(self, tmp_path: Path) -> None:
        """YAML with null safety section should not crash."""
        yaml_content = """
regions:
  - cn-hangzhou
safety:
"""
        yaml_file = tmp_path / "null_safety.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")

        config = AppConfig.from_yaml(yaml_file)

        assert config.regions == ["cn-hangzhou"]
        assert config.buffer_percent == 105  # Default

    def test_yaml_null_list_fields(self, tmp_path: Path) -> None:
        """YAML with null list/dict fields should not crash."""
        yaml_content = """
regions:
cluster_whitelist:
cluster_blacklist:
cluster_tag_filters:
"""
        yaml_file = tmp_path / "null_fields.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")

        config = AppConfig.from_yaml(yaml_file)

        assert config.regions == []
        assert config.cluster_whitelist == []
        assert config.cluster_blacklist == []
        assert config.cluster_tag_filters == {}

    def test_yaml_null_retry_section(self, tmp_path: Path) -> None:
        """YAML with null retry section should not crash."""
        yaml_content = """
regions:
  - cn-hangzhou
retry:
"""
        yaml_file = tmp_path / "null_retry.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")

        config = AppConfig.from_yaml(yaml_file)

        assert config.retry_max_attempts == 3  # Default


class TestRRSAFastFail:
    """Tests for RRSA fast-fail behavior in apply mode."""

    def test_apply_mode_requires_rrsa_role_arn(
        self, isolated_env: dict[str, str]
    ) -> None:
        """apply mode should fail fast if ALIBABA_CLOUD_ROLE_ARN is missing."""
        os.environ["RUN_MODE"] = "apply"
        os.environ["REGIONS"] = "cn-hangzhou"
        # ALIBABA_CLOUD_ROLE_ARN is NOT set

        config = AppConfig.from_env()
        is_valid, error_message = config.validate_rrsa()

        assert is_valid is False
        assert "role" in error_message.lower() or "rrsa" in error_message.lower()

    def test_apply_mode_with_rrsa_role_arn(self, isolated_env: dict[str, str]) -> None:
        """apply mode should pass validation with ALIBABA_CLOUD_ROLE_ARN set."""
        os.environ["RUN_MODE"] = "apply"
        os.environ["REGIONS"] = "cn-hangzhou"
        os.environ["ALIBABA_CLOUD_ROLE_ARN"] = "acs:ram::123456789:role/TestRole"

        config = AppConfig.from_env()
        is_valid, error_message = config.validate_rrsa()

        assert is_valid is True

    def test_apply_mode_with_eci_role_arn(self, isolated_env: dict[str, str]) -> None:
        """apply mode should accept ALIBABA_CLOUD_ECI_ROLE_ARN as alternative."""
        os.environ["RUN_MODE"] = "apply"
        os.environ["REGIONS"] = "cn-hangzhou"
        os.environ["ALIBABA_CLOUD_ECI_ROLE_ARN"] = "acs:ram::123456789:role/TestRole"

        config = AppConfig.from_env()
        is_valid, error_message = config.validate_rrsa()

        assert is_valid is True


class TestRRSADryRunWarning:
    """Tests for RRSA warning in dry-run mode."""

    def test_dry_run_mode_rrsa_optional(self, isolated_env: dict[str, str]) -> None:
        """dry-run mode should not require RRSA credentials."""
        os.environ["RUN_MODE"] = "dry-run"
        os.environ["REGIONS"] = "cn-hangzhou"
        # No RRSA env vars set

        config = AppConfig.from_env()
        is_valid, error_message = config.validate_rrsa()

        # Should be valid even without RRSA
        assert is_valid is True

    def test_dry_run_mode_rrsa_warning(self, isolated_env: dict[str, str]) -> None:
        """dry-run mode without RRSA should issue a warning (not error)."""
        os.environ["RUN_MODE"] = "dry-run"
        os.environ["REGIONS"] = "cn-hangzhou"

        config = AppConfig.from_env()
        is_valid, _ = config.validate_rrsa()

        # Should be valid but log a warning
        assert is_valid is True


class TestRetryConfiguration:
    """Tests for retry configuration validation."""

    def test_retry_max_attempts_positive(self) -> None:
        """retry_max_attempts must be > 0."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            retry_max_attempts=0,  # Invalid
        )

        errors = config.validate()

        assert any("retry_max_attempts" in str(e).lower() for e in errors)

    def test_retry_backoff_base_positive(self) -> None:
        """retry_backoff_base must be > 0."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            retry_backoff_base=-1.0,  # Invalid
        )

        errors = config.validate()

        assert any("retry_backoff_base" in str(e).lower() for e in errors)

    def test_retry_backoff_max_greater_than_base(self) -> None:
        """retry_backoff_max should be >= retry_backoff_base."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            min_change_threshold_gb=10,
            retry_backoff_base=10.0,
            retry_backoff_max=5.0,  # Invalid: less than base
        )

        errors = config.validate()

        assert len(errors) == 1
        assert "backoff_max" in errors[0] and "backoff_base" in errors[0]


class TestConcurrencyConfiguration:
    """Tests for concurrency configuration validation."""

    def test_max_parallel_requests_positive(self) -> None:
        """max_parallel_requests must be > 0."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_parallel_requests=0,  # Invalid
        )

        errors = config.validate()

        assert any("max_parallel_requests" in str(e).lower() for e in errors)

    def test_max_qps_positive(self) -> None:
        """max_qps must be > 0."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_qps=-5,  # Invalid
        )

        errors = config.validate()

        assert any("max_qps" in str(e).lower() for e in errors)

    def test_max_parallel_exceeds_max_qps_rejected(self) -> None:
        """max_parallel_requests > max_qps should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            max_parallel_requests=20,
            max_qps=5,
        )

        errors = config.validate()

        assert any("max_parallel_requests" in e and "max_qps" in e for e in errors)


class TestAPITimeoutConfiguration:
    """Tests for API timeout configuration."""

    def test_default_timeouts(self) -> None:
        """Default timeouts should be reasonable."""
        config = AppConfig(regions=["cn-hangzhou"])
        assert config.api_connect_timeout == 5
        assert config.api_read_timeout == 30

    def test_connect_timeout_from_env(self, isolated_env: dict[str, str]) -> None:
        """api_connect_timeout should be loadable from env."""
        os.environ["API_CONNECT_TIMEOUT"] = "10"
        config = AppConfig.from_env()
        assert config.api_connect_timeout == 10

    def test_read_timeout_from_env(self, isolated_env: dict[str, str]) -> None:
        """api_read_timeout should be loadable from env."""
        os.environ["API_READ_TIMEOUT"] = "60"
        config = AppConfig.from_env()
        assert config.api_read_timeout == 60

    def test_invalid_connect_timeout_rejected(self) -> None:
        """api_connect_timeout <= 0 should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            api_connect_timeout=0,
        )
        errors = config.validate()
        assert any("api_connect_timeout" in str(e).lower() for e in errors)

    def test_invalid_read_timeout_rejected(self) -> None:
        """api_read_timeout <= 0 should be rejected."""
        config = AppConfig(
            regions=["cn-hangzhou"],
            api_read_timeout=-1,
        )
        errors = config.validate()
        assert any("api_read_timeout" in str(e).lower() for e in errors)


class TestConfigImmutability:
    """Tests for configuration immutability and validation."""

    def test_buffer_percent_must_be_integer(self) -> None:
        """buffer_percent must be an integer, not float."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            buffer_percent=105.5,  # Float, should be rejected
        )

        errors = config.validate()

        assert any("integer" in e for e in errors)

    def test_buffer_percent_must_be_greater_than_100(self) -> None:
        """buffer_percent must be > 100 to ensure storage buffer."""
        config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            buffer_percent=110,  # Valid value > 100
        )

        # buffer_percent is now configurable
        assert config.buffer_percent == 110

        # Invalid value (<= 100) should fail validation
        invalid_config = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            buffer_percent=100,  # Invalid - must be > 100
        )
        errors = invalid_config.validate()
        assert any("buffer_percent" in str(e).lower() for e in errors)

    def test_buffer_percent_can_be_customized(self) -> None:
        """buffer_percent can be customized to any value > 100."""
        # Test with 110% (1.10x buffer)
        config_110 = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            buffer_percent=110,
        )
        assert config_110.buffer_percent == 110

        # Test with 120% (1.20x buffer)
        config_120 = AppConfig(
            run_mode="dry-run",
            regions=["cn-hangzhou"],
            buffer_percent=120,
        )
        assert config_120.buffer_percent == 120


class TestClusterTagFilters:
    """Tests for cluster_tag_filters configuration.

    RED Phase: These tests should FAIL because from_env() does not support
    CLUSTER_TAG_FILTERS environment variable parsing.
    """

    def test_from_env_with_tag_filters(self, isolated_env: dict[str, str]) -> None:
        """Test loading cluster_tag_filters from environment variable.

        Format: CLUSTER_TAG_FILTERS=key1:value1,key2:value2

        This test should FAIL because from_env() does not yet support
        parsing CLUSTER_TAG_FILTERS environment variable.
        """
        os.environ["CLUSTER_TAG_FILTERS"] = "Environment:production,Team:backend"
        config = AppConfig.from_env()

        # This assertion will fail because from_env() doesn't parse CLUSTER_TAG_FILTERS
        assert config.cluster_tag_filters == {
            "Environment": "production",
            "Team": "backend",
        }

    def test_from_env_with_empty_tag_filters(
        self, isolated_env: dict[str, str]
    ) -> None:
        """Test empty cluster_tag_filters when env var not set.

        This test should PASS because cluster_tag_filters defaults to {}.
        """
        # CLUSTER_TAG_FILTERS is not set
        config = AppConfig.from_env()

        # Should default to empty dict
        assert config.cluster_tag_filters == {}

    def test_from_env_with_invalid_tag_format(
        self, isolated_env: dict[str, str]
    ) -> None:
        """Test handling of invalid tag format (should skip or warn).

        Format: CLUSTER_TAG_FILTERS=valid:value,invalid_entry,another:valid

        Invalid entries (without colon) should be skipped.
        This test should FAIL because from_env() doesn't parse CLUSTER_TAG_FILTERS.
        """
        os.environ["CLUSTER_TAG_FILTERS"] = "valid:value,invalid_entry,another:valid"
        config = AppConfig.from_env()

        # Should only preserve valid format entries
        assert config.cluster_tag_filters == {
            "valid": "value",
            "another": "valid",
        }

    def test_from_env_with_colon_in_value(self, isolated_env: dict[str, str]) -> None:
        """Test handling of colons in tag values.

        Format: CLUSTER_TAG_FILTERS=key:value:with:colons

        The value should include all parts after the first colon.
        This test should FAIL because from_env() doesn't parse CLUSTER_TAG_FILTERS.
        """
        os.environ["CLUSTER_TAG_FILTERS"] = "url:https://example.com,path:/api/v1"
        config = AppConfig.from_env()

        # Values should include colons
        assert config.cluster_tag_filters == {
            "url": "https://example.com",
            "path": "/api/v1",
        }

    def test_from_yaml_with_tag_filters(self) -> None:
        """Test loading cluster_tag_filters from YAML file.

        This test should PASS because from_yaml() already supports cluster_tag_filters.
        """
        yaml_content = """
regions:
  - cn-hangzhou
cluster_tag_filters:
  Environment: production
  Team: backend
"""
        config_file = Path(__file__).parent / "fixtures" / "test_tag_filters.yaml"
        config_file.write_text(yaml_content)

        try:
            config = AppConfig.from_yaml(config_file)

            assert config.cluster_tag_filters == {
                "Environment": "production",
                "Team": "backend",
            }
        finally:
            # Clean up test file
            config_file.unlink()

    def test_tag_filters_default_empty(self) -> None:
        """Test that cluster_tag_filters defaults to empty dict.

        This test should PASS because the field has default_factory=dict.
        """
        config = AppConfig(regions=["cn-hangzhou"])

        assert config.cluster_tag_filters == {}

    def test_tag_filters_can_be_set_directly(self) -> None:
        """Test that cluster_tag_filters can be set in constructor.

        This test should PASS because cluster_tag_filters is a dataclass field.
        """
        config = AppConfig(
            regions=["cn-hangzhou"],
            cluster_tag_filters={"Environment": "staging", "Owner": "team-alpha"},
        )

        assert config.cluster_tag_filters == {
            "Environment": "staging",
            "Owner": "team-alpha",
        }


# ==============================================================================
# Entry point
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
