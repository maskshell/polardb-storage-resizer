"""
Configuration loading and validation for PolarDB Storage Resizer.

This module provides:
- AppConfig: Application configuration dataclass
- Configuration loading from environment variables and YAML files
- Startup validation (fail-fast for configuration errors)
- RRSA credential validation
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from polardb_storage_resizer.errors import ValidationError


@dataclass
class AppConfig:
    """
    Application configuration.

    Configuration can be loaded from:
    1. Environment variables (highest priority)
    2. YAML configuration file
    3. Default values

    Attributes:
        run_mode: "dry-run" (no changes) or "apply" (make changes)
        regions: List of regions to process
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        metrics_enabled: Whether to emit metrics
        max_parallel_requests: Maximum concurrent API requests
        buffer_percent: Storage buffer percentage (default 105%)
        max_expand_ratio: Maximum expansion ratio (e.g., 2.0 = double)
        max_shrink_ratio: Minimum ratio after shrink (e.g., 0.5 = halve)
        max_single_change_gb: Maximum single change in GB
        min_change_threshold_gb: Minimum change size to trigger
        max_qps: Maximum API queries per second
        retry_max_attempts: Maximum retry attempts
        retry_backoff_base: Base for exponential backoff (seconds)
        retry_backoff_max: Maximum backoff time (seconds)
        cluster_whitelist: Optional list of cluster IDs to process
        cluster_blacklist: Optional list of cluster IDs to exclude
            (takes priority over whitelist)
        cluster_tag_filters: Optional tag filters for cluster selection
        api_connect_timeout: API connection timeout in seconds (default 5)
        api_read_timeout: API read timeout in seconds (default 30)
    """

    run_mode: str = "dry-run"  # "dry-run" or "apply"
    regions: list[str] = field(default_factory=list)
    log_level: str = "INFO"
    metrics_enabled: bool = True
    max_parallel_requests: int = 5
    buffer_percent: int = 105
    max_expand_ratio: float = 2.0
    max_shrink_ratio: float = 0.5
    max_single_change_gb: int = 1000
    min_change_threshold_gb: int = 10
    max_qps: int = 10
    retry_max_attempts: int = 3
    retry_backoff_base: float = 1.0
    retry_backoff_max: float = 30.0
    cluster_whitelist: list[str] = field(default_factory=list)
    cluster_blacklist: list[str] = field(default_factory=list)
    cluster_tag_filters: dict[str, str] = field(default_factory=dict)
    api_connect_timeout: int = 5
    api_read_timeout: int = 30

    def __post_init__(self) -> None:
        """Validate and normalize configuration after initialization."""
        # Normalize run_mode
        if self.run_mode:
            self.run_mode = self.run_mode.lower().strip()

        # Normalize log_level
        if self.log_level:
            self.log_level = self.log_level.upper().strip()

    @classmethod
    def from_env(cls) -> AppConfig:
        """
        Load configuration from environment variables.

        Environment variable mapping:
        - RUN_MODE -> run_mode
        - REGIONS -> regions (comma-separated)
        - LOG_LEVEL -> log_level
        - METRICS_ENABLED -> metrics_enabled
        - MAX_PARALLEL_REQUESTS -> max_parallel_requests
        - MAX_EXPAND_RATIO -> max_expand_ratio
        - MAX_SHRINK_RATIO -> max_shrink_ratio
        - MAX_SINGLE_CHANGE_GB -> max_single_change_gb
        - MIN_CHANGE_THRESHOLD_GB -> min_change_threshold_gb
        - MAX_QPS -> max_qps
        - RETRY_MAX_ATTEMPTS -> retry_max_attempts
        - RETRY_BACKOFF_BASE -> retry_backoff_base
        - RETRY_BACKOFF_MAX -> retry_backoff_max
        - BUFFER_PERCENT -> buffer_percent
        - CLUSTER_WHITELIST -> cluster_whitelist (comma-separated)
        - CLUSTER_BLACKLIST -> cluster_blacklist (comma-separated)
        - CLUSTER_TAG_FILTERS -> cluster_tag_filters (key:value pairs, comma-separated)
        - API_CONNECT_TIMEOUT -> api_connect_timeout
        - API_READ_TIMEOUT -> api_read_timeout
        - ALIBABA_CLOUD_ROLE_ARN / ALIBABA_CLOUD_ECI_ROLE_ARN -> RRSA validation

        Returns:
            AppConfig instance with values from environment
        """

        def get_env_list(key: str) -> list[str]:
            """Get comma-separated list from environment variable."""
            value = os.environ.get(key, "")
            if not value:
                return []
            return [item.strip() for item in value.split(",") if item.strip()]

        def get_env_float(key: str, default: float) -> float:
            """Get float from environment variable."""
            value = os.environ.get(key)
            if value is None:
                return default
            try:
                return float(value)
            except ValueError:
                logging.getLogger(__name__).warning(
                    "Invalid float value for %s=%r, using default %s",
                    key,
                    value,
                    default,
                )
                return default

        def get_env_int(key: str, default: int) -> int:
            """Get int from environment variable."""
            value = os.environ.get(key)
            if value is None:
                return default
            try:
                return int(value)
            except ValueError:
                logging.getLogger(__name__).warning(
                    "Invalid int value for %s=%r, using default %s",
                    key,
                    value,
                    default,
                )
                return default

        def get_env_bool(key: str, default: bool) -> bool:
            """Get bool from environment variable."""
            value = os.environ.get(key)
            if value is None:
                return default
            return value.strip().lower() in ("true", "1", "yes", "on")

        def get_env_dict(key: str) -> dict[str, str]:
            """Parse key:value pairs from environment variable.

            Format: "key1:value1,key2:value2"
            - Entries are comma-separated
            - Split on first colon only (allows values with colons)
            - Invalid entries (no colon) are silently skipped
            """
            value = os.environ.get(key, "")
            if not value:
                return {}

            result: dict[str, str] = {}
            for item in value.split(","):
                item = item.strip()
                if ":" in item:
                    k, v = item.split(":", 1)  # Split on first colon only
                    k = k.strip()
                    v = v.strip()
                    if k:  # Only add if key is non-empty
                        result[k] = v
            return result

        return cls(
            run_mode=os.environ.get("RUN_MODE", "dry-run"),
            regions=get_env_list("REGIONS"),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            metrics_enabled=get_env_bool("METRICS_ENABLED", True),
            max_parallel_requests=get_env_int("MAX_PARALLEL_REQUESTS", 5),
            buffer_percent=get_env_int("BUFFER_PERCENT", 105),
            max_expand_ratio=get_env_float("MAX_EXPAND_RATIO", 2.0),
            max_shrink_ratio=get_env_float("MAX_SHRINK_RATIO", 0.5),
            max_single_change_gb=get_env_int("MAX_SINGLE_CHANGE_GB", 1000),
            min_change_threshold_gb=get_env_int("MIN_CHANGE_THRESHOLD_GB", 10),
            max_qps=get_env_int("MAX_QPS", 10),
            retry_max_attempts=get_env_int("RETRY_MAX_ATTEMPTS", 3),
            retry_backoff_base=get_env_float("RETRY_BACKOFF_BASE", 1.0),
            retry_backoff_max=get_env_float("RETRY_BACKOFF_MAX", 30.0),
            cluster_whitelist=get_env_list("CLUSTER_WHITELIST"),
            cluster_blacklist=get_env_list("CLUSTER_BLACKLIST"),
            cluster_tag_filters=get_env_dict("CLUSTER_TAG_FILTERS"),
            api_connect_timeout=get_env_int("API_CONNECT_TIMEOUT", 5),
            api_read_timeout=get_env_int("API_READ_TIMEOUT", 30),
        )

    @classmethod
    def from_yaml(cls, path: Path) -> AppConfig:
        """
        Load configuration from YAML file.

        Args:
            path: Path to YAML configuration file

        Returns:
            AppConfig instance with values from YAML file

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            ValidationError: If the configuration is invalid
        """
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        try:
            with path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValidationError(
                message=f"Invalid YAML in configuration file {path}: {e}",
                field="config",
            ) from e

        if data is None:
            data = {}

        # Parse nested sections (use `or {}` to handle explicit null values)
        safety = data.get("safety") or {}
        retry = data.get("retry") or {}

        return cls(
            run_mode=data.get("run_mode", "dry-run"),
            regions=data.get("regions") or [],
            log_level=data.get("log_level", "INFO"),
            metrics_enabled=data.get("metrics_enabled", True),
            max_parallel_requests=data.get("max_parallel_requests", 5),
            buffer_percent=safety.get(
                "buffer_percent", data.get("buffer_percent", 105)
            ),
            # From safety section (fallback to top-level for compat)
            max_expand_ratio=safety.get(
                "max_expand_ratio", data.get("max_expand_ratio", 2.0)
            ),
            max_shrink_ratio=safety.get(
                "max_shrink_ratio", data.get("max_shrink_ratio", 0.5)
            ),
            max_single_change_gb=safety.get(
                "max_single_change_gb", data.get("max_single_change_gb", 1000)
            ),
            min_change_threshold_gb=safety.get(
                "min_change_threshold_gb", data.get("min_change_threshold_gb", 10)
            ),
            max_qps=data.get("max_qps", 10),
            # From retry section (with fallback to top-level for backward compatibility)
            retry_max_attempts=retry.get(
                "max_attempts", data.get("retry_max_attempts", 3)
            ),
            retry_backoff_base=retry.get(
                "backoff_base", data.get("retry_backoff_base", 1.0)
            ),
            retry_backoff_max=retry.get(
                "backoff_max", data.get("retry_backoff_max", 30.0)
            ),
            cluster_whitelist=data.get("cluster_whitelist") or [],
            cluster_blacklist=data.get("cluster_blacklist") or [],
            cluster_tag_filters=data.get("cluster_tag_filters") or {},
            api_connect_timeout=data.get("api_connect_timeout", 5),
            api_read_timeout=data.get("api_read_timeout", 30),
        )

    def validate(self) -> list[str]:
        """
        Validate configuration values.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        # Required fields
        if not self.regions:
            errors.append("regions is required and must not be empty")

        if not self.run_mode:
            errors.append("run_mode is required")
        elif self.run_mode not in ("dry-run", "apply"):
            errors.append(
                f"run_mode must be 'dry-run' or 'apply', got '{self.run_mode}'"
            )

        # Validate log level
        valid_log_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        if self.log_level not in valid_log_levels:
            errors.append(
                f"log_level must be one of {valid_log_levels}, got '{self.log_level}'"
            )

        # Safety thresholds
        if self.max_expand_ratio <= 1.0:
            errors.append(
                f"max_expand_ratio must be > 1.0, got {self.max_expand_ratio}"
            )

        if not (0 < self.max_shrink_ratio <= 1.0):
            errors.append(
                f"max_shrink_ratio must be between 0 and 1, got {self.max_shrink_ratio}"
            )

        if self.max_single_change_gb <= 0:
            errors.append(
                f"max_single_change_gb must be > 0, got {self.max_single_change_gb}"
            )

        if self.min_change_threshold_gb < 0:
            errors.append(
                f"min_change_threshold_gb must be >= 0, "
                f"got {self.min_change_threshold_gb}"
            )

        # Validate buffer_percent
        if not isinstance(self.buffer_percent, int):
            errors.append(
                f"buffer_percent must be an integer, "
                f"got {type(self.buffer_percent).__name__}"
            )

        if self.buffer_percent <= 100:
            errors.append(f"buffer_percent must be > 100, got {self.buffer_percent}")

        if self.buffer_percent > 300:
            errors.append(
                f"buffer_percent ({self.buffer_percent}) exceeds maximum "
                f"reasonable value (300); check for configuration typo"
            )

        # Retry settings
        if self.retry_max_attempts <= 0:
            errors.append(
                f"retry_max_attempts must be > 0, got {self.retry_max_attempts}"
            )

        if self.retry_backoff_base <= 0:
            errors.append(
                f"retry_backoff_base must be > 0, got {self.retry_backoff_base}"
            )

        if self.retry_backoff_max <= 0:
            errors.append(
                f"retry_backoff_max must be > 0, got {self.retry_backoff_max}"
            )

        if self.retry_backoff_max < self.retry_backoff_base:
            errors.append(
                f"retry_backoff_max ({self.retry_backoff_max}) must be >= "
                f"retry_backoff_base ({self.retry_backoff_base})"
            )

        # Concurrency settings
        if self.max_parallel_requests <= 0:
            errors.append(
                f"max_parallel_requests must be > 0, got {self.max_parallel_requests}"
            )

        if self.max_qps <= 0:
            errors.append(f"max_qps must be > 0, got {self.max_qps}")

        if self.max_parallel_requests > self.max_qps:
            errors.append(
                f"max_parallel_requests ({self.max_parallel_requests}) "
                f"should be <= max_qps ({self.max_qps}) "
                f"to avoid burst rate-limit violations"
            )

        # API timeout settings
        if self.api_connect_timeout <= 0:
            errors.append(
                f"api_connect_timeout must be > 0, got {self.api_connect_timeout}"
            )

        if self.api_read_timeout <= 0:
            errors.append(f"api_read_timeout must be > 0, got {self.api_read_timeout}")

        # Cross-field validation
        # min_change_threshold_gb >= max_single_change_gb means no change can pass
        if (
            self.min_change_threshold_gb > 0
            and self.min_change_threshold_gb >= self.max_single_change_gb
        ):
            errors.append(
                f"min_change_threshold_gb ({self.min_change_threshold_gb}) "
                f"should be < max_single_change_gb ({self.max_single_change_gb}); "
                f"otherwise no change can satisfy both constraints"
            )

        # max_single_change_gb below STORAGE_STEP_GB prevents all step-aligned changes
        if self.max_single_change_gb < 10:
            errors.append(
                f"max_single_change_gb ({self.max_single_change_gb}) is below "
                f"storage step size (10GB); all changes will be capped to zero "
                f"effective change after step alignment"
            )

        if self.max_single_change_gb % 10 != 0:
            errors.append(
                f"max_single_change_gb ({self.max_single_change_gb}) should be "
                f"a multiple of storage step size (10GB) to avoid ambiguity "
                f"after step alignment"
            )

        if self.min_change_threshold_gb > 0 and self.min_change_threshold_gb % 10 != 0:
            errors.append(
                f"min_change_threshold_gb ({self.min_change_threshold_gb}) should be "
                f"a multiple of storage step size (10GB) to avoid ambiguity "
                f"after step alignment"
            )

        return errors

    def validate_rrsa(self) -> tuple[bool, str]:
        """
        Validate credentials for apply mode.

        In apply mode, one of the following credential types must be present:
        - RRSA: ALIBABA_CLOUD_ROLE_ARN or ALIBABA_CLOUD_ECI_ROLE_ARN
          (for K8s production)
        - AccessKey: ALIBABA_CLOUD_ACCESS_KEY_ID +
          ALIBABA_CLOUD_ACCESS_KEY_SECRET (for local testing)

        In dry-run mode, credentials are optional.

        Returns:
            Tuple of (is_valid, error_message)
        """
        role_arn = os.environ.get("ALIBABA_CLOUD_ROLE_ARN")
        eci_role_arn = os.environ.get("ALIBABA_CLOUD_ECI_ROLE_ARN")
        access_key_id = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID")
        access_key_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

        has_rrsa = bool(role_arn or eci_role_arn)
        has_access_key = bool(access_key_id and access_key_secret)

        if self.run_mode == "apply":
            if not has_rrsa and not has_access_key:
                return (
                    False,
                    "Credentials required in apply mode. "
                    "Set RRSA (ALIBABA_CLOUD_ROLE_ARN) for production, "
                    "or AccessKey (ALIBABA_CLOUD_ACCESS_KEY_ID + "
                    "ALIBABA_CLOUD_ACCESS_KEY_SECRET) for local testing.",
                )
            return (True, "")
        else:
            # dry-run mode: credentials are optional
            return (True, "")

    def raise_if_invalid(self) -> None:
        """
        Validate configuration and raise ValidationError if invalid.

        Raises:
            ValidationError: If configuration validation fails
        """
        errors = self.validate()
        if errors:
            raise ValidationError(
                message="Configuration validation failed",
                field="config",
                errors=errors,
            )

        is_valid, error_message = self.validate_rrsa()
        if not is_valid:
            raise ValidationError(
                message=error_message,
                field="rrsa",
            )
