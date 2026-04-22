"""
Shared pytest fixtures for PolarDB Storage Resizer tests.

This module provides:
- Fake PolarDB client factory for testing
- Standard test configuration fixtures
- Sample cluster data loaders
- Environment isolation
- Logging capture utilities
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

# Import models from actual modules (not duplicated here)
from polardb_storage_resizer.config import AppConfig
from polardb_storage_resizer.models import (
    ClusterDetail,
)

if TYPE_CHECKING:
    from collections.abc import Generator


# Re-export FakePolarDBClient from production package for test convenience
from polardb_storage_resizer.fake_client import FakePolarDBClient  # noqa: F401

# ==============================================================================
# Fixtures Directory Path
# ==============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ==============================================================================
# Cluster Data Loaders
# ==============================================================================


def load_sample_clusters() -> list[dict[str, Any]]:
    """
    Load sample cluster data from JSON fixture file.

    Returns:
        List of cluster data dictionaries
    """
    fixture_path = FIXTURES_DIR / "sample_clusters.json"
    with fixture_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_sample_config() -> dict[str, Any]:
    """
    Load sample configuration from YAML fixture file.

    Returns:
        Configuration dictionary
    """
    import yaml  # type: ignore[import-untyped]

    fixture_path = FIXTURES_DIR / "sample_config.yaml"
    with fixture_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_invalid_config() -> dict[str, Any]:
    """
    Load invalid configuration from YAML fixture file.

    Returns:
        Invalid configuration dictionary
    """
    import yaml  # type: ignore[import-untyped]

    fixture_path = FIXTURES_DIR / "invalid_config.yaml"
    with fixture_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def cluster_data_to_detail(cluster_data: dict[str, Any]) -> ClusterDetail:
    """
    Convert cluster data dictionary to ClusterDetail object.

    Args:
        cluster_data: Dictionary with cluster data

    Returns:
        ClusterDetail object
    """
    return ClusterDetail(
        cluster_id=cluster_data["cluster_id"],
        region=cluster_data["region"],
        cluster_name=cluster_data["cluster_name"],
        status=cluster_data["status"],
        pay_type=cluster_data["pay_type"],
        storage_type=cluster_data["storage_type"],
        used_storage_gb=cluster_data["used_storage_gb"],
        provisioned_storage_gb=cluster_data["provisioned_storage_gb"],
        category=cluster_data.get("category"),
        serverless_type=cluster_data.get("serverless_type"),
        compress_storage_mode=cluster_data.get("compress_storage_mode"),
        raw_used_storage_gb=cluster_data.get("raw_used_storage_gb"),
        create_time=cluster_data.get("create_time"),
        last_modify_time=cluster_data.get("last_modify_time"),
    )


# ==============================================================================
# Environment Isolation
# ==============================================================================


@pytest.fixture
def isolated_env() -> Generator[dict[str, str], None, None]:
    """
    Create an isolated environment for testing.

    This fixture:
    1. Saves current environment variables
    2. Provides a clean environment
    3. Restores original environment after test

    Yields:
        Dictionary of original environment variables
    """
    # Save original environment
    original_env = dict(os.environ)

    # Clear PolarDB-related environment variables
    env_keys_to_clear = [
        "RUN_MODE",
        "REGIONS",
        "LOG_LEVEL",
        "METRICS_ENABLED",
        "MAX_PARALLEL_REQUESTS",
        "MAX_QPS",
        "ALIBABA_CLOUD_ROLE_ARN",
        "ALIBABA_CLOUD_ECI_ROLE_ARN",
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "ALIBABA_CLOUD_SECURITY_TOKEN",
    ]

    for key in env_keys_to_clear:
        os.environ.pop(key, None)

    yield original_env

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


# ==============================================================================
# Client Fixtures
# ==============================================================================


@pytest.fixture
def fake_client() -> FakePolarDBClient:
    """
    Create a basic fake PolarDB client with sample data.

    Returns:
        FakePolarDBClient instance with sample clusters
    """
    clusters_data = load_sample_clusters()
    clusters = [cluster_data_to_detail(c) for c in clusters_data]
    return FakePolarDBClient(clusters=clusters)


@pytest.fixture
def fake_client_factory() -> type[FakePolarDBClient]:
    """
    Provide the FakePolarDBClient class for custom instantiation.

    Returns:
        FakePolarDBClient class
    """
    return FakePolarDBClient


@pytest.fixture
def empty_fake_client() -> FakePolarDBClient:
    """
    Create an empty fake PolarDB client.

    Returns:
        FakePolarDBClient instance with no clusters
    """
    return FakePolarDBClient(clusters=[])


@pytest.fixture
def failing_fake_client() -> FakePolarDBClient:
    """
    Create a fake PolarDB client that always fails modify operations.

    Returns:
        FakePolarDBClient instance configured to fail
    """
    clusters_data = load_sample_clusters()
    clusters = [cluster_data_to_detail(c) for c in clusters_data]
    return FakePolarDBClient(clusters=clusters, modify_should_fail=True)


@pytest.fixture
def transient_failing_fake_client() -> FakePolarDBClient:
    """
    Create a fake PolarDB client with transient failures.

    Returns:
        FakePolarDBClient instance configured with transient failures
    """
    clusters_data = load_sample_clusters()
    clusters = [cluster_data_to_detail(c) for c in clusters_data]
    return FakePolarDBClient(clusters=clusters, transient_fail_count=2)


# ==============================================================================
# Configuration Fixtures
# ==============================================================================


@pytest.fixture
def sample_config() -> AppConfig:
    """
    Create a standard test configuration.

    Returns:
        AppConfig instance with standard test values
    """
    return AppConfig(
        run_mode="dry-run",
        regions=["cn-hangzhou", "cn-beijing"],
        log_level="DEBUG",
        metrics_enabled=True,
        max_parallel_requests=3,
        max_qps=5,
        max_expand_ratio=2.0,
        max_shrink_ratio=0.5,
        max_single_change_gb=500,
        min_change_threshold_gb=10,
    )


@pytest.fixture
def apply_mode_config() -> AppConfig:
    """
    Create a configuration in apply mode.

    Returns:
        AppConfig instance configured for apply mode
    """
    return AppConfig(
        run_mode="apply",
        regions=["cn-hangzhou"],
        log_level="INFO",
        metrics_enabled=True,
        max_parallel_requests=5,
        max_qps=10,
    )


@pytest.fixture
def strict_safety_config() -> AppConfig:
    """
    Create a configuration with strict safety thresholds.

    Returns:
        AppConfig instance with strict safety settings
    """
    return AppConfig(
        run_mode="dry-run",
        regions=["cn-hangzhou"],
        max_expand_ratio=1.2,  # Max 20% expansion
        max_shrink_ratio=0.8,  # Max 20% shrinkage
        max_single_change_gb=100,  # Small changes only
        min_change_threshold_gb=20,  # Ignore small changes
    )


# ==============================================================================
# Sample Data Fixtures
# ==============================================================================


@pytest.fixture
def sample_clusters() -> list[ClusterDetail]:
    """
    Load sample cluster data as ClusterDetail objects.

    Returns:
        List of ClusterDetail objects
    """
    clusters_data = load_sample_clusters()
    return [cluster_data_to_detail(c) for c in clusters_data]


@pytest.fixture
def sample_clusters_raw() -> list[dict[str, Any]]:
    """
    Load raw sample cluster data as dictionaries.

    Returns:
        List of cluster data dictionaries
    """
    return load_sample_clusters()


@pytest.fixture
def prepaid_running_clusters(
    sample_clusters: list[ClusterDetail],
) -> list[ClusterDetail]:
    """
    Filter sample clusters to only include prepaid and running clusters.

    Args:
        sample_clusters: List of all sample clusters

    Returns:
        List of prepaid, running clusters
    """
    return [
        c for c in sample_clusters if c.pay_type == "Prepaid" and c.status == "Running"
    ]


# ==============================================================================
# Boundary Case Fixtures
# ==============================================================================


@pytest.fixture
def empty_cluster() -> ClusterDetail:
    """
    Create a cluster with zero used storage (A=0).

    This represents an empty cluster scenario where
    the target should be minimum storage value.

    Returns:
        ClusterDetail with used_storage_gb = 0
    """
    return ClusterDetail(
        cluster_id="pc-empty-00000000",
        region="cn-hangzhou",
        cluster_name="empty-cluster",
        status="Running",
        pay_type="Prepaid",
        storage_type="PrepaidStorage",
        used_storage_gb=0,
        provisioned_storage_gb=100,
    )


@pytest.fixture
def full_usage_cluster() -> ClusterDetail:
    """
    Create a cluster where used equals provisioned (A=B).

    This represents a cluster at full capacity where
    target should expand by buffer percent.

    Returns:
        ClusterDetail with used_storage_gb = provisioned_storage_gb
    """
    return ClusterDetail(
        cluster_id="pc-full-11111111",
        region="cn-hangzhou",
        cluster_name="full-usage-cluster",
        status="Running",
        pay_type="Prepaid",
        storage_type="PrepaidStorage",
        used_storage_gb=500,
        provisioned_storage_gb=500,
    )


@pytest.fixture
def overage_cluster() -> ClusterDetail:
    """
    Create a cluster where used exceeds provisioned (A>B).

    This represents a cluster with overage billing where
    target should expand to cover actual usage plus buffer.

    Returns:
        ClusterDetail with used_storage_gb > provisioned_storage_gb
    """
    return ClusterDetail(
        cluster_id="pc-over-22222222",
        region="cn-hangzhou",
        cluster_name="overage-cluster",
        status="Running",
        pay_type="Prepaid",
        storage_type="PrepaidStorage",
        used_storage_gb=650,
        provisioned_storage_gb=500,
    )


@pytest.fixture
def underutilized_cluster() -> ClusterDetail:
    """
    Create a cluster with significant unused storage (A << B).

    This represents a cluster with wasteful provisioning
    where target should shrink significantly.

    Returns:
        ClusterDetail with used_storage_gb << provisioned_storage_gb
    """
    return ClusterDetail(
        cluster_id="pc-under-33333333",
        region="cn-hangzhou",
        cluster_name="underutilized-cluster",
        status="Running",
        pay_type="Prepaid",
        storage_type="PrepaidStorage",
        used_storage_gb=50,
        provisioned_storage_gb=1000,
    )


@pytest.fixture
def postpaid_cluster() -> ClusterDetail:
    """
    Create a postpaid cluster (should be filtered out).

    This cluster should not be processed by the resizer
    as it's not on prepaid storage.

    Returns:
        ClusterDetail with pay_type = "Postpaid"
    """
    return ClusterDetail(
        cluster_id="pc-post-55555555",
        region="cn-hangzhou",
        cluster_name="postpaid-cluster",
        status="Running",
        pay_type="Postpaid",
        storage_type="PostpaidStorage",
        used_storage_gb=200,
        provisioned_storage_gb=200,
    )


@pytest.fixture
def non_running_cluster() -> ClusterDetail:
    """
    Create a non-running cluster (should be filtered out).

    This cluster should not be processed by the resizer
    as it's not in Running status.

    Returns:
        ClusterDetail with status != "Running"
    """
    return ClusterDetail(
        cluster_id="pc-stop-66666666",
        region="cn-hangzhou",
        cluster_name="stopped-cluster",
        status="Stopped",
        pay_type="Prepaid",
        storage_type="PrepaidStorage",
        used_storage_gb=100,
        provisioned_storage_gb=200,
    )


# ==============================================================================
# Logging Capture
# ==============================================================================


@pytest.fixture
def log_capture() -> Generator[list[logging.LogRecord], None, None]:
    """
    Capture log records during test execution.

    Yields:
        List to collect log records
    """
    records: list[logging.LogRecord] = []

    class RecordHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = RecordHandler()
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)

    yield records

    root_logger.removeHandler(handler)


# ==============================================================================
# Utility Fixtures
# ==============================================================================


@pytest.fixture
def shutdown_event() -> Generator[threading.Event, None, None]:
    """
    Create a shutdown event for testing graceful shutdown.

    Yields:
        threading.Event instance
    """
    event = threading.Event()
    yield event


# ==============================================================================
# Pytest Configuration
# ==============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Configure custom pytest markers."""
    config.addinivalue_line("markers", "unit: Unit tests (fast, isolated)")
    config.addinivalue_line(
        "markers", "integration: Integration tests (may use external resources)"
    )
    config.addinivalue_line("markers", "slow: Slow tests (skip in quick test runs)")
    config.addinivalue_line(
        "markers", "requires_rrsa: Tests that require RRSA credentials"
    )
