"""
Alibaba Cloud PolarDB client implementation.

This module provides the production implementation of PolarDBClient
using the Aliyun SDK for Python.

Supported authentication methods:
1. AccessKey (ALIBABA_CLOUD_ACCESS_KEY_ID + ALIBABA_CLOUD_ACCESS_KEY_SECRET)
2. RSSA/OIDC (automatic in ACK clusters with RRSA enabled)
3. RAM Role (ALIBABA_CLOUD_ROLE_ARN for ECS/container environments)
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from polardb_storage_resizer.cloud_client import (
    classify_sdk_error,
)
from polardb_storage_resizer.errors import (
    PermanentCloudAPIError,
    TransientCloudAPIError,
)
from polardb_storage_resizer.models import ClusterDetail, ClusterSummary, ModifyResult
from polardb_storage_resizer.redaction import redact_request_id

if TYPE_CHECKING:
    from alibabacloud_polardb20170801.client import Client as PolarDBSDKClient
    from Tea.core import TeaResponse


class AliyunPolarDBClient:
    """
    Production PolarDB client using Aliyun SDK.

    This client supports multiple authentication methods:
    - AccessKey: Set ALIBABA_CLOUD_ACCESS_KEY_ID and ALIBABA_CLOUD_ACCESS_KEY_SECRET
    - RSSA: Automatic when running in ACK with RRSA enabled
    - Role ARN: Set ALIBABA_CLOUD_ROLE_ARN for role assumption

    The client automatically detects and uses the appropriate credential provider.
    """

    def __init__(
        self,
        connect_timeout: int = 5,
        read_timeout: int = 30,
    ) -> None:
        """Initialize the Aliyun PolarDB client with auto-detected credentials.

        Args:
            connect_timeout: Connection timeout in seconds (default 5).
            read_timeout: Read timeout in seconds (default 30).
        """
        from alibabacloud_credentials.client import Client as CredClient

        # Create credentials client (auto-detects from environment)
        # Priority: AccessKey > RSSA/OIDC > RAM Role > ECS Role
        cred_client = CredClient()

        self._cred_client = cred_client
        # SDK expects timeout in milliseconds
        self._connect_timeout = connect_timeout * 1000
        self._read_timeout = read_timeout * 1000

        # Client cache by region (thread-safe via lock)
        self._clients: dict[str, PolarDBSDKClient] = {}
        self._clients_lock = threading.Lock()

    def _get_client(self, region: str) -> PolarDBSDKClient:
        """
        Get or create a PolarDB client for the specified region.

        Args:
            region: Region ID (e.g., cn-hangzhou)

        Returns:
            PolarDB client instance for the region
        """
        with self._clients_lock:
            if region not in self._clients:
                from alibabacloud_polardb20170801.client import (  # noqa: E402
                    Client as PolarDBSDKClient,
                )
                from alibabacloud_tea_openapi.models import Config  # noqa: E402

                config = Config(
                    credential=self._cred_client,
                    region_id=region,
                    # Use HTTPS for security
                    protocol="https",
                    connect_timeout=self._connect_timeout,
                    read_timeout=self._read_timeout,
                )
                self._clients[region] = PolarDBSDKClient(config)

        return self._clients[region]

    def list_clusters(
        self,
        region: str,
        cluster_ids: list[str] | None = None,
        tag_filters: dict[str, str] | None = None,
    ) -> list[ClusterSummary]:
        """
        List all clusters in a region.

        Handles pagination automatically: the API returns max 100 clusters per
        page, so multiple requests are made when a region has more than 100
        clusters.

        Args:
            region: Region to list clusters from
            cluster_ids: Optional list of cluster IDs to filter at API level
            tag_filters: Optional dict of tag key-value pairs to filter clusters.
                         Example: {"Environment": "production", "Team": "backend"}

        Returns:
            List of cluster summaries

        Raises:
            CloudAPIError: If the API call fails
        """
        import logging

        from alibabacloud_polardb20170801 import models as polardb_models

        logger = logging.getLogger(__name__)
        client = self._get_client(region)

        # Convert tag_filters dict to API Tag format
        # API expects: Tag=[Tag(key="Environment", value="production"), ...]
        tag_list = None
        if tag_filters:
            tag_list = [
                polardb_models.DescribeDBClustersRequestTag(key=k, value=v)
                for k, v in tag_filters.items()
            ]

        # Cluster ID filter string (for whitelist optimization)
        cluster_ids_str = ",".join(cluster_ids) if cluster_ids else None

        page_number = 1
        page_size = 100
        summaries: list[ClusterSummary] = []
        total_count: int | None = None

        while True:
            request = polardb_models.DescribeDBClustersRequest(
                region_id=region,
                dbtype="MySQL",  # Only MySQL clusters are supported
                page_number=page_number,
                page_size=page_size,
                tag=tag_list,
            )

            if cluster_ids_str:
                request.dbcluster_ids = cluster_ids_str

            try:
                response = client.describe_dbclusters(request)
                self._check_response(response)

                if response.body and response.body.items:
                    clusters = response.body.items.dbcluster
                    if clusters:
                        for item in clusters:
                            summaries.append(
                                ClusterSummary(
                                    cluster_id=item.dbcluster_id,
                                    region=region,
                                    cluster_name=item.dbcluster_description
                                    or item.dbcluster_id,
                                    status=item.dbcluster_status,
                                    pay_type=item.pay_type,
                                )
                            )

                # Check total count to determine if more pages exist
                if (
                    response.body
                    and hasattr(response.body, "total_record_count")
                    and response.body.total_record_count
                ):
                    total_count = response.body.total_record_count
                    fetched = page_number * page_size
                    if fetched < total_count:
                        page_number += 1
                        continue

                break

            except (TransientCloudAPIError, PermanentCloudAPIError):
                raise
            except Exception as e:
                raise self._handle_error(e) from e

        if total_count and total_count > page_size:
            logger.info(
                "Region %s: fetched all %d clusters across %d page(s)",
                region,
                total_count,
                page_number,
            )

        return summaries

    def get_cluster_detail(self, region: str, cluster_id: str) -> ClusterDetail:
        """
        Get detailed information for a cluster.

        Args:
            region: Region where the cluster is located
            cluster_id: Cluster identifier

        Returns:
            Detailed cluster information

        Raises:
            CloudAPIError: If the API call fails
        """
        from alibabacloud_polardb20170801 import models as polardb_models

        client = self._get_client(region)
        request = polardb_models.DescribeDBClusterAttributeRequest(
            dbcluster_id=cluster_id,
        )

        try:
            response = client.describe_dbcluster_attribute(request)
            self._check_response(response)

            if not response.body:
                raise PermanentCloudAPIError(
                    message=f"Empty response for cluster {cluster_id}",
                    error_code="EmptyResponse",
                )

            item = response.body

            # Get compression mode and storage values
            # Reference: https://help.aliyun.com/zh/polardb/api-polardb-2017-08-01-describedbclusterattribute
            compress_mode = getattr(item, "compress_storage_mode", None)

            # Determine used storage based on compression mode
            # Compression ON: use CompressStorageUsed (billing size)
            # - When compression is OFF or not set: use StorageUsed
            used_storage_gb = 0.0
            raw_used_storage_gb: float | None = None

            if compress_mode == "ON":
                # Use compressed storage for billing-based calculations
                if (
                    hasattr(item, "compress_storage_used")
                    and item.compress_storage_used
                ):
                    used_storage_gb = item.compress_storage_used / (1024**3)

                # Also get raw (uncompressed) size for reference
                if hasattr(item, "storage_used") and item.storage_used:
                    raw_used_storage_gb = item.storage_used / (1024**3)
            else:
                # No compression: use standard used storage
                if hasattr(item, "storage_used") and item.storage_used:
                    used_storage_gb = item.storage_used / (1024**3)

            # Get provisioned storage (storage_space is in bytes, convert to GB)
            if hasattr(item, "storage_space") and item.storage_space:
                provisioned_storage_gb = int(item.storage_space / (1024**3))
            else:
                provisioned_storage_gb = 0

            # Get actual storage engine type (e.g., "PSSD", "PL0", "PL1", etc.)
            # for min-storage enforcement in strategy layer.
            # Fall back to pay_type-derived value only if API field is absent.
            storage_type = getattr(item, "storage_type", None)
            if not storage_type:
                storage_type = (
                    "PrepaidStorage"
                    if item.pay_type == "Prepaid"
                    else "PostpaidStorage"
                )

            return ClusterDetail(
                cluster_id=cluster_id,
                region=region,
                cluster_name=item.dbcluster_description or cluster_id,
                status=item.dbcluster_status,
                pay_type=item.pay_type,
                storage_type=storage_type,
                used_storage_gb=used_storage_gb,
                provisioned_storage_gb=provisioned_storage_gb,
                compress_storage_mode=compress_mode,
                raw_used_storage_gb=raw_used_storage_gb,
                create_time=item.creation_time,
                last_modify_time=None,  # API doesn't return this directly
            )

        except (TransientCloudAPIError, PermanentCloudAPIError):
            raise
        except Exception as e:
            raise self._handle_error(e) from e

    def modify_storage(
        self, region: str, cluster_id: str, new_size_gb: int
    ) -> ModifyResult:
        """
        Modify storage for a cluster.

        Args:
            region: Region where the cluster is located
            cluster_id: Cluster identifier
            new_size_gb: New storage size in GB

        Returns:
            Result of the modification operation

        Raises:
            CloudAPIError: If the API call fails
        """
        import hashlib

        from alibabacloud_polardb20170801 import models as polardb_models

        client = self._get_client(region)

        # Generate ClientToken for idempotency
        # Format: MD5 hash of "region:cluster_id:new_size_gb" (first 32 chars)
        # This ensures the same modification request is idempotent within 24 hours
        token_source = f"{region}:{cluster_id}:{new_size_gb}"
        client_token = hashlib.md5(token_source.encode()).hexdigest()  # noqa: S324

        request = polardb_models.ModifyDBClusterStorageSpaceRequest(
            dbcluster_id=cluster_id,
            storage_space=new_size_gb,
            client_token=client_token,
        )

        try:
            response = client.modify_dbcluster_storage_space(request)
            self._check_response(response)

            request_id = ""
            if response.body and hasattr(response.body, "request_id"):
                request_id = response.body.request_id or ""
            redacted_request_id = redact_request_id(request_id)

            return ModifyResult(
                success=True,
                cluster_id=cluster_id,
                old_storage_gb=0,  # API doesn't return old value
                new_storage_gb=new_size_gb,
                request_id=redacted_request_id,
            )

        except PermanentCloudAPIError:
            raise
        except TransientCloudAPIError:
            raise
        except Exception as e:
            raise self._handle_error(e) from e

    def _check_response(self, response: TeaResponse) -> None:
        """
        Check if the API response indicates success.

        Args:
            response: SDK response object

        Raises:
            CloudAPIError: If the response indicates failure
        """
        if response.status_code is None or response.status_code >= 400:
            error_code = "HTTPError"
            if (
                hasattr(response, "body")
                and response.body
                and hasattr(response.body, "code")
            ):
                error_code = response.body.code

            status = response.status_code or 0
            message = f"API request failed with status {status}"

            # HTTP 429 Too Many Requests is transient (rate limited)
            if status == 429:
                raise TransientCloudAPIError(
                    message=message,
                    error_code=error_code,
                )

            # 5xx server errors are transient (retryable)
            if status >= 500:
                raise TransientCloudAPIError(
                    message=message,
                    error_code=error_code,
                )

            raise PermanentCloudAPIError(
                message=message,
                error_code=error_code,
            )

    def _handle_error(
        self, error: Exception
    ) -> TransientCloudAPIError | PermanentCloudAPIError:
        """
        Convert SDK error to CloudAPIError.

        Args:
            error: Original exception

        Returns:
            TransientCloudAPIError or PermanentCloudAPIError
        """
        return classify_sdk_error(error)
