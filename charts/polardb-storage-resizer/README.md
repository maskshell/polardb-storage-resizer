# PolarDB Storage Resizer Helm Chart

Automatically adjusts PolarDB prepaid storage size based on actual usage.

## Prerequisites

- Kubernetes 1.27+ (CronJob timeZone field requires 1.27+)
- Helm 3.0+
- ACK cluster with RRSA enabled
- RAM Role with PolarDB permissions

## Installation

### Quick Start (Dry-run mode)

```bash
# Install in dry-run mode (no actual changes)
helm install polardb-resizer ./charts/polardb-storage-resizer \
  --namespace dba \
  --create-namespace \
  --set config.regions="cn-hangzhou\,cn-beijing" \
  --set rrsa.roleName="PolardbStorageResizerRole"
```

### Production Installation (Apply mode)

```bash
# Copy the example file and customize
cp values-prod.example.yaml my-values.yaml

# Edit my-values.yaml — IMPORTANT: set config.runMode to "apply"
# Install with production values
helm install polardb-resizer ./charts/polardb-storage-resizer \
  -f my-values.yaml \
  --namespace dba \
  --create-namespace
```

## Configuration

### Key Configuration Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| `config.runMode` | Run mode: `dry-run` or `apply` | `dry-run` |
| `config.regions` | Target regions (comma-separated) | `cn-hangzhou` |
| `config.logLevel` | Log level | `INFO` |
| `config.bufferPercent` | Storage buffer percentage (must be > 100) | `105` |
| `config.clusterWhitelist` | Cluster whitelist (optional) | `""` |
| `config.clusterBlacklist` | Cluster blacklist (optional, higher priority) | `""` |
| `config.clusterTagFilters` | Cluster tag filters (`key1:value1,key2:value2`) | `""` |
| `config.apiConnectTimeout` | API connection timeout (seconds) | `5` |
| `config.apiReadTimeout` | API read timeout (seconds) | `30` |
| `cronjob.schedule` | Cron schedule | `0 2 * * *` |
| `rrsa.enabled` | Enable RRSA authentication | `true` |
| `rrsa.roleName` | RAM Role name (NOT full ARN) | `""` |
| `image.repository` | Container image repository | Required |
| `image.tag` | Container image tag | Chart appVersion |

### RRSA Configuration

```yaml
rrsa:
  enabled: true
  # Use just the role name, NOT the full ARN
  roleName: "PolardbStorageResizerRole"
```

## RAM Role Setup

### 1. Create RAM Role with OIDC Trust Policy

```json
{
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "oidc:aud": [
            "sts.aliyuncs.com"
          ],
          "oidc:iss": [
            "https://oidc-ack-cn-hangzhou.oss-cn-hangzhou.aliyuncs.com/<alibaba-cloud-k8s-cluster-id>"
          ],
          "oidc:sub": [
            "system:serviceaccount:<namespace>:<helm-release-account-name>"
          ]
        }
      },
      "Effect": "Allow",
      "Principal": {
        "Federated": [
          "acs:ram::<alibaba-cloud-account-id>:oidc-provider/ack-rrsa-<alibaba-cloud-k8s-cluster-id>"
        ]
      }
    }
  ],
  "Version": "1"
}
```

### 2. Attach Permission Policy

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "polardb:DescribeDBClusters",
        "polardb:DescribeDBClusterAttribute"
      ],
      "Resource": ["*"]
    },
    {
      "Effect": "Allow",
      "Action": "polardb:ModifyDBClusterStorageSpace",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "acs:ResourceTag/auto-resize": ["on"]
        }
      }
    }
  ]
}
```

## Useful Commands

```bash
# Check CronJob status
kubectl get cronjob -n dba

# Manual run
kubectl create job --from=cronjob/<release>-polardb-storage-resizer \
  -n dba manual-run-$(date +%s)

# View logs
kubectl logs -n dba -l app.kubernetes.io/name=polardb-storage-resizer --tail=100

# Upgrade
helm upgrade polardb-resizer ./charts/polardb-storage-resizer -n dba

# Uninstall
helm uninstall polardb-resizer -n dba
```

## Reference

- [RRSA Documentation](https://help.aliyun.com/zh/ack/serverless-kubernetes/user-guide/use-rrsa-to-authorize-pods-to-access-different-cloud-services)
