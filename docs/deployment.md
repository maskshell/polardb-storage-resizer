# Kubernetes 部署指南

本文档详细说明如何在 Kubernetes 集群中部署 PolarDB Storage Resizer 服务。

## 目录

- [前置条件](#前置条件)
- [快速部署](#快速部署)
- [RSSA/RRSA 配置](#rssarrsa-配置)
- [配置说明](#配置说明)
- [监控与日志](#监控与日志)
- [故障排查](#故障排查)
- [安全最佳实践](#安全最佳实践)

## 前置条件

### 1. Kubernetes 集群要求

- Kubernetes 版本 >= 1.27 (CronJob timeZone 字段需要 1.27+)
- ACK 集群需启用 RRSA 功能
- kubectl 已配置并可访问集群

### 2. 阿里云资源要求

- 已开通 PolarDB 服务
- 已创建 RAM 角色并配置 OIDC 信任策略
- RAM 角色具有 PolarDB 相关权限

### 3. 容器镜像

需要先构建并推送镜像到容器镜像仓库：

```bash
# 构建镜像
docker build -t ghcr.io/maskshell/polardb-storage-resizer:v1.0.0 .

# 推送镜像
docker push ghcr.io/maskshell/polardb-storage-resizer:v1.0.0
```

## 快速部署

### 步骤 1：创建命名空间

```bash
kubectl create namespace dba
```

### 步骤 2：配置 RSSA

在部署 ServiceAccount 之前，需要先修改 `k8s/serviceaccount-rssa.yaml` 中的角色名称：

```yaml
annotations:
  # 只需要角色名称，不需要完整 ARN
  pod-identity.alibabacloud.com/role-name: "PolardbStorageResizerRole"
```

然后部署 ServiceAccount：

```bash
kubectl apply -f k8s/serviceaccount-rssa.yaml -n dba
```

### 步骤 3：配置 CronJob

修改 `k8s/cronjob.yaml` 中的以下配置：

```yaml
# 1. 修改命名空间
namespace: dba

# 2. 修改镜像地址
image: ghcr.io/maskshell/polardb-storage-resizer:v1.0.0

# 3. 修改运行模式（生产环境改为 apply）
- name: RUN_MODE
  value: "apply"

# 4. 修改目标区域
- name: REGIONS
  value: "cn-hangzhou,cn-beijing"
```

部署 CronJob：

```bash
kubectl apply -f k8s/cronjob.yaml -n dba
```

### 步骤 4：验证部署

```bash
# 查看 CronJob 状态
kubectl get cronjob -n dba

# 查看 Job 历史
kubectl get jobs -n dba

# 手动触发一次执行（测试用）
kubectl create job --from=cronjob/polardb-storage-resizer test-run-$(date +%s) -n dba

# 查看 Pod 日志
kubectl logs -l app.kubernetes.io/name=polardb-storage-resizer -n dba
```

## RSSA/RRSA 配置

### 什么是 RSSA/RRSA？

RRSA (RAM Roles for Service Accounts) 是阿里云 ACK 提供的功能，允许 Pod 通过 OIDC 联邦获取临时凭证，无需在集群中存储 AccessKey/SecretKey。

### 配置步骤

#### 1. 创建 RAM 角色

在阿里云 RAM 控制台创建角色，信任策略如下：

```json
{
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "oidc:aud": "sts.amazonaws.com",
          "oidc:sub": "system:serviceaccount:dba:polardb-resizer-sa"
        }
      },
      "Effect": "Allow",
      "Principal": {
        "Federated": [
          "acs:ram::YOUR_ACCOUNT_ID:oidc-provider/ack-rrsa-cls-YOUR_CLUSTER_ID"
        ]
      }
    }
  ],
  "Version": "1"
}
```

**重要参数说明：**

| 参数 | 说明 |
| --- | --- |
| `oidc:sub` | 格式为 `system:serviceaccount:<namespace>:<serviceaccount-name>` |
| `Federated` | OIDC 提供者 ARN，可在 ACK 集群详情页获取 |

#### 2. 创建权限策略

创建自定义策略并附加到 RAM 角色：

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "polardb:DescribeDBClusters",
        "polardb:DescribeDBClusterAttribute",
        "polardb:ModifyDBClusterStorageSpace"
      ],
      "Resource": ["*"]
    }
  ]
}
```

**权限说明：**

| Action | 用途 |
| --- | --- |
| `polardb:DescribeDBClusters` | 列出区域内的 PolarDB 集群 |
| `polardb:DescribeDBClusterAttribute` | 获取集群详细信息（包括存储使用量） |
| `polardb:ModifyDBClusterStorageSpace` | 修改集群存储空间大小 |

#### 3. 配置 ServiceAccount

更新 `k8s/serviceaccount-rssa.yaml`：

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: polardb-resizer-sa
  namespace: dba
  annotations:
    # 只需要角色名称，不需要完整 ARN
    pod-identity.alibabacloud.com/role-name: "PolardbStorageResizerRole"
```

### Terraform 配置示例

```hcl
# 创建 RAM 角色
resource "alicloud_ram_role" "polardb_resizer" {
  name        = "PolardbStorageResizerRole"
  document    = jsonencode({
    Statement = [
      {
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "oidc:aud" = "sts.amazonaws.com"
            "oidc:sub" = "system:serviceaccount:dba:polardb-resizer-sa"
          }
        }
        Effect = "Allow"
        Principal = {
          Federated = [
            "acs:ram::${var.account_id}:oidc-provider/ack-rrsa-cls-${var.cluster_id}"
          ]
        }
      }
    ]
    Version = "1"
  })
}

# 创建权限策略
resource "alicloud_ram_policy" "polardb_resizer" {
  policy_name     = "PolardbStorageResizerPolicy"
  policy_document = jsonencode({
    Version = "1"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "polardb:DescribeDBClusters",
          "polardb:DescribeDBClusterAttribute",
          "polardb:ModifyDBClusterStorageSpace"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# 附加策略到角色
resource "alicloud_ram_role_policy_attachment" "attach" {
  policy_name = alicloud_ram_policy.polardb_resizer.policy_name
  policy_type = "Custom"
  role_name   = alicloud_ram_role.polardb_resizer.name
}
```

## 配置说明

### 环境变量配置

| 变量名 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `RUN_MODE` | 是 | `dry-run` | 运行模式 |
| `REGIONS` | 是 | - | 目标区域列表 |
| `LOG_LEVEL` | 否 | `INFO` | 日志级别 |
| `BUFFER_PERCENT` | 否 | `105` | 存储缓冲百分比（必须 > 100） |
| `MAX_PARALLEL_REQUESTS` | 否 | `5` | 最大并发请求数 |
| `MAX_QPS` | 否 | `10` | 每秒最大 API 请求数 |
| `MAX_EXPAND_RATIO` | 否 | `2.0` | 最大扩容比例（超出则限制到该值而非跳过） |
| `MAX_SHRINK_RATIO` | 否 | `0.5` | 最小缩容比例（缩容时目标不低于当前容量的此比例，超出则限制到该值而非跳过） |
| `MAX_SINGLE_CHANGE_GB` | 否 | `1000` | 单次最大变更量 |
| `MIN_CHANGE_THRESHOLD_GB` | 否 | `10` | 最小变更阈值（必须为 10GB 步长的倍数，[API 文档](https://api.aliyun.com/document/polardb/2017-08-01/ModifyDBClusterStorageSpace)） |
| `RETRY_MAX_ATTEMPTS` | 否 | `3` | 最大重试次数 |
| `RETRY_BACKOFF_BASE` | 否 | `1.0` | 重试退避基数 |
| `RETRY_BACKOFF_MAX` | 否 | `30.0` | 最大退避时间 |
| `CLUSTER_WHITELIST` | 否 | - | 集群白名单（仅处理列表中的集群） |
| `CLUSTER_BLACKLIST` | 否 | - | 集群黑名单（排除列表中的集群，优先级高于白名单） |
| `CLUSTER_TAG_FILTERS` | 否 | - | 集群标签筛选，格式为 `key1:value1,key2:value2`，仅处理具有所有指定标签的集群 |
| `API_CONNECT_TIMEOUT` | 否 | `5` | API 连接超时（秒） |
| `API_READ_TIMEOUT` | 否 | `30` | API 读取超时（秒） |
| `METRICS_ENABLED` | 否 | `true` | 是否输出结构化指标日志 |

### 调度配置

CronJob 默认每天 02:00 北京时间（Asia/Shanghai）执行：

```yaml
spec:
  schedule: "0 2 * * *"
  timeZone: "Asia/Shanghai"  # 默认值，需要 Kubernetes 1.27+
  concurrencyPolicy: Forbid  # 禁止并发执行
```

**修改时区：**

```yaml
# 使用 UTC 时区
timeZone: "UTC"

# 使用其他时区
timeZone: "America/New_York"
```

**修改执行时间：**

```yaml
# 每天 06:00 CST (北京时间)
schedule: "0 22 * * *"  # UTC 时间

# 每 6 小时执行一次
schedule: "0 */6 * * *"
```

### 资源配置

根据实际需求调整资源限制：

```yaml
resources:
  requests:
    cpu: "100m"
    memory: "128Mi"
  limits:
    cpu: "500m"
    memory: "512Mi"
```

## 监控与日志

### 日志查看

```bash
# 查看最近一次执行的日志
kubectl logs -l app.kubernetes.io/name=polardb-storage-resizer -n dba --tail=100

# 实时跟踪日志
kubectl logs -f -l app.kubernetes.io/name=polardb-storage-resizer -n dba

# 查看特定 Job 的日志
kubectl logs job/polardb-storage-resizer-28572960 -n dba
```

### 日志格式

日志输出包含 Trace ID 用于追踪：

```text
INFO [550e8400-e29b-41d4-a716-446655440000]: Starting PolarDB Storage Resizer
INFO [550e8400-e29b-41d4-a716-446655440000]: Discovered 10 total clusters
INFO [550e8400-e29b-41d4-a716-446655440000]: Selected 3 target clusters
```

### Prometheus 监控

可选启用 Prometheus 指标导出：

```yaml
env:
  - name: METRICS_ENABLED
    value: "true"
```

## 故障排查

### 常见问题

#### 1. Pod 无法获取 RAM 凭证

**症状：** 日志显示认证失败

**排查：**

```bash
# 检查 ServiceAccount 注解
kubectl get sa polardb-resizer-sa -n dba -o yaml

# 检查 OIDC 配置
kubectl get configmap -n kube-system | grep rrsa
```

**解决：**

- 确认 RAM 角色的信任策略配置正确
- 确认 OIDC 提供者 ARN 正确
- 确认 ServiceAccount 名称和命名空间匹配

#### 2. 权限不足

**症状：** 日志显示 `ActionDenied` 错误

**排查：**

- 检查 RAM 角色是否附加了正确的权限策略
- 检查目标 PolarDB 集群是否有特殊访问限制

#### 3. 执行超时

**症状：** Pod 被强制终止

**排查：**

```bash
# 检查 Pod 终止原因
kubectl describe pod -l app.kubernetes.io/name=polardb-storage-resizer -n dba
```

**解决：**

- 增加 `activeDeadlineSeconds`
- 检查是否有大量集群需要处理
- 考虑分批处理或增加并发数

### 退出码说明

| 退出码 | 含义 | 处理建议 |
| --- | --- | --- |
| 0 | 成功 | 无需处理 |
| 1 | 部分失败 | 检查日志中的错误详情 |
| 2 | 配置错误 | 检查环境变量配置 |
| 3 | 被信号中断 | 检查是否被手动终止 |

## 安全最佳实践

### 1. 最小权限原则

- 仅授予必要的 PolarDB 权限
- 使用 Resource 限制可操作的集群

```json
{
  "Resource": [
    "acs:polardb:cn-hangzhou:*:cluster/pc-xxxxx",
    "acs:polardb:cn-hangzhou:*:cluster/pc-yyyyy"
  ]
}
```

### 2. 集群白名单与黑名单

使用 `CLUSTER_WHITELIST` 限制处理的集群：

```yaml
env:
  - name: CLUSTER_WHITELIST
    value: "pc-xxxxxxxxx,pc-yyyyyyyyy"
```

使用 `CLUSTER_BLACKLIST` 排除特定集群（优先级高于白名单）：

```yaml
env:
  - name: CLUSTER_BLACKLIST
    value: "pc-zzzzzzzzz"  # 即使在白名单中也会被排除
```

**过滤优先级**：黑名单 > 白名单 > 全部符合条件的集群

### 3. 安全阈值

合理配置安全阈值防止意外：

- **`MAX_EXPAND_RATIO`**：扩容比例超过此值时，限制到 `当前容量 × 比例`（而非跳过），多次执行后逐步扩到目标值
- **`MAX_SHRINK_RATIO`**：缩容比例低于此值时，限制到 `当前容量 × 比例`（而非跳过），多次执行后逐步收敛到目标值
- **`MAX_SINGLE_CHANGE_GB`**：单次变更量超过此值时，限制到此上限

```yaml
env:
  - name: MAX_EXPAND_RATIO
    value: "1.5"      # 超过 1.5 倍扩容则限制（非跳过）
  - name: MAX_SHRINK_RATIO
    value: "0.7"      # 缩容不低于当前 70%，超出则限制（非跳过）
  - name: MIN_CHANGE_THRESHOLD_GB
    value: "20"       # 至少 20GB 变化才执行
```

> `MAX_EXPAND_RATIO` 和 `MAX_SHRINK_RATIO` 均采用渐进策略：例如 1000GB 集群使用率仅 20%，理想目标为 210GB（比例 0.21），在 `MAX_SHRINK_RATIO=0.5` 下每次最多缩到 50%，经过 2-3 次 CronJob 执行后收敛到最优值。同理，扩容也采用渐进式 cap 策略。

### 4. 存储类型限制

程序会根据集群的存储类型自动应用对应的最小和最大存储限制：

| 存储类型 | 最小存储 | 最大存储 |
| --- | --- | --- |
| `psl5` / `psl4` | 10 GB | 500 TB |
| `essdpl0` | 20 GB | 32 TB |
| `essdpl1` / `essdpl2` / `essdpl3` / `essdautopl` | 20/470/1270/40 GB | 64 TB |

> 对于 ESSD PL2/PL3 类型，由于最小存储要求较高，请确保目标存储不会触发过大的缩容比例。
> 企业版实际最大存储取决于节点规格（100TB~500TB），API 会在超出节点规格时拒绝变更。

### 5. 先 dry-run 后 apply

建议先在 dry-run 模式下验证：

```bash
# 1. 先部署 dry-run 模式
kubectl apply -f k8s/cronjob.yaml  # RUN_MODE=dry-run

# 2. 手动触发并检查日志
kubectl create job --from=cronjob/polardb-storage-resizer test-$(date +%s) -n dba
kubectl logs -l app.kubernetes.io/name=polardb-storage-resizer -n dba

# 3. 确认无误后，修改为 apply 模式
# 更新 ConfigMap 或 CronJob 的 env 配置
```

### 6. 审计日志

所有 API 调用都会通过 RAM 角色的 Session Name 记录在 ActionTrail 中，便于审计追踪。

## 参考链接

- [阿里云 RRSA 文档](https://help.aliyun.com/document_detail/439862.html)
- [PolarDB API 文档](https://api.aliyun.com/document/polardb/2017-08-01)
- [Kubernetes CronJob 文档](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
