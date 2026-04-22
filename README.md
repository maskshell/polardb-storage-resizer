# PolarDB Storage Resizer

自动调整 PolarDB 包年包月存储大小的 Kubernetes CronJob 服务。

## 场景概述

本项目用于 **定时将包年包月 PolarDB 实例的"预置存储大小"调整到接近实际使用量**，避免长期预置过大造成闲置成本，并在**用量超过预置时（如存在超额计费）**减少超出部分的按量费用。

## 核心策略

对每个目标实例计算新的预置存储大小：

```text
B_target = ceil(A × buffer_percent / 100)
```

- **A**：实例当前 **已使用存储大小** (GB)。若集群开启了存储压缩（`CompressStorageMode=ON`），使用压缩后大小（`CompressStorageUsed`，即计费口径）；否则使用原始大小（`StorageUsed`）
- **B**：实例当前 **包年包月预置存储大小** (GB)
- **B_target**：目标预置存储大小
- **buffer_percent**：存储缓冲百分比，默认 105（即 1.05 倍），可通过 `BUFFER_PERCENT` 配置

### API 约束

`B_target` 计算后还需通过以下 API 约束校验（`validate_storage_constraints`）：

1. **步长对齐**：目标值必须为 10GB 的整数倍。扩容向上取整，缩容向下取整（避免方向翻转）
2. **最小存储**：不低于存储类型对应的最小值（参见下表）
3. **最大存储**：不超过存储类型对应的最大值（参见下表）
4. **幂等性**：每次 API 调用携带 ClientToken（`MD5(region:cluster_id:new_size_gb)`），同一请求 24 小时内自动去重

## 快速开始

### 1. 环境要求

- Python >= 3.12
- Kubernetes 集群（可选，用于生产部署）
- 阿里云账号及 PolarDB 实例

### 2. 本地开发

```bash
# 克隆项目
git clone https://github.com/maskshell/polardb-storage-resizer
cd polardb-storage-resizer

# 安装依赖
uv sync --frozen --dev
```

**本地 CI 检查**（需要 [just](https://github.com/casey/just)）：

```bash
# 安装 just
brew install just

# 运行全部 CI 检查（lint + test + helm-lint）
just ci

# 或单独运行
just lint
just test
just helm-lint
just docker-build
```

### 3. 本地运行（dry-run 模式）

```bash
# 设置环境变量
export RUN_MODE=dry-run
export REGIONS=cn-hangzhou,cn-beijing
export LOG_LEVEL=DEBUG

# 使用 Fake 客户端运行（不需要真实凭据，用于测试流程）
export USE_FAKE_CLIENT=true
uv run python -m polardb_storage_resizer.main
```

### 4. 本地连接真实 PolarDB（测试模式）

如果需要在本地测试真实 API 调用（dry-run 模式下不会执行修改）：

```bash
# 方式一：使用 AccessKey（推荐用于本地开发）
export ALIBABA_CLOUD_ACCESS_KEY_ID="your-access-key-id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your-access-key-secret"
export RUN_MODE=dry-run
export REGIONS=cn-hangzhou
# 不设置 USE_FAKE_CLIENT，会自动使用真实客户端

# 方式二：使用 RAM Role（适用于 ECS 环境）
export ALIBABA_CLOUD_ROLE_ARN="acs:ram::1234567890123456:role/YourRole"
export RUN_MODE=dry-run
export REGIONS=cn-hangzhou

# 运行（dry-run 只查询不修改）
uv run python -m polardb_storage_resizer.main
```

**注意**：

- AccessKey 方式需要创建具有 PolarDB 只读权限的 RAM 用户
- 生产部署使用 RRSA，无需 AccessKey

### 5. 生产部署（apply 模式）

```bash
# 设置环境变量
export RUN_MODE=apply
export REGIONS=cn-hangzhou
export ALIBABA_CLOUD_ROLE_ARN=acs:ram::1234567890123456:role/PolardbResizerRole

# 运行（会执行实际修改）
uv run python -m polardb_storage_resizer.main
```

## 配置说明

### 环境变量

| 变量名 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `RUN_MODE` | 是 | `dry-run` | 运行模式：`dry-run`（仅计划）或 `apply`（执行修改） |
| `REGIONS` | 是 | - | 目标区域，多个用逗号分隔，如 `cn-hangzhou,cn-beijing` |
| `LOG_LEVEL` | 否 | `INFO` | 日志级别：`DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `BUFFER_PERCENT` | 否 | `105` | 存储缓冲百分比（必须 > 100），如 110 表示目标为使用量的 1.1 倍 |
| `MAX_PARALLEL_REQUESTS` | 否 | `5` | 最大并发请求数 |
| `MAX_QPS` | 否 | `10` | 每秒最大 API 请求数 |
| `MAX_EXPAND_RATIO` | 否 | `2.0` | 最大扩容比例（相对当前大小） |
| `MAX_SHRINK_RATIO` | 否 | `0.5` | 最小缩容比例（目标/当前） |
| `MAX_SINGLE_CHANGE_GB` | 否 | `1000` | 单次最大变更量（GB） |
| `MIN_CHANGE_THRESHOLD_GB` | 否 | `10` | 最小变更阈值（GB），低于此值不执行变更。必须为 10 的倍数 |
| `RETRY_MAX_ATTEMPTS` | 否 | `3` | 最大重试次数 |
| `RETRY_BACKOFF_BASE` | 否 | `1.0` | 重试退避基数（秒） |
| `RETRY_BACKOFF_MAX` | 否 | `30.0` | 最大退避时间（秒） |
| `ALIBABA_CLOUD_ROLE_ARN` | apply 模式必填 | - | RRSA 角色 ARN |
| `CLUSTER_WHITELIST` | 否 | - | 集群白名单，多个用逗号分隔，设置后仅处理白名单内集群 |
| `CLUSTER_BLACKLIST` | 否 | - | 集群黑名单，多个用逗号分隔，优先级高于白名单 |
| `CLUSTER_TAG_FILTERS` | 否 | - | 集群标签筛选，格式为 `key1:value1,key2:value2`，仅处理具有所有指定标签的集群 |
| `API_CONNECT_TIMEOUT` | 否 | `5` | API 连接超时（秒） |
| `API_READ_TIMEOUT` | 否 | `30` | API 读取超时（秒） |
| `METRICS_ENABLED` | 否 | `true` | 是否输出结构化指标日志 |
| `USE_FAKE_CLIENT` | 否 | `false` | 使用 Fake 客户端（测试用），设为 `true` 启用 |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | 否 | - | AccessKey ID（本地开发用） |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 否 | - | AccessKey Secret（本地开发用） |

### YAML 配置文件

```yaml
# config.yaml
run_mode: dry-run
regions:
  - cn-hangzhou
  - cn-beijing
log_level: INFO
buffer_percent: 105  # 存储缓冲百分比，默认 105 (1.05 倍)
max_parallel_requests: 5
max_qps: 10
max_expand_ratio: 2.0
max_shrink_ratio: 0.5
max_single_change_gb: 1000
min_change_threshold_gb: 10
cluster_whitelist:  # 白名单：仅处理列表中的集群
  - pc-xxxxxxxxx
  - pc-yyyyyyyyy
cluster_blacklist:  # 黑名单：排除列表中的集群（优先级高于白名单）
  - pc-zzzzzzzzz
cluster_tag_filters:  # 标签筛选：仅处理具有所有指定标签的集群（API 级别筛选）
  Environment: production
  Team: backend
```

## 退出码

| 退出码 | 含义 |
| --- | --- |
| 0 | 成功（包括无需变更的情况） |
| 1 | 部分变更失败 |
| 2 | 配置错误或启动验证失败 |
| 3 | 被信号中断（SIGTERM/SIGINT） |

## 处理对象与过滤规则

仅处理满足以下条件的 PolarDB 实例：

1. **计费类型**：包年包月（Prepaid）
2. **运行状态**：运行中（Running）
3. **区域**：在配置的 REGIONS 列表中
4. **集群类型**：标准版 ESSD 集群硬编码排除（所有操作包括扩容和缩容）
5. **黑名单**：如果配置了 CLUSTER_BLACKLIST，则排除黑名单内的集群
6. **白名单**：如果配置了 CLUSTER_WHITELIST，则仅处理白名单内的集群（黑名单优先级更高）

### 集群类型与操作限制

> 参考：[手动扩缩容限制](https://help.aliyun.com/zh/polardb/polardb-for-mysql/user-guide/manually-scale-up-the-storage-capacity-of-a-cluster-1)

| 集群类型 | API 识别 | 操作 | 原因 |
| --- | --- | --- | --- |
| **企业版** | `category="Normal"`, PSL 存储 | 扩容 + 缩容 | 主要处理对象 |
| **标准版 ESSD** | `storage_type` ∈ ESSD 集合 | 全部排除 | 缩容需数据迁移（150MB/s），扩容由 PolarDB 自带自动扩容处理 |
| **多主集群** | `category="NormalMultimaster"` | 仅扩容 | 不支持存储缩容 |
| **Serverless** | `category="SENormal"` | 仅扩容 | 不支持存储缩容 |

识别规则：
- **标准版**：通过 `DescribeDBClusterAttribute` 返回的 `StorageType` 判断，ESSD 类型（`essdpl0`/`essdpl1`/`essdpl2`/`essdpl3`/`essdautopl`）即为标准版
- **多主集群 / Serverless**：通过 `Category` 字段判断
- `serverless_type` 字段（如 `SteadyServerless`）为企业版代理特性，不影响集群类型判定

## 安全机制

所有安全阈值采用 **封顶（cap）而非跳过（skip）** 策略：当理想目标超出阈值时，将目标限制到阈值边界，而非放弃本次变更。经过多次 CronJob 执行后逐步收敛到最优值。

### 渐进收敛示例

假设 1000GB 集群使用率仅 20%，理想目标为 210GB（比例 0.21），在 `MAX_SHRINK_RATIO=0.5` 下：

```text
第 1 次执行：1000 → 500（cap 到 50%）
第 2 次执行：500  → 250（210/500=0.42 < 0.5，再次 cap）
第 3 次执行：250  → 210（210/250=0.84 > 0.5，到达理想目标）
```

扩容同理：理想目标超过 2 倍时，每次最多扩到当前容量的 `MAX_EXPAND_RATIO` 倍。

### 安全阈值

| 参数 | 默认值 | 行为 |
| --- | --- | --- |
| `MAX_EXPAND_RATIO` | `2.0` | 扩容：目标超过当前 × 此值时，限制到 `当前 × 此值` |
| `MAX_SHRINK_RATIO` | `0.5` | 缩容：目标低于当前 × 此值时，限制到 `当前 × 此值` |
| `MAX_SINGLE_CHANGE_GB` | `1000` | 单次变更绝对值不超过此值（必须为 10 的倍数） |
| `MIN_CHANGE_THRESHOLD_GB` | `10` | 变更量低于此值时跳过（必须为 10 的倍数） |

### 存储类型限制

PolarDB 的存储限制与集群的存储类型相关，程序自动根据 `DescribeDBClusterAttribute` 返回的 `StorageType` 应用对应的最小值和最大值：

| 存储类型 | 最小存储 | 最大存储 | 说明 |
| --- | --- | --- | --- |
| `psl5` / `psl4` | 10 GB | 500 TB | 企业版。实际最大取决于节点规格（100TB~500TB），超出时 API 拒绝变更 |
| `essdpl0` | 20 GB | 32 TB | 标准版 ESSD PL0 |
| `essdpl1` / `essdpl2` / `essdpl3` | 20/470/1270 GB | 64 TB | 标准版 ESSD PL1/PL2/PL3 |
| `essdautopl` | 40 GB | 64 TB | 标准版 ESSD 通用云盘 |

> **未知存储类型**：默认最小值 20GB，默认最大值 32TB（使用最保守的标准版 PL0 限制）。
>
> **参考文档**：
>
> - 最小存储：[ModifyDBClusterStoragePerformance](https://api.aliyun.com/document/polardb/2017-08-01/ModifyDBClusterStoragePerformance)
> - 最大存储：[PolarDB MySQL 版选型指南](https://help.aliyun.com/zh/polardb/polardb-for-mysql/polardb-mysql-edition-selection-guide)

### 敏感信息保护

所有日志输出自动脱敏：

- Request ID 自动隐藏
- Access Key 自动替换
- 错误信息中的敏感数据自动清除

## Kubernetes 部署

详见 [docs/deployment.md](docs/deployment.md)

### 快速部署

```bash
# 1. 创建命名空间
kubectl create namespace dba

# 2. 创建 ServiceAccount（配置 RRSA）
kubectl apply -f k8s/serviceaccount-rrsa.yaml -n dba

# 3. 部署 CronJob
kubectl apply -f k8s/cronjob.yaml -n dba

# 4. 验证部署
kubectl get cronjob -n dba
kubectl get pods -n dba
```

## API 参考

本项目使用以下阿里云 PolarDB API：

- [DescribeDBClusters](https://api.aliyun.com/document/polardb/2017-08-01/DescribeDBClusters) - 查询集群列表（自动分页，每页最多 100 条）
- [DescribeDBClusterAttribute](https://api.aliyun.com/document/polardb/2017-08-01/DescribeDBClusterAttribute) - 查看集群详细属性（返回 StorageType、Category、ServerlessType、StorageSpace、StorageUsed、CompressStorageMode 等字段）
- [ModifyDBClusterStorageSpace](https://api.aliyun.com/document/polardb/2017-08-01/ModifyDBClusterStorageSpace) - 变更存储空间（步长 10GB）

**存储类型限制参考**：

- [ModifyDBClusterStoragePerformance](https://api.aliyun.com/document/polardb/2017-08-01/ModifyDBClusterStoragePerformance) - 存储类型与最小存储限制的对应关系
- [PolarDB MySQL 版选型指南](https://help.aliyun.com/zh/polardb/polardb-for-mysql/polardb-mysql-edition-selection-guide) - 各存储类型最大存储限制

## RAM 权限准备

在部署前需要完成以下 RAM 配置：

### 1. 前置条件

- ACK 集群版本 ≥ 1.22
- 集群已启用 RRSA 功能
- 已获取集群的 OIDC Provider ARN（在集群详情页查看）

### 2. 创建 RAM 角色（带 OIDC 信任策略）

在 RAM 控制台创建角色，信任策略配置：

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
            "system:serviceaccount:<namespace>:<sa-name>"
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

**替换参数**：

- `<namespace>`: 部署命名空间（如 `dba`）
- `<sa-name>`: ServiceAccount 名称（Helm 部署时为 `<release>-polardb-storage-resizer`）
- `<alibaba-cloud-account-id>`: 阿里云账号 ID
- `<alibaba-cloud-k8s-cluster-id>`: ACK 集群 ID

### 3. 创建权限策略并附加到角色

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
      "Resource": ["*"],
      "Condition": {
        "StringEquals": {
          "acs:ResourceTag/auto-resize": ["on"]
        }
      }
    }
  ]
}
```

### 4. Helm 部署时指定角色名

```bash
helm install polardb-resizer ./charts/polardb-storage-resizer \
  --set rrsa.roleName="PolardbStorageResizerRole"
```

**注意**：只需指定角色名称（如 `PolardbStorageResizerRole`），不是完整 ARN。

详细配置说明请参考 [docs/deployment.md](docs/deployment.md)。

## 开发指南

### 项目结构

```text
src/polardb_storage_resizer/
├── __init__.py        # 包入口
├── aliyun_client.py   # 阿里云 PolarDB 客户端实现
├── config.py          # 配置加载与验证
├── errors.py          # 错误类型定义
├── executor.py        # 执行器与并发控制
├── cloud_client.py    # 云 API 抽象层 (Protocol)
├── logging_setup.py   # 日志配置
├── main.py            # CLI 入口
├── metrics.py         # 指标收集
├── models.py          # 数据模型
├── redaction.py       # 敏感信息脱敏
└── strategy.py        # 存储调整策略

tests/
├── conftest.py        # 共享 fixtures
├── fixtures/          # 测试数据
├── test_config.py     # 配置测试
├── test_strategy.py   # 策略测试
├── test_executor.py   # 执行器测试
└── ...

k8s/
├── cronjob.yaml           # CronJob 定义
└── serviceaccount-rrsa.yaml # RRSA ServiceAccount
```

### 运行测试

```bash
# 运行所有测试
uv run pytest tests/ -v

# 运行特定测试文件
uv run pytest tests/test_strategy.py -v

# 带覆盖率
uv run pytest tests/ --cov=polardb_storage_resizer --cov-report=html
```

### 代码规范

```bash
# 检查
uv run ruff check src/

# 格式化
uv run ruff format src/
```

## License

MIT
