# 迭代计划:存储判定语义修正

> 权威链:本计划遵从 `docs/design-storage-edition-refactor.md`(arch-design,冲突时 arch-design 胜)。
> 执行:由 `/solidforge:parallel-development` 消费冻结的可执行子集(item_id / seq / depends_on / dod_ref)按 DAG 实现。

## 1. 复杂度分级(complexity tiers)

| Tier | 含义 | 本计划项 |
|---|---|---|
| S | 单文件小改、低风险 | ser-1 models、ser-2 aliyun_client |
| M | 决策核心、需配套测试 | ser-3 strategy |
| M | 测试改写+新增 | ser-4 tests |
| S | 文档重写 | ser-5 docs |

## 2. 依赖边与 DAG(dependency edges / dag)

```
ser-1 (models: 加 storage_max_gb + 修 docstring)
 ├──▶ ser-2 (aliyun_client: 解析 storage_max)
 └──▶ ser-3 (strategy: 检测改 category + effective_max + 表补 key)
        ├──▶ ser-4 (tests: 改写 SENormal 误模型 + 新增 storage_max 用例)
        └──▶ ser-5 (docs: README + deployment 类型表重写)
ser-2 ──▶ ser-4
```

并行展开点:ser-2 与 ser-3 在 ser-1 后可并行(wave-1);ser-4 与 ser-5 在 ser-3 后可并行(wave-2)。

## 3. 可执行项(items)

> 注:ser-3 既改行为也**就地改写被该行为直接打破的测试**,使 ser-3 单独可绿;ser-4 只做**新增覆盖 + fixture 接线**(纯加法),避免"代码绿了但测试还在 ser-4 里红"的 transient-red(外环 novel-1)。

### ser-1 — models.py:新增字段 + 修正语义
- `ClusterDetail` 加 `storage_max_gb: int | None = None`(注释:来自 API `StorageMax`,byte→GB)。
- docstring:`SENormal` 由 "Serverless — expand only" 改为 **"标准版(Standard Edition)— select 阶段完全排除"**;补 `serverless_type` 与版本/缩容判定无关。
- 复杂度 S;依赖 []。
- DoD:`mypy src/` 通过;字段默认 None 不破坏现有构造。

### ser-2 — aliyun_client.py:解析 storage_max(含单测)
- `get_cluster_detail`(L285-300):`storage_max_gb = int(storage_max/1024**3) if storage_max else None`,填入新字段。
- 复杂度 S;依赖 [ser-1]。
- DoD(可单测,外环 novel-6):单测用桩 `DescribeDBClusterAttribute` 响应 `storage_max=109951162777600` → 断言 `storage_max_gb==102400`;`storage_max` 缺省/0 → 断言 `None`。另:18 实例 describe 实测企业盘 102400。

### ser-3 — strategy.py:检测改挂 category + API 上限 + 表补 key + 就地改写被破测试
- `_is_standard_edition`:`category=="SENormal"` 主信号 OR `storage_type∈ESSD` 次信号。
- `_is_expand_only`:仅 `category=="NormalMultimaster"`(移除 SENormal)。
- 新增 `effective_max_storage_gb(detail)`:`detail.storage_max_gb or get_max_storage_gb(storage_type)`。
- compute L313-314 用之;**validate 插入点精确**:在 `shrink_safe_floor` 块(L418-422)之后、`if result == current: return None`(L426)**之前**插入 `result = min(result, effective_max_storage_gb(detail))`(外环 novel-3,避免封顶后绕过 no-op/阈值检查)。
- `STORAGE_TYPE_MIN_GB`/`MAX_GB` 补 `highperformance`/`standard` key。
- 修正 L212 日志:`"Skipping Standard Edition ESSD cluster %s"` → 含 category/storage_type(外环 novel-4,因主信号已不限 ESSD)。
- 修正注释(删 Serverless/psl 误述)。
- **就地改写被本项行为直接打破的测试**:`test_serverless_psl_shrink_blocked`、`test_serverless_psl_expand_allowed`、`test_serverless_cluster_selected_but_shrink_blocked`(SENormal 现为"标准版完全排除"语义);并更新 `test_empty_whitelist_selects_all_eligible` 的 expected_count 公式为双信号(外环 novel-1/novel-2)。
- **新增 `effective_max_storage_gb` 直测**(外环 novel-8):`storage_max_gb` 有值→返回之;`None`→回退 `get_max_storage_gb(storage_type)`。
- 复杂度 M;依赖 [ser-1]。
- DoD:`pytest tests/test_strategy.py` 全绿(含就地改写的用例 + `effective_max_storage_gb` 双路径直测)。

### ser-4 — tests:新增覆盖 + fixture 接线(纯加法)
- `conftest.cluster_data_to_detail` 加 `storage_max_gb=cluster_data.get("storage_max_gb")`。
- `sample_clusters.json`:`pc-sless-bbbbbbbbbb` description 由"Serverless 仅扩容"改为"标准版(完全排除)"(category=SENormal 不变)。
- 新增用例(均为 `compute_target_storage` 集成级,非 `effective_max_storage_gb` 直测——后者在 ser-3):`storage_max_gb` 命中截断 target、`storage_max_gb=None` 走兜底表、`_is_standard_edition` 对 `category=SENormal + storage_type=HighPerformance` 判 True、HighPerformance/Standard min=10。
- 复杂度 M;依赖 [ser-2, ser-3]。
- DoD(可证伪,外环 novel-5):`pytest tests/ -v --tb=short` 全绿;**且无任何测试断言出现于 `STORAGE_TYPE_MIN_GB`/`MAX_GB` 表中的字面量**(每个断言目标值须由测试内文档化的输入 used/buffer/caps 推导)。

### ser-5 — docs:README + deployment 重写
- `README.md` §"处理对象与过滤规则" + 集群类型表 + storage_type 上下限表;`docs/deployment.md`。
- 版本检测改述为 category 主信号;`SENormal=标准版`;上限注明"以 API `StorageMax` 为准,企业盘实测 100TB"。
- 复杂度 S;依赖 [ser-3]。
- DoD(外环 novel-7):**先按当前文件重新核对所引行号区间**(README/deployment 有未提交改动),区间漂移则同步更新 ser-5 指引;改后文档与代码语义一致、无残留 psl*/Serverless 误述。

## 4. 每迭代完成定义(per-iteration DoD / validation gate)

- 每项完成后:`uv run ruff check src/ tests/` + `uv run ruff format` + `uv run mypy src/` 必须绿。
- 全部完成后:`uv run pytest tests/ -v --tb=short` 全绿 + import-linter。
- 收尾:18 实例 describe 回归 + `RUN_MODE=dry-run` 端到端。

## 5. 阶段验收闸(phase acceptance gates)

- Gate-A(ser-1/2/3 完成后):核心逻辑可单测;`effective_max_storage_gb` 在 compute+validate 双侧生效;describe 显示企业盘上限=API 值。
- Gate-B(ser-4/5 完成后):全套测试绿 + 文档与代码一致 + dry-run E2E 通过。

## 6. 风险与缓解(risks and mitigations)

| 风险 | 缓解 |
|---|---|
| 改写 SENormal 测试时误删有效覆盖 | 保留"企业 serverless PSL 可缩"等价用例(`category=Normal+serverless_type=SteadyServerless`) |
| `storage_max_gb` 缺省时行为漂移 | 保留静态表兜底,缺省→表(与旧行为一致) |
| 文档/代码语义再分叉 | ser-5 显式以 ser-3 实现为准对齐 |

## 7. 范围外(out-of-scope)

- 标准版纳入管理、多主放开缩容、buffer_percent/floor 调参、PG 支持(见 arch-design §3 / §9)。

## 8. 横切任务(cross-cutting tasks)

- 注释/文档中所有 `SENormal=Serverless`、`psl*/essd* 为真实 storage_type` 的误述统一修正(跨 strategy.py / models.py / README / deployment)。
- 不得回归 `docs/design-shrink-safety-floor.md` 的 compress 门控与 floor 数学。
