# 设计文档:存储判定语义修正(edition 检测 + API StorageMax 上限)

> 状态:草案,待 `/solidforge:cross-source-review` 同源+异源多轮收敛。
> 权威链:本设计 → 迭代计划 → 实现。内容正确性属 outcome 轴,需人工确认。

## 1. 背景与动机

`strategy.py` 的存储判定逻辑建立在**与真实 API 返回值不符的假设**之上。通过对 `cn-hangzhou` 全部 18 个带 `auto-resize:on` 标签的 MySQL 集群逐个 `DescribeDBClusterAttribute`(只读)取证,确认四类根因:

### 1.1 `storage_type` 查表是"虚构"的

真实 `StorageType` 字段只返回三种值:

| 真实值 | 含义 | 出现实例数 |
|---|---|---|
| `HighPerformance` | 企业版 PSL(历史遗留伞标) | 15(含 1 多主) |
| `Standard` | 企业版压缩盘(通用云盘) | 2(均 compress=ON) |
| `essdpl1` | 标准版 ESSD PL1 | 1 |

而 `STORAGE_TYPE_MIN_GB` / `STORAGE_TYPE_MAX_GB` 的 key 是 `psl5/psl4/essdpl0/essdpl2/essdpl3/essdautopl` —— **企业盘的 `HighPerformance`/`Standard` 一个都命中不了**,全部 fallback 到 `DEFAULT_MIN_STORAGE_GB=20` / `DEFAULT_MAX_STORAGE_GB=32000`。代码里 PSL/ESSD 各分支在这批实例上从未生效。

### 1.2 真实上限被错误封顶 32TB

API 明确返回每实例 `StorageMax=109951162777600 B = 102400 GB`(100TB,14 个企业盘一致);标准版 dianplus-open 为 65500GB(~64TB)。代码却用静态表 32000 去 `min(target, 32000)` 截断。一旦某企业盘需扩到 32TB 以上,会被错误截断(真实上限 100TB)。

### 1.3 版本检测挂错字段

`_is_standard_edition` 靠 `storage_type ∈ ESSD集合` 判标准版,纯属运气命中(本批唯一标准版 dianplus-open 恰好返回 `essdpl1`)。可靠信号是 `category=SENormal`:`DescribeDBClusterAttribute` 文档 `Category` 取值 `SENormal=标准版`,数据印证(dianplus-open: SENormal + essdpl1 + 64TB)。

### 1.4 `SENormal` 被误当 Serverless

`models.py` 注释与 `_is_expand_only` 把 `SENormal` 当 Serverless。实际:

- 文档 `Category`:`SENormal=标准版`;`serverless_type`(`AgileServerless`/`SteadyServerless`)才是 serverless 标记。
- 数据:13 个 `category=Normal` 企业盘带 `serverless_type=SteadyServerless` —— serverless 挂在企业版上,与版本正交。
- `serverless_type` 全仓**只被 log、从未进任何决策**(已由集成排查确认)。

这与用户确认一致:**serverless 节点可附加在企业版上;纯 serverless PSL 集群,只要存储为 PSL 系列,目前无规则表明其不能缩容。**

## 2. 18 实例真值快照(扩缩容关键字段)

`buffer_percent` 默认 105。`used%` = 计费用量/已分配。

| 实例 | category | storage_type | compress | 已分配GB | 用量GB(计费) | used% | 当前代码判定 |
|---|---|---|---|---|---|---|---|
| dianplus-pay | Normal | HighPerformance | OFF | 2620 | 2545.6 | 97% | 正常 |
| dianplus-corp | Normal | HighPerformance | OFF | 60 | 55.3 | 92% | 正常 |
| common | Normal | HighPerformance | OFF | 340 | 327.6 | 96% | 正常 |
| dianplus-order | Normal | HighPerformance | OFF | 3970 | 3777.0 | 95% | 正常 |
| dianplus-index | Normal | HighPerformance | OFF | 1340 | 1278.4 | 95% | 正常 |
| dianplus-goods | Normal | HighPerformance | OFF | 1560 | 1485.7 | 95% | 正常 |
| dianplus-platform | Normal | HighPerformance | OFF | 1070 | 943.9 | 88% | 正常(想缩,被 floor 挡) |
| dianplus-wms | Normal | HighPerformance | OFF | 3890 | 3699.8 | 95% | 正常 |
| dianplus-drp | Normal | HighPerformance | OFF | 720 | 691.6 | 96% | 正常 |
| dianplus-common | Normal | HighPerformance | OFF | 190 | 183.4 | 97% | 正常 |
| dianplus-scm | Normal | HighPerformance | OFF | 220 | 214.9 | 98% | 正常 |
| dianplus-scrm | Normal | HighPerformance | OFF | 2280 | 2162.6 | 95% | 正常 |
| dianplus-stock | Normal | HighPerformance | OFF | 3880 | 3687.9 | 95% | 正常 |
| incapp | Normal | HighPerformance | OFF | 20 | 24.0 | 120% | 正常(已超额) |
| dianplus-mixed | **NormalMultimaster** | HighPerformance | OFF | 1740 | 1664.1 | 96% | **expand_only** |
| misc-cluster | Normal | **Standard** | **ON** | 190 | 182.5(原始1317.8) | 96% | 正常 |
| dianplus-report | Normal | **Standard** | ON | 1320 | 1264.4(原始2185.9) | 96% | 正常 |
| dianplus-open | **SENormal** | essdpl1 | OFF | 1710 | 1299.6 | 76% | **excluded(标准版)** |

所有实例 `StorageMax` = 102400GB(企业盘)/ 65500GB(标准版)。原始 body 存于 `$CLAUDE_JOB_DIR/tmp/details_raw.json`。

> 注(2026-07-30):"当前代码判定"列为取证时刻快照。shrink-safety-floor 已移除(§6):
> dianplus-platform 现可正常计划缩容;dianplus-pay 的 2620GB/97% 状态即为 floor 移除前
> 人工缩容成功的直接证据(80%/20G guard 不适用于企业版 PSL)。

## 3. 策略边界(用户已确认,不在本次改动范围)

- NormalMultimaster(多主):**保持仅扩容**(零回归)。
- 标准版(SENormal / ESSD):**保持完全排除**,仅修检测方式。
- `serverless_type` **不进任何决策**(仅日志)。
- 不动:`buffer_percent=105`。
- ~~不动:shrink-safety-floor 全套规则~~ **(2026-07-30 修订:已移除)**:官方文档将
  80%/20G 缩容限制限定于标准版 ESSD(本工具完全排除的版本);触发集群
  (pc-bp101=dianplus-pay,企业版 PSL)手工缩容 3260→2620 成功(利用率 97%),
  证明该 guard 不适用于企业版 PSL。floor 反而阻挡合法缩容(dianplus-platform,
  88%)且 SF-4 会在 INFO 级吞掉 shrink 路径上任何 `InvalidParam`。全套规则
  (floor、SF-3 re-check、SF-4 soft-skip、`ChangePlan.compress_storage_mode`、
  `ChangeResult.skipped`)已由 revert commit 移除,详见 §6。

## 4. 技术设计

### 4.1 判定决策树(目标态)

```
select_target_clusters:
  pay_type==Prepaid? status==Running? region∈config?  → 否:跳过
  ↓ 是
  _is_standard_edition?  → 是:排除(标准版)
    = category=="SENormal"  (主信号)
      OR storage_type.lower() ∈ STANDARD_EDITION_STORAGE_TYPES  (次信号)
  ↓ 否
  blacklist/whitelist 过滤
  ↓
  入选

compute_target_storage:
  B_target = ceil(used * buffer/100)
  若 target<current 且 _is_expand_only → None(仅扩容)
    _is_expand_only = category=="NormalMultimaster"   # serverless_type 不参与
  上限 = effective_max_storage_gb(detail)
    = detail.storage_max_gb  (API 真值,优先)
      else get_max_storage_gb(storage_type)  (兜底表)
```

### 4.2 改动震中:`src/polardb_storage_resizer/strategy.py`

1. **`_is_standard_edition`(L41-55)** — 改挂 category 主信号 + ESSD 次信号:
   ```python
   def _is_standard_edition(cluster) -> bool:
       if cluster.category == "SENormal":
           return True
       return (cluster.storage_type or "").lower().strip() in STANDARD_EDITION_STORAGE_TYPES
   ```
   `STANDARD_EDITION_STORAGE_TYPES`(L25-38)保留(essd* 作次信号)。

2. **`_is_expand_only`(L58-69)** — 移除 SENormal(上游已排除),只留多主:
   ```python
   def _is_expand_only(cluster) -> bool:
       return cluster.category == "NormalMultimaster"
   ```
   修正 L61 / L184-186 / L265 注释:删 "Serverless (SENormal) … cannot shrink",改为"多主仅扩容;serverless_type 与缩容无关(PSL 可缩)"。

3. **上限改用 API StorageMax** — 新增 `effective_max_storage_gb(detail)`(保留静态表兜底,**现有 max 测试零改动通过**):
   ```python
   def effective_max_storage_gb(detail) -> int:
       if detail.storage_max_gb:
           return detail.storage_max_gb
       return get_max_storage_gb(detail.storage_type)
   ```
   - `compute_target_storage` L313-314 改用之。
   - **补不对称**:`validate_storage_constraints` 加 `result = min(result, effective_max_storage_gb(detail))`(当前只在 compute 限 max、validate 不复查)。**插入点精确**:在 `if result == current: return None` **之前**——确保封顶后仍经 no-op 与 min_change_threshold 检查(末尾插入会绕过它们)。(2026-07-30 修订:原先"在 shrink_safe_floor 块之后"的表述随 floor 移除作废,cap 现独立位于 no-op 检查前。)
   - `STORAGE_TYPE_MAX_GB`(L111-128)保留兜底;新增 `'highperformance': 100000, 'standard': 100000`。

4. **下限表补 key** — `STORAGE_TYPE_MIN_GB`(L86-102)新增 `'highperformance': 10, 'standard': 10`;essd 条目保留(无害兜底)。

### 4.3 `models.py`

- `ClusterDetail` 新增 `storage_max_gb: int | None = None`(近 `provisioned_storage_gb`;注释:来自 API `StorageMax`,byte→GB)。
- 修正 docstring L65-74:`SENormal` 由 "Serverless — expand only" 改为 **"标准版(Standard Edition)— select 阶段完全排除"**;补 `serverless_type` 与版本/缩容判定无关。

### 4.4 `aliyun_client.py`

- `get_cluster_detail`(L285-300):解析 `storage_max`,`storage_max_gb = int(storage_max/1024**3) if storage_max else None`,填新字段。

### 4.5 文档(随真实字段重写)

- `README.md` §"处理对象与过滤规则" + 集群类型表 + storage_type 上下限表(L189-246):版本检测改述为 category 主信号;`SENormal=标准版`(非 Serverless);上限注明"以 API `StorageMax` 为准,企业盘实测 100TB"。
- `docs/deployment.md`(L504-527)同步。

## 5. 爆炸半径(测试)

> 排查结论:保留 ESSD 次信号 + 静态表兜底后,多数现有测试无需改动。

- `conftest.py::cluster_data_to_detail`(L88-113):加 `storage_max_gb=cluster_data.get("storage_max_gb")`。
- `test_strategy.py`:
  - **改写误模型测试**(`TestClusterTypeFiltering` L949-1012):`test_serverless_cluster_selected_but_shrink_blocked` 重命名为标准版排除语义;`test_serverless_psl_expand_allowed`/`test_serverless_psl_shrink_blocked`(SENormal+psl4)删除或改为 `category=Normal + serverless_type=SteadyServerless + HighPerformance` 的"企业 serverless PSL 可缩"用例。
  - `test_empty_whitelist_selects_all_eligible`(L109,直接 import frozenset):因保留 ESSD 次信号,essdpl1 用例仍排除;按新双信号复核 expected_count。
  - **新增**:`storage_max_gb` 命中时截断 target;`storage_max_gb=None` 走兜底表(现有 `TestMaxStorageLimits` 已覆盖);`_is_standard_edition` 对 `category=SENormal + storage_type=HighPerformance` 判 True;HighPerformance/Standard min=10。
- `fixtures/sample_clusters.json`:`pc-sless-bbbbbbbbbb`(`category=SENormal`,现述"Serverless 仅扩容")重标注为"标准版(完全排除)";`pc-std-aaaaaaaaaa`(essdpl1)保留。
- `test_executor.py`:apply-time re-check(compress 门控)不受影响;若内联 `ClusterDetail` 涉及 SENormal 按新语义复核。

## 6. 不得回归

> 2026-07-30 修订:本节原先转录的 shrink-safety-floor 六条约束(来自
> `docs/design-shrink-safety-floor.md`)**全部作废** —— 该设计的经验锚点
> (企业版 PSL 集群 shrink 被 80%/20G guard 拒绝)不成立:官方文档将该 guard
> 限定于标准版 ESSD,且触发集群手工缩容 3260→2620 在 97% 利用率下成功。
> floor 全套实现(floor、SF-3 apply-time re-check、SF-4 InvalidParam soft-skip、
> `ChangePlan.compress_storage_mode`、`ChangeResult.skipped`)已由 revert commit
> 移除;`docs/design-shrink-safety-floor.md` 与对应迭代计划已标注 SUPERSEDED。
> 保留的通用约束:

- `CLAUDE.md` L1 红线:命名须表意(本方案修正 SENormal 命名矛盾)、不得靠硬编码数字把测试刷绿。
- 缩容下限回归 `buffer_percent`(默认 105)单约束:目标 ≥ `ceil(used * buffer/100)`,
  按 10GB 步长对齐;企业版 PSL 无额外 margin guard(实测),标准版 ESSD 的
  80%/20G 规则不进入决策(该版本完全排除)。
- shrink 路径上的 `InvalidParam` 等永久错误**必须硬失败**(ERROR 计数进
  `total_failed`),不得 INFO 级软跳过吞掉。

## 7. 收敛流程(用户要求)

1. 本文档作为 cross-source-review 目标产物。
2. 运行 `/solidforge:cross-source-review` 同源+异源多轮收敛 → converged 文档 + convergence-record。
3. 按 converged 结论落地代码/测试/文档。
4. 回归 18 实例 describe 验证。

## 8. 验证

1. **门禁**:`uv run ruff check src/ tests/`、`uv run ruff format`、`uv run mypy src/`、`uv run pytest tests/ -v --tb=short`、import-linter。
2. **18 实例回归**:`…/tmp/describe_clusters.py` 重跑,确认 14 企业盘上限从 32000→100000(或命中 `storage_max_gb=102400`);dianplus-mixed 仍 expand_only;dianplus-open 仍 excluded;压缩盘正常。
3. **cross-source-review**:本文档多轮收敛通过、convergence-record 诚实。
4. dry-run 端到端:`RUN_MODE=dry-run uv run python -m polardb_storage_resizer.main`。

## 9. 架构锚点(arch-design 完备性补全)

- **定位 / what it does NOT do**:仅修正"如何识别版本/上限"(检测改挂 category、上限用 API `StorageMax`),**不改策略语义**。不做:标准版纳入管理、多主放开缩容、buffer 调参、PG 集群支持。(2026-07-30 修订:"floor 调参"随 floor 移除从清单删除。)
- **分层与依赖方向(layering)**:`aliyun_client.get_cluster_detail`(解析 API 字段)→ `models.ClusterDetail`(纯数据载体)→ `strategy.compute/validate`(纯决策,无 I/O)→ `executor`(编排 + apply-time 门控)。决策层不触网,字段单向自下而上流动。
- **跨边界契约(cross-boundary contracts)**:新增字段 `ClusterDetail.storage_max_gb` 是 aliyun_client↔strategy 的唯一新契约;`effective_max_storage_gb(detail)` 是 compute 与 validate 共用的取上限入口(消除"compute 限 max、validate 不复查"的不对称)。`serverless_type` 退化为纯日志字段。
- **并行边界(files_touched)**:models.py、aliyun_client.py、strategy.py、tests/、README.md、deployment.md —— 见 §4 与 iteration-plan 的 DAG,文件级无写冲突。
- **失败模式 / 降级(failure modes)**:API 未返回 `StorageMax` → `storage_max_gb=None` → 回退静态表(不报错);`storage_type` 未知 → 回退 DEFAULT。均为降级而非硬失败。不得回归项见 §6。
- **版本策略(version strategy)**:`ClusterDetail` 加可选字段(默认 None),向后兼容;静态表只增 key 不删,旧测试零改动。schema 演进为加性。

