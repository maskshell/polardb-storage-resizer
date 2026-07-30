# PolarDB Shrink 80%-Safety-Floor Fix — Converged Design

> **SUPERSEDED (2026-07-30) — the floor logic shipped in 05b6e3b has been REMOVED.**
> The design's empirical anchor did not hold: a manual shrink of the very cluster
> behind the trigger case (pc-bp101 = dianplus-pay, Enterprise `category=Normal`,
> PSL `storage_type=HighPerformance`, compress OFF) from 3260 → 2620 GB succeeded
> at ~97% utilization — impossible if the 80%/20 GB guard applied to it. The
> official doc scopes that guard to **Standard Edition ESSD only**
> (help.aliyun.com "手动扩容/缩容" 使用限制), an edition this tool excludes from
> all operations. The floor also over-blocked legitimate Enterprise shrinks
> (dianplus-platform, 88% used) and its SF-4 branch soft-skipped ANY `InvalidParam`
> on shrinks at INFO, masking real errors. See the removal commit
> (`revert: remove shrink safety floor`) and `design-storage-edition-refactor.md`
> §3/§6. Kept for historical record only.

> Status: design converged via `solidforge:cross-source-review` (same-family
> `doc-reviewer` + different-family DeepSeek/MiniMax; both legs × 2 rounds).
> See the **Convergence Record** at the end for the review trail. Date: 2026-07-29.

## Context

A `RUN_MODE=apply` run generated a shrink plan for `pc-bp101zlhwn5642n48`
(3260GB → 2660GB; plan-time used=2539.1 GB per the run log) that PolarDB **rejected at apply**:

```
InvalidParam code: 400, ... current dataSize(MB) is larger than targetSize * 0.8:
2723840MB or left space is smaller than: 20480.0MB
```

**Root cause (verified against live `DescribeDBClusterAttribute` + source):**

- PolarDB's shrink guard rejects when **`dataSize > targetSize × 0.8`** (≥20% free)
  OR **left space < 20GB**. `data_size`/`log_size`/`cold_data_size` are NOT exposed
  by the API (observed `None`) — only `storage_used` + `compress_storage_used`.
- The differentiator between this failure and a sibling shrink that **succeeded**
  (`pc-bp1zyvqt14f729u10`, 1320→1290) is **compression mode**: pc-bp101 is
  `compress_storage_mode='OFF'` (dataSize ≈ storage_used), pc-bp1zyv is `'ON'`.
- The resizer plans `target = ceil(used × buffer_percent/100)` with default
  `buffer_percent=105` ⇒ ~95% post-shrink utilization. For **non-compressed**
  clusters that structurally violates the 80% guard, so non-compressed shrink plans
  are doomed. **Provenance of the pc-bp101 `used` figures** (R2-D-1): `2539.1 GB` is
  the plan-time value from the run log; `2545.2 GB` is `storage_used` from a later
  live `DescribeDBClusterAttribute` pull (data grew ~6 GB between snapshots — both
  exceed 2660×0.8 = 2128, so the rejection holds for either). The error's `2723840`
  is in **MiB**; 2723840 MiB = 2660 GiB = the target (R2-D-2). Compressed clusters
  pass because the physical (compressed) data has effective headroom in logical
  capacity.

**Goal:** non-compressed shrinks never emit an API-rejectable plan; compressed
clusters keep working; doomed shrinks soft-skip instead of hard-failing.

## Converged Plan (cross-source-reviewed, substantive-converged)

All non-compressed-only logic is gated on `compress_storage_mode != "ON"`. When
the field is `None`/unknown (legal per §3), `None != "ON"` is True ⇒ the
non-compressed path applies (conservative: the floor may over-block a genuinely
compressed cluster whose mode the API didn't return, but it never under-protects).
Compression mode is treated as **authoritative at plan time** for the cycle (R2-D-5):
the apply-time re-check (§4) refreshes only `used`, not `compress_storage_mode`.

### 1. Safe floor in `validate_storage_constraints` — `strategy.py:322-399`
In the **shrink branch**, AFTER the 10GB step alignment (`strategy.py:356-360`),
for non-compressed clusters enforce:
```python
floor = max(math.ceil(used * 1.25), used + 20)   # 1.25 = inverse of ×0.8; +20 = left-space guard
floor = math.ceil(floor / STORAGE_STEP_GB) * STORAGE_STEP_GB   # strict round-UP to step
result = max(result, floor)
if result >= current:          # no safe shrink this cycle
    return None                # → plan_changes skips the cluster (executor.py:93-103)
```
`ceil(used×1.25)` is the algebraic inverse of `dataSize ≤ target×0.8`; `used+20`
covers the alternative 20GB-free guard (catches small clusters where `0.25×used <
20GB`). Applied **after** step-down-alignment so round-down can't re-violate it.
`math.ceil(floor/STEP)*STEP` is a **strict** round-up — if `floor` is already
step-aligned (e.g. 3190) it stays 3190, with no 10GB overshoot (R2-D-3; the earlier
`((floor//STEP)+1)*STEP` form over-shot by one step when already aligned). Verified
pc-bp101 path: used=2545.2 → floor `max(3182,2565.2)=3182` → round-up 3190 → 3190 <
3260 (stays a shrink) → 3190×0.8=2552 ≥ 2545.2 (passes, ~7GB margin).

### 2. `buffer_percent` default — UNCHANGED (105)
Decision (user): keep `buffer_percent=105` (`config.py:61`); do NOT raise to 125.
The §1 safe floor is the binding guarantee for non-compressed shrinks **regardless
of `buffer_percent`**: at 105, `raw_target = ceil(used×1.05)` ≈ used×1.05, well
below the floor `max(ceil(used×1.25), used+20)`, so the floor alone raises the
target to the safe level (~used×1.25). The expand path is untouched (still
`ceil(used×1.05)`) — no behavior change for expansions. (Supersedes the earlier
C5.3 raise-to-125 proposal; D3's buffer→expand interaction is now moot.)

### 3. Carry compression mode on the plan — `models.py` `ChangePlan` (~L98-117)
Add `compress_storage_mode: str | None = None`. Populate in `plan_changes`
(`executor.py:106-112`) from `cluster.compress_storage_mode`. Needed so
`_execute_single_change` can evaluate the non-compressed gate without re-fetching
every shrink.

### 4. Apply-time re-check — `executor.py::_execute_single_change`
For **non-compressed only** (gated on the plan-time `ChangePlan.compress_storage_mode`,
which is authoritative for the cycle — R2-D-5): re-fetch live used via
`client.get_cluster_detail` (exists on all client layers — `cloud_client.py:104/185`,
`aliyun_client.py:209`) and abort this attempt (retry next cycle) if
`target < max(ceil(used×1.25), used+20)`. On re-fetch **failure**: do NOT proceed to
`modify_storage` — treat as transient retry (R2-C1). QPS: one extra
`DescribeDBClusterAttribute` per non-compressed shrink only; confirm
`max_parallel×2 ≤ max_qps` (defaults `max_parallel_requests=5` at `config.py:60`,
`max_qps=10` at `config.py:66` → 5×2 ≤ 10 OK — R2-D-6) — else reuse plan-time detail
or raise `max_qps`.

### 5. Soft-skip the margin-rejection error — `errors.py` + `executor.py`
- **Detection** (D2-H1): in `_execute_single_change`'s Permanent handler
  (`executor.py:378-390`), match on **`error_code == 'InvalidParam'`** (primary),
  with message-pattern (`targetSize * 0.8` / `dataSize`) as a secondary signal.
  Verified path: the SDK **raises** on the 400 (the live "Permanent error on…" log
  line proves a raise, not a returned response), so it flows through
  `_handle_error`→`classify_sdk_error` (`cloud_client.py:295` default Permanent —
  `InvalidParam` matches no code set and is distinct from the codebase's
  `InvalidParameter` at `cloud_client.py:54`); `redact_sdk_error` **preserves the
  rich message** and `error_code='InvalidParam'` on the raised exception. Code-based
  detection is robust to the alternate `_check_response` path (`aliyun_client.py:402`,
  which also stamps `error_code` from `response.body.code`) — message-only would NOT
  be, since `_check_response` emits a generic "API request failed with status N".
- **Outcome model** (D2-H2): add `skipped: bool = False` to `ChangeResult`
  (`models.py`); `errors.py` keeps Transient/Permanent (no new category). The
  soft-skip handler returns `ChangeResult(plan=…, success=False, skipped=True,
  error=e)` and the branch condition is `if result.skipped:` (checked FIRST in the
  loop, before the success/failed split).
- **Aggregation** (R2-W3): the apply-mode loop (`executor.py:219-249`) currently
  routes only by `result.success` → add a first branch
  `if result.skipped: context.skipped.append(result)` and surface `total_skipped`
  in `ExecutionReport`.

### 6. Tests — `tests/`
- Floor math: pc-bp101 (used=2545.2, non-compressed) → 3190, passes 80%+20GB;
  small cluster (used=50) → `max(63,70)=70` (the +20 term dominates);
  floor ≥ current ⇒ `None` (skip).
- Compression gating: pc-bp1zyv (compress ON) keeps current path (no floor, no
  re-check), target unchanged.
- Soft-skip: an `InvalidParam … targetSize * 0.8` response routes to skipped, not
  failed.
- `ChangePlan.compress_storage_mode` populated by `plan_changes`.
- `buffer_percent` default stays **105** (unchanged); the §1 floor — not buffer —
  makes non-compressed shrinks safe (a buffer=105 non-compressed shrink still
  reaches the floor). Validation still rejects ≤100 / >300.

## Verification

```bash
uv run pytest tests/ -v --tb=short          # unit tests above
uv run mypy src/                             # types
uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/
```
End-to-end (read-only, no mutation): a `RUN_MODE=dry-run` run against the live
account should now plan pc-bp101 at ~3190GB (a safe shrink) — or skip it — instead
of the doomed 2660GB; pc-bp1zyv unchanged. A targeted `apply` on a non-compressed
cluster should succeed or soft-skip, never raise the `InvalidParam` 400.

## Convergence Record (solidforge cross-source-review)

- **Artifact**: this fix proposal. **Authority**: self-contained, grounded in live
  API data + source. **Tier**: short, cap=2 rounds. **Legs**: same-family
  (`solidforge:doc-reviewer`, primary) + different-family (DeepSeek `hetero_doc_review.py`).
- **Round 1**:
  - same-family: **2 Blockers** — B1 (floor must apply AFTER 10GB step alignment,
    else round-down re-violates it) · B2 (re-check must be compression-gated, else
    it aborts the compressed pc-bp1zyv shrink). + W1–W5 advisories (20GB guard;
    InvalidParam≠InvalidParameter + soft-skip wiring; QPS; floor redundancy /
    skip-vs-expand; citation ranges). All reconciled.
  - different-family: **0 Blockers**, 3 advisories — D1 (reversed floor/raw_target
    inequality) · D2 (`None` call-chain through `plan_changes`) · D3 (resolve
    buffer→expand, don't defer). All reconciled.
- **Round 2**:
  - same-family: **NO new Blocker (convergence signal)**. Re-derived pc-bp101 math
    end-to-end (target 3190, passes guard). + R2-W1 (qualify "floor≥raw_target
    always" to buffer≤125) · R2-W2 (`ChangePlan` needs `compress_storage_mode`) ·
    R2-W3 (aggregation loop needs a skipped branch) · R2-C1 (re-fetch-failure →
    transient retry). All reconciled.
  - different-family (deepseek + minimax, re-run with `HETERO_DOC_TIMEOUT=1200`
    after the first attempt stalled at the 600s default): **1 Blocker** — R2-D-1
    (three `used` values for pc-bp101 cited without provenance — a doc-consistency
    defect, NOT a design-correctness one) · + 8 advisory — R2-D-2 (MiB unit label)
    · R2-D-3 (floor round-up overshoot → `math.ceil`) · R2-D-4 (`None` compress
    default) · R2-D-5 (compress authoritative at plan time) · R2-D-6 (QPS default
    cites) · D2-H1 (detect via `error_code='InvalidParam'`, not message-only) ·
    D2-H2 (commit `skipped` flag + branch condition) · R2-D-7 (this very coverage
    gap, now closed). All reconciled.
- **Coverage**: C1 (data_size unexposed — grep-verified), C3 math (1.25 = inverse
  of ×0.8; 2723840=2660×1024; 95.2%=1/1.05) independently confirmed. Live-API
  empirical claims (C1/C2/C4) rest on observation, not source — disclosed; the C3
  "dataSize ≈ storage_used (non-compressed)" link is INFERRED (weakest link); the
  floor is conservative even if it slightly over-counts dataSize.
- **`substantive_converged`: true** — core claims coverage-verified; both legs × 2
  rounds ran; the single R2-different-family Blocker (R2-D-1) was a
  doc-consistency/provenance defect, fully reconciled; no UNRESOLVED Blocker
  remains. **Caveat**: a brand-new Blocker did appear in R2-different-family, so a
  strict reading would want one R3 confirmation round; given it was non-substantive
  (clarity, resolved) and we are at the short-doc cap, convergence is declared with
  that caveat — an optional R3 confirmation is the human's call.
  **Outcome-axis (is this the right requirement?)**: human — not claimed by this review.
