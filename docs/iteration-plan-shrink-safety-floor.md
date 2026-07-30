# Iteration Plan — PolarDB Shrink 80%-Safety-Floor Fix

> **SUPERSEDED (2026-07-30)**: the work items below shipped in 05b6e3b and were
> subsequently REMOVED — the trigger case (80%/20 GB rejection on an Enterprise
> PSL cluster) did not hold up: the official doc scopes the guard to Standard
> Edition ESSD only, and a manual 3260 → 2620 GB shrink on the trigger cluster
> succeeded at ~97% utilization. See the superseded banner in
> `design-shrink-safety-floor.md` and the removal commit
> (`revert: remove shrink safety floor`). Kept for historical record only.

> Authority: defers to `docs/design-shrink-safety-floor.md` (cross-source-reviewed,
> substantive-converged). On conflict, the design doc wins. Produced via
> `solidforge:blueprint-crafting` for `solidforge:parallel-development` to execute.

## Goal (per iteration)

Non-compressed PolarDB shrinks never emit an API-rejectable plan; compressed clusters
keep working; doomed shrinks soft-skip instead of hard-failing. `buffer_percent` stays
at its current default (105) — the safe floor, not the buffer, is the guarantee.

## Work items (complexity-tiers)

| item_id | seq | tier | title | depends_on |
|---|---|---|---|---|
| SF-1 | 1 | M | Non-compressed shrink 80%/20GB safe floor in `validate_storage_constraints` | — |
| SF-2 | 2 | S | `models.py` fields: `ChangePlan.compress_storage_mode` + `ChangeResult.skipped`; populate in `plan_changes` | — |
| SF-3 | 3 | M | Compression-gated apply-time re-check (`+ import math`) **+ skipped→`total_skipped` aggregation routing** | SF-1, SF-2 |
| SF-4 | 4 | S | Soft-skip the `InvalidParam` margin error — **detection only** (reuses SF-2 field + SF-3 routing) | SF-1, SF-3 |
| SF-5 | 5 | M | Unit tests (floor math, gating, skip, fields, buffer default) | SF-1..SF-4 |

Complexity tiers (S/M/L/XL) per `blueprint-crafting` iteration-plan §1. SF-2 is S
(single dataclass field + one populate site); SF-1/3/4/5 are M (multi-site logic +
tests).

## Dependency-edges / DAG

```
SF-1 (floor) ──┬──> SF-3 (re-check) ──> SF-4 (soft-skip) ──> SF-5 (tests)
SF-2 (field) ──┘
```

- wave-1 (parallel): SF-1 (`strategy.py`), SF-2 (`models.py` both fields + `executor.plan_changes` L106-112) — disjoint files/sites.
- wave-2: SF-3 (executor `_execute_single_change` re-check + `import math` + aggregation-loop routing) — needs SF-2's `skipped` field so the abort is self-consistent at Gate B (novel-5).
- wave-3: SF-4 (executor `_execute_single_change` Permanent handler, detection only) — serialized after SF-3 (same function, disjoint line ranges L312 vs L378-390).
- wave-4: SF-5 (tests all).

## Per-iteration DoD (per-iteration-dod)

- **SF-1**: in `validate_storage_constraints` (`strategy.py:322-399`), shrink branch,
  AFTER 10GB step align, for `compress_storage_mode != "ON"`: `floor = max(ceil(used*1.25), used+20)`,
  `floor = math.ceil(floor/STORAGE_STEP_GB)*STORAGE_STEP_GB`, `result = max(result, floor)`;
  `return None` if `result >= current`. pc-bp101 path ⇒ 3190, passes 80%+20GB.
- **SF-2**: `models.py` — add `compress_storage_mode: str | None = None` to `ChangePlan`
  AND `skipped: bool = False` to `ChangeResult`; `plan_changes` (`executor.py:106-112`)
  sets `compress_storage_mode` from `cluster.compress_storage_mode`. Both fields land here
  (wave-1) so SF-3 can build `ChangeResult(skipped=True)` (novel-5).
- **SF-3**: add `import math` to `executor.py`. In `_execute_single_change`, non-compressed
  only (gated on the plan-time field), re-fetch via `client.get_cluster_detail`; if
  `target < max(ceil(used×1.25), used+20)` → abort this cycle returning
  `ChangeResult(success=False, skipped=True)`. **Add `if result.skipped: context.skipped.append(result)`
  FIRST in the aggregation loop** (`executor.py:219-249`) so skipped results route to
  `total_skipped` (field from SF-2; `ExecutionReport.total_skipped`/`skipped` already exist,
  `models.py:189,193`). On re-fetch error → transient retry (no modify). Confirm
  `max_parallel×2 ≤ max_qps`.
- **SF-4**: in `_execute_single_change` Permanent handler (`executor.py:378-390`), detect
  `error_code == 'InvalidParam'` (msg `targetSize * 0.8` secondary) → return
  `ChangeResult(success=False, skipped=True)` **logged at INFO** (not the L380 `logger.error`).
  Detection only — reuses the `skipped` field (SF-2) + aggregation routing (SF-3); adds no
  new field/branch.
- **SF-5**: pytest cases per design §6 — floor math (pc-bp101→3190; small used=50→70;
  floor≥current→None); **post-cap `result×0.8 ≥ used` guard (B1)**; compression gating
  (compress ON unchanged, B2); soft-skip routing (InvalidParam→skipped, logged INFO);
  **SF-3 floor-violation abort → `total_skipped`**; `ChangePlan.compress_storage_mode`
  populated; `buffer_percent` default still 105.

## Phase acceptance gates (phase-acceptance-gates)

- Gate A (post wave-1): `uv run mypy src/` clean; `uv run ruff check src/ tests/` clean;
  SF-1/SF-2 unit smoke passes.
- Gate B (post wave-2): full `uv run pytest tests/ -v --tb=short` green; mypy + ruff clean.
- Gate C (post wave-3 / release): `RUN_MODE=dry-run` against the live account plans
  pc-bp101 at ~3190GB (or skips it) — never 2660GB; pc-bp1zyv unchanged.

## Risks and mitigations (risks-mitigations)

- **Floor undone by later caps** (B1): apply floor AFTER step align; SF-5 asserts the
  post-cap result still satisfies `result×0.8 ≥ used`.
- **Re-check aborts compressed shrinks** (B2): gate strictly on `compress_storage_mode != "ON"`;
  SF-5 asserts pc-bp1zyv path unchanged.
- **Soft-skip mis-detection** (D2-H1): detect on `error_code == 'InvalidParam'` (robust to
  both `_check_response` and `classify_sdk_error` paths), message secondary.
- **QPS burst** (W3/R2-D-6): one extra Describe per non-compressed shrink; verify
  `max_parallel×2 ≤ max_qps` (5×2 ≤ 10).
- **`None` compress mode** (R2-D-4): treated as non-compressed (conservative — may over-block,
  never under-protects); documented.
- **Stale `used` (dataSize ≈ storage_used inferred)**: the floor is conservative even if
  storage_used slightly over-counts dataSize.

## Out of scope (out-of-scope)

- Changing `buffer_percent` default (stays 105 — user decision; the floor is the guarantee).
- Compressed-cluster planning changes (current `compress_storage_used` path is correct).
- Reading a `data_size` field (not exposed by the API — confirmed `None`).
- Any expand-path behavior change.

## Cross-cutting tasks (cross-cutting-tasks)

- `models.py`: two field additions (`ChangePlan.compress_storage_mode`, `ChangeResult.skipped`)
  — coordinate so both land before SF-5.
- `executor.py`: touched by SF-2 (`plan_changes` L106-112), SF-3 (`_execute_single_change`
  re-check + `import math` + aggregation-loop routing L219-249), SF-4 (`_execute_single_change`
  Permanent handler L378-390, detection only). The DAG serializes SF-3→SF-4 (same function,
  disjoint line ranges); SF-2's `plan_changes` site is disjoint (wave-1). `ExecutionReport`
  already has `total_skipped`/`skipped` (`models.py:189,193`).
- Redaction: the soft-skip path reuses existing `redact_sdk_error`/`redact_request_id` (no new
  secret handling).
- Logging: soft-skips (SF-3 abort + SF-4 InvalidParam) log at INFO (not ERROR), consistent with
  the dry-run/shutdown skip logs.
