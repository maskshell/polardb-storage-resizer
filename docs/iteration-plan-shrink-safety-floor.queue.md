---
queue_version: v1
frozen_at: 2026-07-29
plan_ref: docs/design-shrink-safety-floor.md
authority_chain:
  - docs/design-shrink-safety-floor.md
  - docs/iteration-plan-shrink-safety-floor.md
status: frozen
---

# Plan Queue — iteration-plan

FROZEN plan interpretation emitted by blueprint-crafting `freeze`. Read-only for the executor; revise only via the Revision Channel (`status` -> `revising` -> edit + queue_version bump -> `status: frozen`). See parallel-development `references/plan-driven-mode.md`.

## Summary (checkpoint view)

5 item(s). DoD source: docs/design-shrink-safety-floor.md.

## Items

```json
[
  {
    "item_id": "SF-1",
    "seq": 1,
    "depends_on": [],
    "dod_ref": "docs/iteration-plan-shrink-safety-floor.md#SF-1",
    "title": "Non-compressed shrink 80%/20GB safe floor",
    "scope": "In validate_storage_constraints shrink branch (after 10GB step align), for compress_storage_mode != 'ON', enforce floor = max(ceil(used*1.25), used+20) rounded up via math.ceil; return None if floor >= current.",
    "source_location": "design-shrink-safety-floor.md §1; strategy.py:322-399 (math already imported at strategy.py:13)",
    "parallel_group": "wave-1",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "SF-2",
    "seq": 2,
    "depends_on": [],
    "dod_ref": "docs/iteration-plan-shrink-safety-floor.md#SF-2",
    "title": "models.py field additions (ChangePlan.compress_storage_mode + ChangeResult.skipped)",
    "scope": "Add compress_storage_mode: str | None = None to ChangePlan AND skipped: bool = False to ChangeResult (models.py); populate compress_storage_mode in plan_changes (executor.py:106-112) from cluster.compress_storage_mode. BOTH dataclass fields land here (wave-1) so SF-3 (wave-2) can construct ChangeResult(success=False, skipped=True) and route it without depending on SF-4 (novel-5).",
    "source_location": "design-shrink-safety-floor.md §3 + §5(outcome model); models.py ChangePlan + ChangeResult",
    "parallel_group": "wave-1",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "SF-3",
    "seq": 3,
    "depends_on": [
      "SF-1",
      "SF-2"
    ],
    "dod_ref": "docs/iteration-plan-shrink-safety-floor.md#SF-3",
    "title": "Compression-gated apply-time re-check + skipped aggregation routing",
    "scope": "Add `import math` to executor.py. In _execute_single_change, non-compressed only (gated on plan-time ChangePlan.compress_storage_mode), re-fetch live used via client.get_cluster_detail; if target < max(ceil(used*1.25), used+20) abort this cycle returning ChangeResult(success=False, skipped=True). Add 'if result.skipped: context.skipped.append(result)' branch FIRST in the aggregation loop (executor.py:219-249) so skipped results route to total_skipped (ExecutionReport.total_skipped + skipped list already exist, models.py:189,193). On re-fetch error treat as transient retry (no modify).",
    "source_location": "design-shrink-safety-floor.md §4 + §5(aggregation); executor.py:9-15, 265-315, 219-249",
    "parallel_group": "wave-2",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "SF-4",
    "seq": 4,
    "depends_on": [
      "SF-1",
      "SF-3"
    ],
    "dod_ref": "docs/iteration-plan-shrink-safety-floor.md#SF-4",
    "title": "Soft-skip the InvalidParam margin error (detection only)",
    "scope": "In _execute_single_change Permanent handler (executor.py:378-390), detect error_code == 'InvalidParam' (msg 'targetSize * 0.8' secondary) -> return ChangeResult(success=False, skipped=True) logged at INFO (not the L380 logger.error). REUSES ChangeResult.skipped (SF-2) + the aggregation routing (SF-3) — adds no new field/branch. Serialized after SF-3 (both edit _execute_single_change, disjoint line ranges L378-390 vs L312).",
    "source_location": "design-shrink-safety-floor.md §5(detection); executor.py:378-390",
    "parallel_group": "wave-3",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "SF-5",
    "seq": 5,
    "depends_on": [
      "SF-1",
      "SF-2",
      "SF-3",
      "SF-4"
    ],
    "dod_ref": "docs/iteration-plan-shrink-safety-floor.md#SF-5",
    "title": "Unit tests for floor, gating, soft-skip, fields, buffer default",
    "scope": "pytest cases: floor math (pc-bp101 used=2545.2 -> 3190; used=50 -> 70; floor>=current -> None); post-cap result*0.8 >= used (B1 guard); compression gating (compress ON unchanged, B2 guard); SF-3 floor-violation abort -> total_skipped; SF-4 InvalidParam -> skipped, logged INFO; ChangePlan.compress_storage_mode populated; ChangeResult.skipped default False; buffer_percent default still 105.",
    "source_location": "design-shrink-safety-floor.md §6; tests/",
    "parallel_group": "wave-4",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  }
]
```
