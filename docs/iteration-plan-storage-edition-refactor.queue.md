---
queue_version: v1
frozen_at: 2026-07-30
plan_ref: docs/design-storage-edition-refactor.md
authority_chain:
  - docs/design-storage-edition-refactor.md
  - docs/iteration-plan-storage-edition-refactor.md
status: frozen
---

# Plan Queue — iteration-plan

FROZEN plan interpretation emitted by blueprint-crafting `freeze`. Read-only for the executor; revise only via the Revision Channel (`status` -> `revising` -> edit + queue_version bump -> `status: frozen`). See parallel-development `references/plan-driven-mode.md`.

## Summary (checkpoint view)

5 item(s). DoD source: docs/design-storage-edition-refactor.md.

## Items

```json
[
  {
    "item_id": "ser-1",
    "seq": 0,
    "depends_on": [],
    "dod_ref": "docs/iteration-plan-storage-edition-refactor.md#ser-1",
    "title": "models: ClusterDetail.storage_max_gb + SENormal docstring",
    "scope": "Add optional storage_max_gb field; fix SENormal=Standard Edition semantics in docstring; note serverless_type is decision-orthogonal.",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "ser-2",
    "seq": 1,
    "depends_on": [
      "ser-1"
    ],
    "dod_ref": "docs/iteration-plan-storage-edition-refactor.md#ser-2",
    "title": "aliyun_client: parse StorageMax into storage_max_gb (unit-tested)",
    "scope": "In get_cluster_detail parse storage_max (byte->GB) into the new field; None when absent. DoD unit-testable: stubbed DescribeDBClusterAttribute response storage_max=109951162777600 -> storage_max_gb==102400; absent/0 -> None.",
    "parallel_group": "wave-1",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "ser-3",
    "seq": 2,
    "depends_on": [
      "ser-1"
    ],
    "dod_ref": "docs/iteration-plan-storage-edition-refactor.md#ser-3",
    "title": "strategy: category-based edition detection + API StorageMax cap + in-place rewrite of directly-broken tests",
    "scope": "_is_standard_edition on category SENormal (primary) OR ESSD storage (secondary); _is_expand_only only NormalMultimaster; effective_max_storage_gb(detail) used in compute AND validate (validate cap inserted AFTER shrink_safe_floor block, BEFORE the result==current no-op check); min/max tables add highperformance/standard keys; fix L212 log + comments. Rewrite the tests directly broken by this behavior change IN THIS ITEM (test_serverless_psl_shrink_blocked/expand_allowed, test_serverless_cluster_selected_but_shrink_blocked -> Standard-Edition-excluded semantics) and update test_empty_whitelist expected_count to dual-signal, so ser-3 is individually green. Add DIRECT unit tests for effective_max_storage_gb (set -> returns it; None -> fallback to get_max_storage_gb).",
    "parallel_group": "wave-1",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "ser-4",
    "seq": 3,
    "depends_on": [
      "ser-2",
      "ser-3"
    ],
    "dod_ref": "docs/iteration-plan-storage-edition-refactor.md#ser-4",
    "title": "tests: additive new coverage + fixture wiring",
    "scope": "conftest.cluster_data_to_detail add storage_max_gb; sample_clusters.json pc-sless description relabel (category unchanged). NEW cases only (compute_target_storage integration-level; direct effective_max_storage_gb unit tests live in ser-3): storage_max_gb cap, storage_max_gb=None fallback, _is_standard_edition SENormal+HighPerformance=True, HighPerformance/Standard min=10. Falsifiable DoD: no test asserts a literal present in STORAGE_TYPE_MIN_GB/MAX_GB; every asserted target derives from documented test inputs.",
    "parallel_group": "wave-2",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  },
  {
    "item_id": "ser-5",
    "seq": 4,
    "depends_on": [
      "ser-3"
    ],
    "dod_ref": "docs/iteration-plan-storage-edition-refactor.md#ser-5",
    "title": "docs: README + deployment edition/storage tables",
    "scope": "Rewrite README handling rules + cluster-type table + storage min/max table; deployment.md; edition detection via category, StorageMax authoritative. DoD: re-verify cited line ranges against current (uncommitted) README/deployment before editing; update scope if shifted.",
    "parallel_group": "wave-2",
    "blueprint_subset": [],
    "producer": "blueprint-crafting",
    "plan_model_version": "v1"
  }
]
```
