# PolarDB Storage Resizer

继承: `~/.cursor/rules/`, `.cursor/rules/` 下的 *.mdc 规则，并以文件中 frontmatter 的定义来判定是否应用到本项目。

## Python

异常: 访问属性用 `getattr(e, 'message', str(e))`，禁止 `e.message`。重新抛出用 `raise X from e`。

类型: SDK 类型导入放 `TYPE_CHECKING` 块。参数类型禁止 `object`，用具体类型如 `logging.Logger`。

测试: conftest.py 导入实际模块，禁止重复定义 dataclass。

## 配置

YAML 嵌套逐层解析：`data.get("safety", {}).get("max_expand_ratio", default)`

## 检查项

YAML嵌套解析 | raise...from e | getattr替代.message | TYPE_CHECKING块

Important: 测试导入实际模块 | 参数具体类型

## 命令

测试: uv run pytest tests/ -v --tb=short
运行: set -a; source .env; set +a && uv run python -m polardb_storage_resizer.main
类型检查: uv run mypy src/
格式化: uv run ruff format src/ tests/
lint: uv run ruff check src/ tests/

## L1 Constitution (uncodable red lines)

Red lines that cannot be encoded as deterministic architecture-contract rules live here. These are Blockers: a violation returns the work for rewrite. Codable red lines (circular dependencies, layer isolation, concurrency baselines) are enforced deterministically by the inner Architecture-Contract Gate — do not duplicate them here; declare them in the project's arch-contract config (.importlinter.ini / .dependency-cruiser.cjs / .swiftlint.yml).

- Abstraction level must be appropriate: a helper must not leak domain logic into a generic utility, and a high-level policy must not reach into a low-level primitive directly.
- Naming must reflect intent, not implementation accident. A name that contradicts what the code does is a Blocker.
- No emergent coupling: two modules that are not explicitly wired must not secretly depend on each other's internal behavior or ordering.
- No "delete the error" fixes: removing a failing module, hardcoding a value to turn a test green, or wrapping logic in a bare catch to silence a failure are Blockers (the fast gate + blueprint diff catch most of these).
- All authentication/authorization that cannot be statically proven to flow through the unified gateway is a Blocker.

When a Reviewer flags one of these, the convergence loop treats it as an outer- ring Blocker and returns the change for rewrite — not a Warning.

## Deterministic Gate Toolchain

The convergence-loop gates degrade gracefully and never report a silent green when a tool is absent. To arm them on a new machine or in CI, restore/install the gate tools for the ecosystems this project uses:

- Python: `uv sync` — ruff / import-linter / pylint are in dev deps (uv.lock)
