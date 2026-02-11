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
