# PolarDB Storage Resizer - local CI commands

# Run all CI checks (mirror of .github/workflows/ci.yml)
ci: lint test helm-lint

# Lint: ruff check + format check + mypy (mirrors CI lint job)
lint:
    uv sync --frozen --all-extras
    uv run ruff check src/ tests/
    uv run ruff format --check src/ tests/
    uv run mypy src/

# Run tests (mirrors CI test job)
test:
    uv sync --frozen --all-extras
    uv run pytest tests/ -v --tb=short

# Helm chart lint (mirrors CI helm-lint job)
helm-lint:
    helm lint ./charts/polardb-storage-resizer/ --set image.repository=test/polardb

# Build Docker image locally (single-arch, no push)
docker-build:
    docker build -t polardb-storage-resizer:local .
