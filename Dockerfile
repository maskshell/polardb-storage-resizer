# =============================================================================
# PolarDB Storage Resizer - Multi-stage Docker Build
# =============================================================================
FROM python:3.12-slim AS builder

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ src/

RUN pip install --no-cache-dir uv && \
    uv export --frozen --no-dev --no-editable --no-emit-project > requirements.txt && \
    pip install --no-cache-dir -r requirements.txt

# =============================================================================
FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src/ src/

ENV PYTHONPATH=/app/src

RUN groupadd -r app && useradd -r -g app -d /app -s /sbin/nologin app && \
    mkdir -p /tmp /var/cache/resizer && \
    chown -R app:app /tmp /var/cache/resizer

USER app

ENTRYPOINT ["python", "-m", "polardb_storage_resizer.main"]
