FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/enrichment/pyproject.toml services/enrichment/pyproject.toml
COPY services/enrichment/src/ services/enrichment/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/enrichment/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/enrichment/src/ src/

ENV PYTHONPATH=/app/src
