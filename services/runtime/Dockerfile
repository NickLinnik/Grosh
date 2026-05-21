FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/normalizer/pyproject.toml services/normalizer/pyproject.toml
COPY services/normalizer/src/ services/normalizer/src/
COPY services/pipeline/pyproject.toml services/pipeline/pyproject.toml
COPY services/pipeline/src/ services/pipeline/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/normalizer/ && \
    uv pip install --system --no-cache --no-sources services/pipeline/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/normalizer/src/ src/
COPY services/pipeline/src/ src/

ENV PYTHONPATH=/app/src
