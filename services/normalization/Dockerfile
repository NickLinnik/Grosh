FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/normalization/pyproject.toml services/normalization/pyproject.toml
COPY services/normalization/src/ services/normalization/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/normalization/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/normalization/src/ src/

ENV PYTHONPATH=/app/src
