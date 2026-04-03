FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/consumer/pyproject.toml services/consumer/pyproject.toml
COPY services/consumer/src/ services/consumer/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/consumer/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/consumer/src/ src/

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "grosh_consumer.main"]
