FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/ingestion/pyproject.toml services/ingestion/pyproject.toml
COPY services/ingestion/src/ services/ingestion/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/ingestion/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/ingestion/src/ src/

ENV PYTHONPATH=/app/src

EXPOSE 8001

CMD ["uvicorn", "grosh_ingestion.main:app", "--host", "0.0.0.0", "--port", "8001"]
