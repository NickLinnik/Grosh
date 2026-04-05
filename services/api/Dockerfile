FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/api/pyproject.toml services/api/pyproject.toml
COPY services/api/src/ services/api/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/api/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/api/src/ src/

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "grosh_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
