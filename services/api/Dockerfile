FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY pyproject.toml .
COPY src/ src/

RUN uv pip install --system --no-cache .


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src/ src/

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "grosh_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
