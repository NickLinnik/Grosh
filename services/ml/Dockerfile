FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install uv

COPY shared/ shared/
COPY services/ml/pyproject.toml services/ml/pyproject.toml
COPY services/ml/src/ services/ml/src/

RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache torch --index-url https://download.pytorch.org/whl/cpu && \
    uv pip install --system --no-cache --no-sources services/ml/


FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY services/ml/src/ src/

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "grosh_ml.main"]
