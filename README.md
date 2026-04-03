# Grosh

Self-hosted family finance platform — automated transaction ingestion, intelligent classification, and cash flow forecasting for a small family network.

Replaces a manual spreadsheet workflow with a real-time pipeline that ingests transactions from Monobank via webhook, classifies them automatically, and forecasts cash flow 90 days to 1 year forward.

## Monorepo Layout

| Path | What it does |
|---|---|
| `services/api/` | FastAPI webhook receiver and REST API |
| `services/consumer/` | Redpanda consumer — transaction enrichment and classification pipeline |
| `services/ml/` | Classifier (sentence-transformers + pgvector) and Prophet-based forecasting |
| `services/frontend/` | Next.js App Router UI |
| `shared/` | pip-installable Pydantic models shared across Python services |
| `infra/` | Docker Compose, Terraform (Hetzner), k3s manifests |

## Quick Start

```bash
# First time only: install all Python and Node dependencies
make setup

# Copy env template and fill in values
cp infra/.env.example infra/.env

# Start all services
make dev
```

See the [architecture](context/product/architecture.md) and [roadmap](context/product/roadmap.md) for full context.

## Docs

- [Product Definition](context/product/product-definition.md)
- [Architecture](context/product/architecture.md)
- [Roadmap](context/product/roadmap.md)
