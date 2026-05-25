# Tasks: Transaction Ingestion Pipeline

**Shipped.** The pipeline is in production on `main`. The functional spec and the technical considerations describe the resulting system; this file no longer tracks per-slice work.

For per-commit history:

    git log --oneline 003-transaction-ingestion-pipeline -- services/ shared/

## Work landed outside the slice framework

A handful of cross-cutting items shipped on this branch but were not framed as AWOS slices, so they wouldn't be discoverable from the spec alone:

- **E2E test stack** (`tests/e2e/services/`, `infra/docker-compose.test.yml`) — a separate Compose project (`grosh-test`) bound to ports 8010/8011 that runs in parallel with the dev stack. Each test session truncates user-scoped tables and recreates Kafka topics on the test cluster; no container churn between sessions.
- **Pre-push hook + CI E2E job** (`scripts/pre-push-e2e.sh`, `.github/workflows/ci-e2e.yml`) — smart-skip pre-push hook (runs E2E only when backend changes are pushed) plus a CI workflow that brings up the test stack and runs the full suite on every PR.
- **Test tree mirrors source tree** — `services/{api,ingestion,normalization,enrichment}/tests/{unit,integration}/` files are organized to mirror the source layout. Documented as a convention in `CLAUDE.md`.
