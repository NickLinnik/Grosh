# ADR: Consumer Pipeline Architecture

How raw bank events flow from the ingestion service through two independently-deployable consumer services to the `transactions` table.

---

## Problem

The transaction consumer started as a simple normalize→enrich→persist pipe. As the system grew, it accumulated layers with fundamentally different concerns:

- **Normalization** — map raw bank data to internal schema
- **Currency conversion** — multi-hop rate resolution
- **Transfer detection** — pair-matching with claim locks and anomaly recording
- **Classification** (future) — rule lookup, MCC fallback, ML embedding
- **Reprocessing** — replay historical data through the full pipeline

Each layer has a different relationship with source-specificity:

| Layer              | Source-specific? | Why                                                                          |
|--------------------|------------------|------------------------------------------------------------------------------|
| Normalization      | Always           | Each bank has its own API schema, field semantics, ID format                 |
| Transfer detection | Always           | Detection signals are bank-specific (Monobank MCC 4829, description patterns) |
| Currency conversion | No              | Source-agnostic by design. Per-source variation is data-driven via `rate_source_config` |
| Classification     | Probably mixed   | MCC mapping is universal; merchant name patterns may be bank-specific        |

A naive design — "normalize in ingestion and publish a unified `RawTransactionEvent`" — creates two problems:

1. **Information loss** — ingestion drops fields it deems useless. If a consumer later needs them, both services must be updated and redeployed.
2. **Fake abstraction** — a "unified" event is actually shaped like whichever bank the system was built around. The "source-agnostic" event is anything but.

---

## Architectural constraint: no information loss through the pipeline

Whatever the consumer persists must be sufficient to reprocess the data without information loss. This means:

- Persistence captures all meaningful fields from `NormalizedTransaction`.
- Anything dropped at normalization time is lost forever (recoverable only by re-fetching from the bank API).
- New consumer layers must extend storage if they require inputs that wouldn't survive a reprocess cycle.

This rule is what makes reprocessing possible without re-fetching. It constrains every layer's design: if you compute something from the raw payload that downstream layers or future reprocessing might need, you must persist it.

---

## Topology

### Per-source Kafka topics with raw bank payloads

Ingestion is a thin gateway:

- Validate webhook authenticity (or JWT, for user-facing manual entry).
- Resolve `user_id` and `account_id` via DB lookup — ingestion owns account records, so it's the natural place for account-shape validation.
- Publish the **raw bank payload** + routing envelope to a per-source topic.

```
raw_transactions.monobank   — MonobankStatementItem + routing metadata
raw_transactions.manual     — ManualTransactionPayload (user-supplied, already canonical)
raw_transactions.pumb       — (future)
raw_transactions.revolut    — (future)
```

**Routing envelope** (minimal, source-agnostic):

```python
class TransactionEnvelope(BaseModel):
    user_id: UUID
    account_id: UUID
    source: str
    payload: dict  # Raw bank payload, schema varies by source
```

The `payload` field is intentionally untyped at the Kafka boundary — each topic carries a different schema. **Validation happens in the normalization strategy:** the normalizer deserializes `payload` into the source-specific Pydantic model (e.g. `MonobankStatementItem(**envelope.payload)`). Malformed payloads fail at this point with a validation error (skip + commit + log).

**Kafka message key:** `str(user_id)` — preserves per-user ordering.

### Two-consumer architecture with intermediate normalized topic

```
raw_transactions.monobank ──┐
raw_transactions.manual   ──┼──> [Normalization Service] ──> normalized_transactions ──> [Enrichment Service]
raw_transactions.pumb     ──┘                                                              (transfer detection
raw_transactions.revolut  ──┘                                                               → conversion
                                                                                            → classification
                                                                                            → persistence)
```

**Two independently-deployable services**, connected by an intermediate Redpanda topic:

- **Normalization service** — subscribes to all `raw_transactions.*` topics, dispatches to per-source `NormalizationStrategy`, publishes `NormalizedTransaction` to `normalized_transactions`. Also owns the staging buffer and the reprocess job — every producer of `normalized_transactions` lives here.
- **Enrichment service** — pure consumer of `normalized_transactions`. Runs transfer detection → currency conversion → classification → persistence.

**Why two stages:**

- **Reprocessing is source-agnostic.** The reprocess job publishes `NormalizedTransaction` directly to `normalized_transactions`. One format regardless of how many banks exist. No per-source reconstruction logic, no linear growth with bank count.
- **Clear failure domains.** A normalization bug (bad field mapping) vs. an enrichment bug (wrong rate lookup) are different failure classes with different remediation. Normalization bugs require re-fetch from bank API; enrichment bugs are fixed by reprocessing.
- **Independent scaling.** Normalization is CPU-cheap (field mapping). Enrichment is IO-heavy (DB queries for rates, pair-matching). Different resource profiles, naturally separate.
- **Operational isolation.** A normalization-loop crash doesn't take down enrichment, and vice versa.

**Re-fetch is a sibling operation, not a reprocess variant.** Bugs in the normalization layer mean the stored data is already wrong; reprocess can't fix it. The source of truth (raw bank data) must be re-fetched from the API. Re-fetch is out of scope for this ADR but acknowledged as a known operational need.

### Layered enrichment with Strategy pattern per layer

Each processing layer defines a Strategy protocol. The enrichment service runs layers in sequence. Each layer decides independently whether it needs per-source dispatch or can be source-agnostic.

```
normalized_transactions
        │
        ▼
┌─────────────────┐
│  Transfer        │  ← Per-source (MonobankTransferDetection, or no-op for sources
│  Detection       │     that provide explicit transfer flags)
└────────┬────────┘
         ▼
┌─────────────────┐
│  Currency        │  ← Source-agnostic by design. Per-source rate variation is
│  Conversion      │     data-driven via `rate_source_config` rows, not code dispatch.
└────────┬────────┘
         ▼
┌─────────────────┐
│  Classification  │  ← Mixed: shared MCC table + per-source merchant patterns (future)
└────────┬────────┘
         ▼
┌─────────────────┐
│  Persistence     │  ← Source-agnostic (single repo, unified DB schema)
└─────────────────┘
```

**Why this layer order:** Conversion and transfer detection are independent — neither reads the other's output. The order between them is free. Transfer detection runs first because it sets `special_category`, which classification later branches on (transfers are excluded from category assignment). Conversion's output is also classifier input. Both must complete before classification. Persistence is always terminal.

### Strategy protocols

```python
class NormalizationStrategy(Protocol):
    def normalize(self, envelope: TransactionEnvelope) -> NormalizedTransaction: ...

class TransferDetectionStrategy(Protocol):
    async def detect_and_pair(
        self, conn: asyncpg.Connection, tx: NormalizedTransaction, own_accounts: list[Account]
    ) -> TransferResult: ...

class ClassificationStrategy(Protocol):  # future
    async def classify(
        self, conn: asyncpg.Connection, tx: NormalizedTransaction
    ) -> ClassificationResult: ...
```

Each strategy has a registry: `dict[str, Strategy]`. A layer with no per-source strategy for the given source either:

- Falls through to a default implementation, OR
- Is skipped (e.g. no transfer detection for a source that marks transfers explicitly in the payload — the normalizer sets `special_category = 'transfer'` directly).

The Strategy pattern is justified by the explicit multi-bank roadmap (PUMB and Revolut planned). The cost is small (a protocol + registry dict per layer); the benefit pays out on the third bank without refactoring.

### NormalizedTransaction is the inter-service contract

`NormalizedTransaction` carries every field needed downstream. Per-source raw models (e.g. `MonobankStatementItem`) live in each source's module; `NormalizedTransaction` is the contract between the normalization service (producer) and the enrichment service (consumer).

---

## Cohesion rule

"Which service owns each module?" is answered by **"who produces events on `normalized_transactions`?"**

The normalization service owns every producer of that topic:

- **Steady-state normalization** — subscribes to `raw_transactions.*`, normalizes, publishes.
- **Staging drain** — periodic sweep of `staging_normalized_transactions` rows whose `user_id` no longer holds a reprocessing lock; republishes them to `normalized_transactions`. Same long-running process as steady-state.
- **Reprocess job** — K8s Job that reads stored `transactions`, reconstructs `NormalizedTransaction` via the inverse mapping, deletes originals, republishes, polls for catchup. Same image, different entrypoint.

The enrichment service owns the **single consumer** of `normalized_transactions` plus the downstream enrichment layers. It knows nothing about reprocess, staging, or normalization.

The reprocess job belongs to the normalization service because it republishes events to `normalized_transactions` — it does not invoke enrichment layers itself; the enrichment service picks the republished events up through its normal consumer path. This is cohesion by **responsibility** ("who produces normalized events?") rather than by table-touch.

---

## Layout

```
services/normalization/
  src/grosh_normalization/
    consumers/normalization_consumer.py     — raw → normalized
    services/staging_drain_service.py       — staged → normalized (periodic sweep + LISTEN/NOTIFY)
    services/reprocess_orchestrator.py      — snapshot + delete + replay + verify + restore
    sources/{monobank,manual}/normalizer.py — per-source normalization strategies
    repositories/{staging,reprocessing_locks,reprocessing_backups,transactions_read}.py
    main.py                                 — long-running entrypoint (normalization consumer + staging drain)
    reprocess_main.py                       — K8s Job entrypoint

services/enrichment/
  src/grosh_enrichment/
    consumers/enrichment_consumer.py        — the only reader of `normalized_transactions`
    services/enrichment_orchestrator.py     — layer dispatch
    layers/{transfer_detection,currency_conversion,persistence}/
    sources/{monobank,manual}/transfer/     — per-source transfer-detection strategies
    repositories/transactions.py            — write-owner of `transactions` (INSERT/UPDATE)
    main.py                                 — long-running entrypoint
```

---

## Image and deployment strategy

**One Docker image, multiple entrypoints.** The `grosh-consumer:latest` image carries both service trees. Compose and k3s manifests pick the entrypoint per service.

The three entrypoints are fixed by spec — Dockerfiles declare no `CMD`/`ENTRYPOINT` that would silently fall through to the wrong service; every manifest specifies the entrypoint explicitly:

- **Normalization steady state:** `python -m grosh_normalization.main` (long-running). Runs the normalization consumer loop + the staging drain background task.
- **Enrichment:** `python -m grosh_enrichment.main` (long-running). Runs the enrichment consumer loop.
- **Reprocess K8s Job:** `python -m grosh_normalization.reprocess_main` (one-shot). Reads target user list from the `USER_IDS_JSON` env var (JSON array of UUIDs as strings) and orchestrates snapshot + delete + replay + verify + restore per user.

The trade-off — image carries both trees and is slightly larger than a split-image approach — is irrelevant at this scale (a few extra MB). The benefit is concrete: `make dev-k8s-setup` runs one `docker save | ctr images import` cycle instead of two.

If image size ever matters, splitting into two images is a one-day refactor: separate `pyproject.toml` files, two Dockerfiles, two image tags. The migration cost is bounded.

---

## Service ownership rules

| Table/Resource | Write owner      | Read access | RLS enforced? |
|---------------|------------------|-------------|---------------|
| `accounts`    | Ingestion service | All services | Yes (user-facing) |
| `transactions` | Enrichment (INSERT/UPDATE) + Normalization (DELETE on reprocess) | All services | Yes (user-facing) |
| `users`       | API service (most cols) + Ingestion (`last_reprocess_started_at` only) | All services | Yes (user-facing) |
| `currency_rates` | Ingestion service | All services | No (reference data) |
| `transfer_match_anomalies` | Enrichment (INSERT/auto-resolve DELETE) | All services | Yes |
| `reprocessing_locks` | Ingestion (INSERT) + Normalization (DELETE) | All services | Yes |
| `reprocessing_backups` | Normalization (reprocess job) | All services | Yes |
| `staging_normalized_transactions` | Normalization | All services | No (operational queue) |

User-facing services (API, ingestion) enforce RLS via `set_config('app.current_user_id', ...)`. The consumer services operate above RLS — they're background processors with full table access, processing events on behalf of all users.

### Single-writer rule carve-outs

The two-service split introduces two exceptions to CLAUDE.md's "single writer per table" invariant:

- **`transactions`** is co-written: enrichment is the only writer that INSERTs new rows (`ON CONFLICT (id) DO NOTHING`) and performs in-place UPDATEs (during transfer pair claiming); normalization performs the bulk DELETE during a reprocess job (snapshot → DELETE → republish).
- **`reprocessing_locks`** is co-written: ingestion INSERTs the lock row inside its trigger transaction (before submitting the K8s Job); normalization DELETEs the row on completion. Neither service UPDATEs the row — it has no mutable state.

Each carve-out is bounded to a single SQL command per service so the invariant remains auditable. The carve-out grep test (`tests/e2e/integration/test_single_writer_carveout.py`) walks the source trees and asserts: no `INSERT INTO transactions` or `UPDATE transactions` in `services/normalization/`; no `DELETE FROM transactions` in `services/enrichment/`.

### Database role

Both consumer services connect as the existing `grosh_consumer` role. The carve-out is enforced by code review and integration tests, not by DB grants. Rationale: splitting the role into `grosh_normalization` + `grosh_enrichment` with scoped grants would be a stronger guarantee but requires a new migration, two new role credentials in Infisical, and a Compose/k3s rewire — disproportionate cost at single-node 3-user scale. The split-role option is a documented follow-up if a future incident reveals convention drift.

The `reprocessing_locks` carve-out, in contrast, IS enforced by DB grants (migration 0012): `grosh_ingestion` gets `INSERT`, `grosh_consumer` keeps `DELETE`. The grant split was cheap there because both roles already existed and the column-level grants didn't need a new migration cycle.

---

## TransactionRow in shared

`shared/src/grosh_shared/domain/normalized.py` carries both `NormalizedTransaction` (the Kafka contract) and `TransactionRow` (a persistence-row shape with column names matching the `transactions` table), plus `TransactionRow.to_normalized()` — the inverse mapping that strips `metadata.layer` and preserves `metadata.source`.

This co-location is a deliberate exception to "shared is schema-free." The reprocess flow inside the normalization service needs the inverse mapping to reconstruct events from stored rows during replay. The enrichment service's persistence layer already encodes the forward mapping. Placing both shapes in shared keeps them in sync without forcing a normalization-imports-enrichment dependency.

**Trade-off.** A future `transactions` schema change touches one extra file (`shared/.../domain/normalized.py`) alongside the migration and `enrichment/.../transaction_repo.py`. This is acceptable because the change set is small and locally co-located.

**The API-service no-unify rule.** `services/api/src/grosh_api/repositories/transaction_repo.py` has its own local `TransactionRow` dataclass for read-only HTTP response shapes (`GET /v1/transactions`). It does NOT import the shared `TransactionRow` and the two must not be unified. The API's version is a query-result row; the shared version is the reprocess inverse-mapping shape. Unifying them would expand the schema-awareness exception to a third service that doesn't need reprocess. Enforced by `tests/e2e/integration/test_single_writer_carveout.py` plus code review.

**Revisit trigger.** If a future change requires a third service to import `TransactionRow` without needing the reprocess inverse mapping, extract `to_normalized()` into a normalization-private module first and let only `NormalizedTransaction` stay in shared.

---

## Implications for other ADRs

### Transfer detection (`adr-transfer-detection.md`)

The Monobank transfer detection strategy receives a `NormalizedTransaction` as input. Its output is consumed by the enrichment orchestrator, which writes results to `transactions` and `transfer_match_anomalies`. The strategy itself is bank-specific business logic — see that ADR for the algorithm.

### Transaction reprocessing (`adr-transaction-reprocessing.md`)

Reprocessing publishes `NormalizedTransaction` to `normalized_transactions` — one format, source-agnostic. The reprocess job reconstructs `NormalizedTransaction` from stored DB columns (all meaningful fields are persisted per the information-loss rule). No per-source reconstruction logic needed. See that ADR for the full reprocess flow including the staging buffer and lock model.

---

## What this ADR does NOT decide

- **Classification layer design** — deferred until ML spec is written. The layer slot exists; the strategy protocol is defined; position and inputs are committed; implementation waits.
- **Consumer scaling** — single consumer instance per Kafka partition is sufficient at ~3 users. If horizontal scaling becomes necessary, increase the partition count rather than spawning multiple instances per partition (per-user ordering is preserved because each user's events always hash to the same partition).
- **Re-fetch mechanism** — when a normalization bug corrupts stored data, the fix is re-fetching from bank APIs. This is a sibling operation to reprocessing, not a variant of it. Out of scope.
