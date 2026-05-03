# ADR: Consumer Pipeline Architecture

Status: **Draft** — designed, not yet implemented.

---

## Problem

The transaction consumer started as a simple normalize→enrich→persist pipe. As the system grows, it accumulates layers with fundamentally different concerns:

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
| Currency conversion | No              | Source-agnostic by design. Per-source rate variation is data-driven via `rate_source_config` |
| Classification     | Probably mixed   | MCC mapping is universal; merchant name patterns may be bank-specific        |

The current design normalizes in ingestion and publishes a "unified" `RawTransactionEvent`. This creates two problems:

1. **Information loss** — ingestion drops fields it deems useless (e.g. `originalMcc`). If the consumer later needs them, both services must be updated and redeployed.
2. **Fake abstraction** — `RawTransactionEvent` is shaped like Monobank's data. If PUMB has different fields, we either bloat the shared schema or lose data. The "source-agnostic" event is actually Monobank-shaped.

---

## Architectural constraint: no information loss through the pipeline

Whatever the consumer persists must be sufficient to reprocess the data without information loss. This means:

- Persistence captures all meaningful fields from `NormalizedTransaction`.
- Anything dropped at normalization time is lost forever (recoverable only by re-fetching from the bank API).
- New consumer layers must extend storage if they require inputs that wouldn't survive a reprocess cycle.

This rule is what makes reprocessing possible without re-fetching. It constrains every layer's design: if you compute something from the raw payload that downstream layers or future reprocessing might need, you must persist it.

---

## Decision

### 1. Per-source Kafka topics with raw bank payloads

Ingestion becomes a thin gateway:
- Validate webhook authenticity
- Resolve `user_id` and `account_id` (DB lookup — ingestion owns account records, so it's the natural place for account-shape validation)
- Publish the **raw bank payload** + routing envelope to a per-source topic

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

The `payload` field is intentionally untyped at the Kafka boundary — each topic carries a different schema. **Validation happens in the normalization strategy:** the normalizer deserializes `payload` into the source-specific Pydantic model (e.g. `MonobankStatementItem(**envelope.payload)`). Malformed payloads fail at this point with a validation error, same as today's deserialization failures (skip + commit + log).

**Kafka message key:** `str(user_id)` (same as today — preserves per-user ordering).

### 2. Two-consumer architecture with intermediate normalized topic

```
raw_transactions.monobank ──┐
raw_transactions.manual   ──┼──> [Normalization Consumer] ──> normalized_transactions ──> [Pipeline Consumer]
raw_transactions.pumb     ──┘                                                              (transfer detection
raw_transactions.revolut  ──┘                                                               → conversion
                                                                                            → classification
                                                                                            → persistence)
```

**Two consumer processes (or consumer groups within one process):**

- **Normalization consumer**: subscribes to all `raw_transactions.*` topics, dispatches to per-source normalizer strategy, publishes `NormalizedTransaction` to a single `normalized_transactions` topic.
- **Pipeline consumer**: subscribes to `normalized_transactions`, runs transfer detection → currency conversion → classification → persistence.

**Why two stages:**
- **Reprocessing is source-agnostic.** The reprocess job publishes `NormalizedTransaction` directly to `normalized_transactions`. It reconstructs one format regardless of how many banks exist. No per-source reconstruction logic, no linear growth with bank count.
- **Clear failure domains.** A normalization bug (bad field mapping) vs. a pipeline bug (wrong rate lookup) are different failure classes with different remediation. Normalization bugs require re-fetch from bank API; pipeline bugs are fixed by reprocessing.
- **Independent scaling.** Normalization is CPU-cheap (field mapping). Pipeline is IO-heavy (DB queries for rates, pair-matching). Different resource profiles, naturally separate.

**Re-fetch is a sibling operation, not a reprocess variant.** Bugs in the normalization layer mean the stored normalized data is already wrong. Reprocessing can't fix it — the source of truth (raw bank data) must be re-fetched from the API. Re-fetch is out of scope for this ADR but acknowledged as a known operational need.

### 3. Layered pipeline with Strategy pattern per layer

Each processing layer defines a Strategy protocol. The pipeline consumer runs layers in sequence. Each layer decides independently whether it needs per-source dispatch or can be source-agnostic.

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

**Why this layer order:**

Conversion and transfer detection are independent — neither reads the other's output. The order between them is free. Transfer detection runs first because it sets `transaction_type`, which classification later branches on (transfers are excluded from category assignment). Conversion's output (amounts in display currencies) is also classifier input (e.g. "transactions over $100 in a category"). Both must complete before classification. Persistence is always terminal.

### 4. Strategy protocol per layer

```python
# --- Normalization (in normalization consumer) ---
class NormalizationStrategy(Protocol):
    def normalize(self, envelope: TransactionEnvelope) -> NormalizedTransaction: ...

# --- Transfer detection ---
class TransferDetectionStrategy(Protocol):
    async def detect_and_pair(
        self, conn: asyncpg.Connection, tx: NormalizedTransaction, own_accounts: list[Account]
    ) -> TransferResult: ...

# --- Classification (future) ---
class ClassificationStrategy(Protocol):
    async def classify(
        self, conn: asyncpg.Connection, tx: NormalizedTransaction
    ) -> ClassificationResult: ...
```

Types referenced above are defined during implementation:
- `TransactionEnvelope` — Kafka routing wrapper (defined in "What lives where" section below)
- `NormalizedTransaction` — replaces `RawTransactionEvent` (section 5 below)
- `TransferResult`, `ClassificationResult` — layer output types (defined in their respective ADRs)
- `Account` — existing model from `account_repo.py`
- `asyncpg.Connection` — the DB connection type (replaces the abstract `Connection` placeholder)

Each strategy has a registry: `dict[str, Strategy]`. A layer with no per-source strategy for the given source either:
- Falls through to a default implementation, OR
- Is skipped (e.g. no transfer detection for a source that marks transfers explicitly in the payload — the normalizer already sets `transaction_type = transfer`)

The Strategy pattern is appropriate given the explicit multi-bank roadmap commitment (PUMB and Revolut planned). The cost is small (a protocol + registry dict per layer); the benefit pays out on the third bank without refactoring.

### 5. NormalizedTransaction replaces RawTransactionEvent

`RawTransactionEvent` (the current shared model) is retired. It's replaced by:

- **Per-source raw models** (e.g. `MonobankStatementItem`) — published to `raw_transactions.*` topics, owned by each source
- **`NormalizedTransaction`** — published to `normalized_transactions` topic by the normalization consumer, consumed by the pipeline consumer

`NormalizedTransaction` carries the same fields as today's `RawTransactionEvent` but is explicitly the contract between the two consumer stages, not a cross-service contract with ingestion.

### 6. Classification layer commitments

Classification is deferred to its own ADR. However, its layer slot constrains the design:

- **Position:** after currency conversion and transfer detection, before persistence.
- **Inputs available:** converted amounts (UAH/USD/EUR), final `transaction_type` (income/expense/transfer), counterparty info (IBAN, counter_name), description text, MCC code, account metadata.
- **Source-specificity:** likely mixed — shared MCC table + per-source merchant pattern lookups. The Strategy pattern accommodates this without redesign.

The classification ADR can refine *what* classification does, but the layer's position and inputs are committed.

---

## Service ownership rules

| Table/Resource | Write owner      | Read access | RLS enforced? |
|---------------|------------------|-------------|---------------|
| `accounts`    | Ingestion service | All services | Yes (user-facing) |
| `transactions` | Consumer (pipeline) | All services | Yes (user-facing) |
| `users`       | API service      | All services | Yes (user-facing) |
| `currency_rates` | Ingestion service | All services | No (reference data) |
| `transfer_match_anomalies` | Consumer (pipeline) | All services | Yes |

- Only one service writes to each table.
- All services have read access to all tables.
- User-facing services (API, ingestion) enforce RLS via `set_config('app.current_user_id', ...)`.
- The consumer operates above RLS — it's a background processor with full table access, processing events on behalf of all users.

This affects repository organization: consumer's `transaction_repo.py` writes transactions; ingestion's `account_repo.py` writes accounts. Reading is unrestricted.

---

## Implications for existing ADRs

### Transfer detection (adr-transfer-detection.md)

**No logic changes.** The Monobank transfer detection strategy receives a `NormalizedTransaction` instead of `RawTransactionEvent` — same fields, different type name. The strategy's input is the output of `MonobankNormalizer`.

**Minor patches needed:**
- Strategy protocol signature: `RawTransactionEvent` → `NormalizedTransaction`
- "Source-agnostic design" section: already describes the strategy pattern correctly; just update the input type reference

### Transaction reprocessing (adr-transaction-reprocessing.md)

**Simplified by this architecture.** Reprocessing publishes `NormalizedTransaction` to `normalized_transactions` — one format, source-agnostic. The reprocess job reconstructs `NormalizedTransaction` from stored DB columns (all meaningful fields are persisted per the information-loss rule). No per-source reconstruction logic needed.

**Patches needed:**
- Reconstruction target: `NormalizedTransaction` (not raw bank payload)
- Topic: `normalized_transactions` (not per-source topics)
- Remove per-source reconstruction tables — single reconstruction function
- The "exercises normalization" benefit goes away (normalization is upstream of where reprocessing publishes), which is acceptable: normalization bugs require re-fetch, not reprocess

---

## What lives where

### Ingestion service (thin gateway)

```
sources/monobank/
    router.py         — webhook endpoint, validates, publishes raw payload
    models.py         — MonobankStatementItem, MonobankWebhookPayload (bank API models)
    backfill.py       — fetches historical statements, publishes raw payloads
    client.py         — Monobank API HTTP client
sources/manual/
    router.py         — user API endpoint, publishes ManualTransactionPayload
```

**Removed from ingestion:** `transaction_adapter.py` (normalization moves to normalization consumer).

### Normalization consumer (per-source → normalized)

```
sources/monobank/
    normalizer.py     — MonobankNormalizer (raw payload → NormalizedTransaction)
sources/manual/
    normalizer.py     — ManualNormalizer (trivial — payload is already canonical)
```

### Pipeline consumer (normalized → DB)

```
sources/monobank/
    transfer.py       — MonobankTransferDetection (the full ADR algorithm)
    descriptions.py   — Description guard dictionary and validation

services/
    currency_conversion_service.py  — Source-agnostic (unchanged)
    pipeline.py                     — Orchestrates layer sequence

models/
    normalized.py     — NormalizedTransaction dataclass
    envelope.py       — TransactionEnvelope (Kafka deserialization target for normalization consumer)
    transfer.py       — TransferResult, PairMatch
    conversion.py     — ConversionResult (unchanged)

repositories/
    transaction_repo.py   — DB insert, ON CONFLICT (id) DO NOTHING (writes — consumer owns transactions)
    account_repo.py       — Account lookups (reads only)
    currency_rate_repo.py — Rate queries (reads only)
    anomaly_repo.py       — Transfer anomaly recording (writes — consumer owns anomalies)
```

### Shared package

```
grosh_shared/
    models.py        — Enums (TransactionSource, TransactionType, Topic, RateSource)
    id_utils.py      — Deterministic UUID generation
    iso_4217.py      — Currency code conversion (used by normalizers)
    auth.py          — JWT utilities (unchanged)
```

**Removed from shared:** `events.py` (`RawTransactionEvent` retired). The shared package no longer defines the Kafka message schema — each source owns its own raw format, and `NormalizedTransaction` is the internal consumer contract.

---

## Implementation sequence

Three ADRs need implementation. The order matters because they share infrastructure (consumer handler, DB schema, Kafka topics). Wrong sequencing creates friction — building transfer detection on the old architecture and immediately refactoring it into the new one.

### Recommended order

```
1. This ADR (pipeline architecture)     — refactors the skeleton
2. Transfer detection ADR               — plugs into the new skeleton
3. Reprocessing ADR                     — builds on top of the complete pipeline
```

### Rationale

**Pipeline architecture first:** Establishes per-source topics, normalization consumer, pipeline consumer, and the layered dispatch. Everything else plugs into this structure. Doing it second would mean building transfer detection against the old `RawTransactionEvent` contract and then immediately migrating it.

**Transfer detection second:** The most complex new logic. Needs the normalization layer to exist (it reads bank-native fields via `NormalizedTransaction`). Needs the anomaly table and claim lock infrastructure. Does NOT need reprocessing — we can run a one-time reconciliation pass after deployment to pair historical transactions.

**Reprocessing last:** By design, reprocessing replays through the *complete* pipeline (minus normalization). It should be implemented after all layers exist, so it exercises the real code path from day one. Building it earlier means testing against an incomplete pipeline and patching later.

### Dependency graph

```
Pipeline architecture
    │
    ├── Normalization consumer + strategies (Monobank, Manual)
    ├── Pipeline consumer + layer dispatch (pipeline orchestrator)
    ├── Per-source topics + normalized_transactions topic
    ├── NormalizedTransaction model
    │
    ▼
Transfer detection
    │
    ├── MonobankTransferDetection strategy
    ├── Anomaly table + recording
    ├── Claim lock mechanism
    ├── Description guard dictionary
    │
    ▼
Reprocessing
    │
    ├── Backup table
    ├── Advisory lock mechanism
    ├── NormalizedTransaction reconstruction from DB
    ├── Verification
    └── Per-user endpoint + K8s Job
```

### Migration strategy (pipeline architecture)

The system is pre-production. Simple drain-and-switch:

1. Pause ingestion writes (stop webhook processing briefly).
2. Wait for consumer to drain the old `raw_transactions` topic (seconds at current volume).
3. Deploy new architecture: normalization consumer + pipeline consumer + new topics.
4. Deploy ingestion changes (publish raw payloads to per-source topics).
5. Resume ingestion writes.

A few minutes of pause. No dual-subscription code to write and later remove.

---

## What this ADR does NOT decide

- **Classification layer design** — deferred until ML spec is written. The layer slot exists; the strategy protocol is defined; position and inputs are committed; implementation waits.
- **Consumer scaling** — single consumer process is sufficient at ~3 users. If Kafka partitioning or consumer groups become relevant, the architecture supports it (per-user key partitioning is already in place).
- **Re-fetch mechanism** — when a normalization bug corrupts stored data, the fix is re-fetching from bank APIs. This is a sibling operation to reprocessing, not a variant of it. Out of scope.

---

## Testing

After implementation, create regression test suites covering:
- Normalization strategies (raw payload → NormalizedTransaction, per source)
- Pipeline layer dispatch (correct strategy selected, correct sequence)
- End-to-end: raw payload in → correct DB row out (both consumers in sequence)

Specific test cases to be determined during implementation with fresh context.
