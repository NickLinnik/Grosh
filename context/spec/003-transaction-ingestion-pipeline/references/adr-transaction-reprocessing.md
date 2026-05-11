# ADR: Transaction Reprocessing

Status: **Draft** — designed, not yet implemented. Last revised 2026-05-08 to add session-scoped advisory lock semantics, the normalization-side staging buffer, LISTEN/NOTIFY drain, and explicit decoupling from pipeline-layer business logic.

---

## Prerequisite: metadata wrapping convention

This ADR depends on a wrapping convention for the `metadata` JSONB: source-original keys live under `metadata.source`, layer-derived keys under `metadata.layer.<name>`. The convention is what makes the strip rule one line ("drop `metadata.layer`") instead of an enumeration of every layer's namespace. See "Metadata separation: wrapped namespaces" below for the full description.

**The convention is implemented as part of Slice 16, before the reprocess job itself:**

1. Refactor each pipeline layer to write under `metadata.layer.<name>` (Slice 16 task).
2. Refactor the normalizer to write under `metadata.source` (Slice 16 task).
3. Refactor the orchestrator's `_merge_metadata` to merge structurally (Slice 16 task).
4. One-time SQL migration to wrap existing flat metadata (Slice 16 task).

Without these prerequisites, the reprocess job's strip rule degrades to per-layer enumeration — which works, but defeats the point of the wrapping design and creates ongoing maintenance every time a new layer is added.

`adr-transfer-detection-v2.md` also depends on the wrapping convention (it writes to `metadata.layer.transfer`). Both v2 ADRs share the same Slice 16 prerequisite work.

**Implementation order:**
1. Slice 16 metadata wrapping refactor.
2. This ADR (reprocess v2) implementation.
3. `adr-transfer-detection-v2.md` implementation.
4. Reprocess sweep across all users (operationalized by step 2; cuts over data to the wrapped + v2-detection shape).

Steps 2 and 3 can ship in either order in principle, but step 4 requires both.

---

## Problem

The consumer pipeline evolves over time — new layers are added (transfer detection, ML classification, rate improvements). When a layer is introduced or its logic changes, existing transactions in the database were processed under the old rules. We need a way to re-run the full pipeline on historical data without:

- Re-fetching from Monobank API (rate-limited, slow, unnecessary)
- Writing per-feature migration scripts (diverge from runtime code path)
- Losing data during the process

## Decision

A **reprocessing job** that:
1. Reads existing transaction rows from the database
2. Reconstructs `NormalizedTransaction` from stored columns (source-agnostic — one format regardless of bank count)
3. Deletes the original rows
4. Republishes events to the `normalized_transactions` Kafka topic
5. The pipeline consumer processes them fresh through all downstream layers (transfer detection, rates, classification)

This exercises the same code path as normal pipeline consumption. Normalization is upstream (in the normalization consumer) and is NOT re-exercised — if normalization logic has a bug, the fix is re-fetching from bank APIs (a sibling operation, not a reprocess variant). See `adr-consumer-pipeline-architecture.md` for the two-consumer architecture.

## Why topic round-trip (not direct handler call)

- **Same code path:** No special "reprocess" mode in the pipeline consumer. Events flow through the standard layer sequence.
- **Source-agnostic:** The reprocess job publishes `NormalizedTransaction` — one format. No per-source reconstruction logic, no linear growth with bank count.
- **Load balancing:** At scale, we control pace via publish rate. The pipeline consumer processes at its own speed.
- **Ordering:** The job publishes in `(time, id)` order within a single producer — Kafka preserves this within a partition. Note: transfer detection is order-independent by design (first leg inserts, second leg finds and pairs regardless of arrival order), so ordering is for reproducibility rather than correctness. Producer should use `enable.idempotence=true` to prevent reordering on retries.
- **Decoupling:** The job only needs DB read access + Kafka producer. No import of consumer internals.

## Prerequisites

All prerequisites below are **hard blockers** — reprocessing cannot function without them. They must be implemented before the reprocessing job is deployed.

### `rate_source` column

The currency conversion layer uses `rate_source` to select which rate chain to query. Currently not stored in the `transactions` table. Without it, `NormalizedTransaction` cannot be reconstructed and reprocessing is impossible.

```sql
ALTER TABLE transactions ADD COLUMN rate_source TEXT;
-- Backfill: derivable from source
UPDATE transactions SET rate_source = 'monobank' WHERE source = 'monobank';
UPDATE transactions SET rate_source = 'nbu' WHERE source = 'manual';
```

### `reprocessing_locks` table (routing signal + status indicator)

```sql
CREATE TABLE reprocessing_locks (
    user_id    UUID PRIMARY KEY REFERENCES users(id),
    locked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Two roles:
- **Routing signal** for the normalization consumer's staging-vs-publish decision.
- **Status indicator** for frontend/observability.

Actual mutual exclusion between two reprocess attempts is via `pg_advisory_lock` (session-scoped). Separate from `users` — avoids contention on RLS-protected core table.

### `reprocessing_backups` table

```sql
CREATE TABLE reprocessing_backups (
    id         UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id    UUID NOT NULL REFERENCES users(id),
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Stores pre-delete snapshots. Survives pod termination (unlike local filesystem in K8s Jobs).

### `staging_normalized_transactions` table

```sql
CREATE TABLE staging_normalized_transactions (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id     UUID NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_staging_user_created
    ON staging_normalized_transactions (user_id, created_at);
```

Holds normalized events for users who are mid-reprocess. The normalization consumer routes events here when the user has a `reprocessing_locks` row; a drain task replays them to `normalized_transactions` after the lock is released. See "Concurrency" below.

No FK on `user_id` — this is an operational queue, not a relational entity. Rows live for seconds-to-minutes during a reprocess.

### Metadata separation: wrapped namespaces

The stored `metadata` JSONB has two top-level wrappers — **no flat keys**:

```jsonc
metadata = {
  "source": { ... },   // everything from the bank (or manual entry source)
  "layer":  { ... }    // everything written by pipeline layers
}
```

- **`metadata.source`** — written by the normalizer when constructing the `NormalizedTransaction`. Contains the bank's original metadata fields (e.g. for Monobank: `counter_name`, `counter_edrpou`, `comment`, `receipt_id`). Reprocess preserves this byte-identical.
- **`metadata.layer`** — written by pipeline layers downstream of normalization. Each layer writes under its own sub-namespace: `metadata.layer.rate` (currency conversion), `metadata.layer.transfer` (transfer detection), and so on. Reprocess strips this entire sub-tree before reconstruction; layers regenerate their content on replay.

**Reprocess strip rule (the entire mechanism):**

```python
metadata_for_replay = {
    "source": stored_row.metadata.get("source", {})
    # metadata.layer dropped entirely — layers will regenerate
}
```

One rule, one branch. No enumeration of layer names. No enumeration of source fields.

**Why wrapping over allow-list / block-list:**

- An allow-list (preserve specific source keys) couples reprocess to every source. New bank → update reprocess.
- A block-list (drop specific layer keys) couples reprocess to every layer. New layer or new namespace → update reprocess.
- Wrapping pushes the responsibility to the writer: normalizers write under `source`, layers write under `layer`. Reprocess knows one rule forever. Adding new banks, new layers, or new metadata fields requires zero changes to reprocess.

**Layer namespace contract:**

Each pipeline layer that emits metadata writes its output as a single sub-key under `metadata.layer`, named after the layer. Output for that layer is fully contained in that sub-key. The orchestrator's `_merge_metadata` function does a structured merge (`metadata.layer.<namespace> = layer_output`) rather than a flat `dict.update()`. This means:

- Layers have isolated, non-colliding namespaces by construction.
- Each layer can be reasoned about independently; one layer cannot read or overwrite another's metadata accidentally.
- New layers slot in by registering a new namespace under `layer.*` — no coordination with reprocess or other layers.

**Cutover from flat metadata:**

Existing rows have flat metadata (`{counter_name: "...", rate_uah: 4123, ...}`) — pre-wrapping era. The cutover is performed once via:

1. A pre-replay SQL migration that wraps existing flat source keys under `metadata.source` (since reprocess can't replay normalization, source-side wrapping must be done by hand).
2. The reprocess sweep itself, which strips flat layer keys (`rate_*`, `transfer`) along with `metadata.layer`, then layers regenerate under the new wrapped shape.

After the cutover, the strip rule simplifies permanently to "drop `metadata.layer`." See Slice 16 tasks for the migration script.

**One-time enumeration cost.** The cutover migration in step 1 enumerates known flat layer keys (currently `rate_*` and `transfer`) so it can strip them before wrapping the rest under `source`. This is the only place enumeration appears anywhere in the system — it's a one-time operational cost paid once during the cutover. After cutover, every layer writes under `metadata.layer.<name>` from inception, every source writes under `metadata.source` from inception, and reprocess strips a single sub-tree without enumerating layer names. **Adding a new bank or a new layer post-cutover requires zero changes to the strip rule.** The wrapping convention is permanent; the enumeration is migrational.

## Event reconstruction

The reprocessing job reconstructs `NormalizedTransaction` from stored DB columns and publishes to the `normalized_transactions` topic. This is **source-agnostic** — one reconstruction function regardless of how many banks exist. The information-loss rule (see `adr-consumer-pipeline-architecture.md`) guarantees all meaningful normalized fields are persisted.

The authoritative `NormalizedTransaction` shape lives at `services/consumer/src/grosh_consumer/models/normalized.py`. Each field maps directly to the stored DB column of the same name, with one exception (`metadata`):

| Field                      | Source column              | Notes                                              |
|----------------------------|----------------------------|----------------------------------------------------|
| `id`                       | `id`                       | UUID5 deterministic from (source, source_id)       |
| `source`                   | `source`                   |                                                    |
| `source_id`                | `source_id`                |                                                    |
| `user_id`                  | `user_id`                  |                                                    |
| `account_id`               | `account_id`               |                                                    |
| `time`                     | `time`                     |                                                    |
| `amount_cents`             | `amount_cents`             | Always positive; `direction` carries flow direction |
| `operation_amount_cents`   | `operation_amount_cents`   |                                                    |
| `operation_currency_code`  | `operation_currency_code`  | Merchant-side currency                             |
| `description`              | `description`              |                                                    |
| `mcc`                      | `mcc`                      |                                                    |
| `cashback_amount_cents`    | `cashback_amount_cents`    |                                                    |
| `balance_cents`            | `balance_cents`            |                                                    |
| `hold`                     | `hold`                     |                                                    |
| `direction`                | `direction`                | Immutable, from normalizer; preserved through replay |
| `counterparty_iban`        | `counterparty_iban`        |                                                    |
| `rate_source`              | `rate_source`              | Stored explicitly so conversion can re-resolve the chain |
| `metadata`                 | `{"source": stored.metadata.source}` — `metadata.layer` is dropped | See "Metadata separation" |

**Note:** `NormalizedTransaction` does **not** carry `special_category`. That field is set by the pipeline's transfer detection layer on the persisted row, never on the event. Reprocessing therefore re-runs detection from scratch — see "Preserved vs. re-derived fields" below.

Similarly, the account's base `currency_code` is not on `NormalizedTransaction`. The pipeline re-resolves it from the `accounts` table at write time using `account_id`.

**Topic:** publishes to `normalized_transactions`.

## Preserved vs. re-derived fields

The reprocessability invariant: deleting a user's transactions and replaying through stored data alone produces the same DB state, modulo a small set of fields that are deterministically recomputed by the pipeline. The invariant is testable — see Slice 16's verify task.

**Preserved** (read from DB, passed through the reconstructed event unchanged, persisted unchanged on the new row):

- `id`, `source`, `source_id`, `user_id`, `account_id`
- `time`, `amount_cents`, `operation_amount_cents`, `operation_currency_code`
- `description`, `mcc`, `cashback_amount_cents`, `balance_cents`, `hold`
- `direction` — immutable money-flow direction set by the normalizer; **never re-derived** on replay
- `counterparty_iban`, `rate_source`
- `metadata.source` — bank-original fields, preserved byte-identical

**Re-derived by pipeline on replay** (CASCADE-cleared on delete, regenerated as the event flows through pipeline layers):

- `special_category` — set by whatever pipeline layer claims the row; for the transfer detection layer specifically, pairings rebuild from scratch and result is deterministic given the same set of transactions and current detection logic.
- `amount_uah_cents` / `amount_usd_cents` / `amount_eur_cents` — currency conversion re-runs against the **current** rate tables. A backfill of historical rates between the original ingestion and the reprocess will surface as updated amounts. This is intentional: reprocessing is the mechanism by which rate corrections propagate.
- `related_transaction_id` — set by the layer that claims a pair (currently transfer detection).
- `metadata.layer.*` — every layer's sub-namespace is regenerated by that layer on replay. The reprocess job strips the entire `metadata.layer` sub-tree on the way in; layers write their own sub-namespaces on the way out as they normally would.
- `transfer_match_anomalies` rows — `ON DELETE CASCADE` on `transaction_id` clears them at delete; the pipeline regenerates them as it encounters anomalous pairings during replay.
- `currency_code` (the account's base currency) — re-resolved from the `accounts` table at write time using `account_id`.

**Reprocessability test:** capture a row's full state pre-reprocess, run the job, compare post-reprocess. All "Preserved" fields must match exactly. "Re-derived" fields are allowed to change (and may, if rates were backfilled or detection logic improved between runs).

**Re-derived state is regenerated against current state, not historical state.** Each pipeline layer's output reflects: (a) the layer's current code, (b) the current rate tables / vocabulary / model parameters, and (c) the set of sibling rows present in the database at replay time. This means a transaction that originally claimed a particular partner under v1 detection may claim a different partner (or none) post-reprocess under v2 detection. Similarly, `metadata.layer.transfer.pair.iban_evidence` for a paired transfer reflects the algorithm's decision against the **current** sibling rows, not the rows that existed at original ingestion. The original classification is not preserved — reprocess is the mechanism by which classification corrections propagate, so retaining historical decisions would defeat the purpose. If "before/after reprocess" comparison is needed for debugging, capture the snapshot before triggering reprocess (this is what `reprocessing_backups` already provides).

**Boundary call-out:** normalization itself is **not** re-exercised by reprocessing. If the normalizer mapped a bank field wrong, the stored row is already wrong-normalized and reprocessing will reproduce the same error. Fixing normalization bugs requires re-fetching from the bank API — a separate operational procedure, out of scope here.

### Why publish to `normalized_transactions` (not per-source topics)

Reprocessing fixes downstream-layer bugs (transfer detection, currency conversion, classification). It does NOT fix normalization bugs — if the normalizer mapped a field wrong, the stored data is already wrong-normalized and reprocessing would reproduce the same error. Normalization bugs require **re-fetching** from bank APIs (a separate operational procedure, out of scope for this ADR).

Publishing to `normalized_transactions` means:
- One reconstruction format regardless of bank count (no per-source logic)
- No coupling to bank-native payload formats (which may have changed since ingestion)
- The reprocess job imports nothing from the normalization consumer

## Job design

```
Location: services/consumer/src/grosh_consumer/jobs/run_reprocess.py
Scope: configurable — accepts an optional list of user_ids
```

### Parameters

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `user_ids` | `list[UUID] \| None` | `None` | Users to reprocess. `None` = all users. |

When `user_ids` is `None`, the job queries all distinct user_ids from the transactions table and processes them sequentially (one user at a time — backup, delete, publish, verify, then next user). This keeps the deletion window per-user rather than global.

### Trigger mechanisms

**Per-user endpoint** (consumer service, authenticated):
```
POST /reprocess
```
Acts on the current authenticated user. No admin role required — users can trigger reprocessing of their own data (e.g. after linking a new account, or if they notice stale classification).

**Batch K8s Job** (ops, all users or subset):
```bash
# All users (USER_IDS_JSON unset)
kubectl apply -f infra/k8s/reprocess-job-template.yaml

# Specific users (JSON-encoded array)
USER_IDS_JSON='["uuid1","uuid2"]' envsubst < infra/k8s/reprocess-job-template.yaml | kubectl apply -f -
```

`USER_IDS_JSON` is a JSON array of UUID strings. The job entrypoint parses it with `json.loads` and validates each element as `UUID` — invalid JSON or a non-UUID element fails loudly at startup. JSON is preferred over comma-separated for type safety: matches the project-wide preference (see `CLAUDE.md` "List-typed query params") for typed list values over delimited strings, even at the env-var boundary.

Used for maintenance operations (new pipeline layer deployed, logic change, bulk fix). Runs to completion independently — no HTTP timeout concerns. Visible via `kubectl get jobs` / `kubectl logs`.

**Why K8s Job for batch (not an admin endpoint):** Reprocessing all users takes minutes. That's a batch workload, not a request/response API call. An admin endpoint would just be a thin wrapper that creates the Job anyway — skip the indirection.

### Flow

```mermaid
sequenceDiagram
    participant Job as Reprocess Job
    participant DB as Postgres
    participant Norm as Normalization Consumer
    participant Kafka as normalized_transactions
    participant Pipe as Pipeline Consumer

    Note over Job,DB: Step 1-2: cleanup + acquire (atomic)
    Job->>DB: DELETE stale reprocessing_locks rows
    Job->>DB: BEGIN; INSERT reprocessing_locks; pg_advisory_lock(); COMMIT
    Note over DB: Status row visible AND lock held
    Note over Norm: From now, normalizer routes this user's events to staging table
    Job->>DB: INSERT INTO reprocessing_backups (snapshot)
    Note over Job: Backup saved — restore point guaranteed
    Job->>DB: SELECT * FROM transactions WHERE user_id = $1 ORDER BY time, id
    Job->>Job: Reconstruct NormalizedTransaction (strip pipeline-derived metadata keys)
    Job->>DB: DELETE FROM transactions WHERE user_id = $1
    Note over DB: CASCADE clears anomalies; self-FK SET NULL on remaining rows of same user
    Job->>Kafka: Publish replay events directly (bypassing staging)
    Pipe->>Kafka: Poll, process replay events through pipeline layers
    Pipe->>DB: INSERT processed transactions
    Job->>Kafka: Wait for consumer offset to catch up
    Job->>DB: Verify: snapshot IDs all present
    Note over Job: If verification fails → restore from backup
    Note over Job,DB: Step 9: release (atomic)
    Job->>DB: BEGIN; DELETE reprocessing_locks; pg_advisory_unlock(); NOTIFY reprocess_complete; COMMIT
    Note over Norm: NOTIFY wakes drain task
    Norm->>DB: SELECT staged events for this user, ORDER BY created_at
    Norm->>Kafka: Publish staged events one by one (in order)
    Norm->>DB: DELETE drained staging rows
    Pipe->>Kafka: Poll, process staged events normally
```

### Safety: backup before delete

The job inserts a full snapshot into `reprocessing_backups` before any destructive operation. This is a few KB of JSONB for ~2700 rows — milliseconds to write. If anything goes wrong during replay (consumer crash, logic bug, partial processing), restore from the backup row.

No shadow tables, no schema duplication, no DI for table names, no ephemeral filesystem.

### Why not a shadow table?

Considered and rejected. A shadow table would need:
- RLS policies duplicated
- All partial indexes recreated
- FK constraints from other tables can't reference a temp table

The complexity of reproducing the full table infrastructure on a temp table exceeds the benefit. A JSONB backup row achieves the same safety guarantee with zero schema complexity.

### Concurrency: per-user reprocessing lock + normalization-side staging

A race exists between the reprocess job (deleting and republishing rows) and the system's normal ingestion path (webhook events flowing through the normalization consumer into `normalized_transactions`, then through the pipeline consumer into the DB). Without protection:

- A webhook event could be inserted after the snapshot read but deleted by the subsequent bulk DELETE — losing data.
- A webhook event for the reprocessing user could interleave with replayed events in `normalized_transactions`, fragmenting a coherent replay into a mix of stale + fresh + replayed rows.
- Webhook events for **other users** sharing the same Kafka partition as the reprocessing user could queue up behind the replay batch, suffering tail latency unrelated to their own data.

The design solves all three with two pieces:

- A **session-scoped advisory lock** held by the reprocess job for the entire duration of the job (acquire → snapshot → delete → publish → wait-for-consumer-drain → release).
- A **staging buffer table** on the normalization consumer side. While a user is locked, the normalization consumer routes that user's normalized events into the staging table instead of `normalized_transactions`. After the lock is released, a drain task replays the staged events in order.

Together these decouple the reprocess job from the pipeline consumer entirely. The pipeline consumer never blocks on reprocess; co-tenant users on the same Kafka partition are unaffected.

#### Status indicator

```sql
CREATE TABLE reprocessing_locks (
    user_id    UUID PRIMARY KEY REFERENCES users(id),
    locked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Row exists = reprocessing is in progress. Separate from `users` to avoid contention on a core RLS-protected table. Used by the frontend to show a "refreshing" state and by the normalization consumer to decide whether to stage or publish (see below). The row is the **routing signal**; the advisory lock is the mutual exclusion mechanism.

#### Advisory lock (session-scoped)

The reprocess job acquires `pg_advisory_lock(hashtext('reprocess:' || user_id::text))` on a dedicated session connection at job start and releases at job end via `pg_advisory_unlock(...)`. The lock is held across the whole job, not just the snapshot/delete transaction — this matters because the job has work after the delete (publish to Kafka, wait for consumer drain, verify) and the lock must persist through it all.

If the job process crashes, the session terminates, and Postgres releases the advisory lock automatically. No TTL bookkeeping needed.

#### Reprocess job flow

1. **Clean up stale status rows:** DELETE rows whose advisory lock is no longer held by any session, detected via `pg_locks` introspection — NOT a time-based TTL. Postgres releases an advisory lock at session end, so "no holder in `pg_locks`" is a definitive crash signal:
   ```sql
   DELETE FROM reprocessing_locks rl
   WHERE NOT EXISTS (
       SELECT 1
       FROM pg_locks
       WHERE locktype = 'advisory'
         AND objid = (hashtext('reprocess:' || rl.user_id::text)::bigint & x'ffffffff'::bigint)::int
   );
   ```
   No TTL bookkeeping, no false positives (a live session that's mid-work still owns the lock and the row is preserved), no false negatives (a crashed session has already released the lock and its row is reaped on the next reprocess attempt). The `objid` mask is `x'ffffffff'` — the full 32-bit unsigned mask, NOT `x'7fffffff'`. `hashtext` returns a signed int4 reinterpreted as uint32 in `pg_locks.objid`; a 31-bit mask would strip the sign bit and miss every lock whose `hashtext` was negative. `locked_at` remains as a debug breadcrumb only.
2. **Acquire status + lock atomically.** In a single transaction:
   ```sql
   BEGIN;
     INSERT INTO reprocessing_locks (user_id) VALUES ($1);
     SELECT pg_advisory_lock(hashtext('reprocess:' || $1::text));
   COMMIT;
   ```
   If two reprocess attempts race, the second's `INSERT` hits the `PRIMARY KEY` constraint and fails — single-flight enforced.
   The atomicity matters: between the row insert and the lock acquire, a webhook event for this user must not be able to slip through the normalization consumer's "is user locked?" check and reach `normalized_transactions` before the lock is held. By making them one transaction, the row becomes visible only at commit, and the lock is held by then.
3. **Snapshot to backup:** `INSERT INTO reprocessing_backups (user_id, data) VALUES ($1, $snapshot_jsonb)`.
4. **Read rows, reconstruct events:** `SELECT * FROM transactions WHERE user_id = $1 ORDER BY time, id`. Strip pipeline-derived metadata keys per the strip list.
5. **Delete:** `DELETE FROM transactions WHERE user_id = $1`. Cascades clear `transfer_match_anomalies` rows for those transactions; the self-FK `related_transaction_id` is set to `NULL` on remaining rows of the same user (all of which are also being deleted, so this is a no-op in practice).
6. **Publish replay events** to `normalized_transactions` directly (bypassing staging — the reprocess job is the lock holder, the staging path is for non-reprocess events).
7. **Wait for consumer drain.** Monitor Kafka consumer offset until it reaches the highest published offset.
8. **Verify** (see Verification section below).
9. **Release:**
   ```sql
   BEGIN;
     DELETE FROM reprocessing_locks WHERE user_id = $1;
     -- pg_advisory_unlock can run inside or outside transaction;
     -- doing it inside keeps cleanup atomic.
     SELECT pg_advisory_unlock(hashtext('reprocess:' || $1::text));
     NOTIFY reprocess_complete, $1::text;
   COMMIT;
   ```
   The `NOTIFY` wakes the normalization consumer's drain task (see staging buffer section).

#### Normalization consumer: staging buffer

The normalization consumer's loop processes raw bank events into `NormalizedTransaction`s. Without reprocess in the picture, it would publish each result directly to `normalized_transactions`. With reprocess, it adds a routing step:

```python
# After normalizing a raw event into a NormalizedTransaction:
async with conn.transaction():
    user_locked = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM reprocessing_locks WHERE user_id = $1)",
        normalized.user_id,
    )
    if user_locked:
        await conn.execute(
            "INSERT INTO staging_normalized_transactions (user_id, payload) VALUES ($1, $2)",
            normalized.user_id, normalized.model_dump_json(),
        )
    else:
        await producer.send("normalized_transactions", key=user_id, value=payload)
```

The check + write happens in one transaction so the routing decision is consistent: by the time the transaction commits, either the staging row exists OR the Kafka publish has been initiated (Kafka producer commits separately, but the at-least-once contract is preserved by the existing idempotency guard in the pipeline consumer).

#### Staging table

```sql
CREATE TABLE staging_normalized_transactions (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id     UUID NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_staging_user_created
    ON staging_normalized_transactions (user_id, created_at);
```

`payload` is the JSON-encoded `NormalizedTransaction`. `created_at` defines drain ordering. No FK on `user_id` — this is an operational queue, not a relational entity. Rows live for seconds-to-minutes during a reprocess, deleted after drain.

#### Drain task (normalization consumer)

Triggered two ways:

1. **`LISTEN reprocess_complete`** — the normalization consumer subscribes on a long-lived asyncpg listener connection at startup. When the reprocess job's `NOTIFY reprocess_complete, '<user_id>'` fires (after the lock is released), the listener callback runs.
2. **Periodic sweep, every 60 seconds** — fallback for missed notifications (listener connection blip, consumer restart).

Drain logic:

```python
async def drain_for_user(user_id: UUID):
    rows = await conn.fetch(
        "SELECT id, payload FROM staging_normalized_transactions "
        "WHERE user_id = $1 ORDER BY created_at",
        user_id,
    )
    # Sort defensively in Python (rows already ordered by SQL, but explicit).
    rows.sort(key=lambda r: r["created_at"])
    for row in rows:
        await producer.send("normalized_transactions", key=user_id, value=row["payload"])
        await conn.execute(
            "DELETE FROM staging_normalized_transactions WHERE id = $1",
            row["id"],
        )
```

Publish-then-delete is the at-least-once order: a crash between publish and delete causes a re-publish on retry, idempotency-guarded by the pipeline consumer's `SELECT EXISTS WHERE id = $1` check. Publish failure leaves the row in staging for the next sweep.

The periodic sweep variant scans for **all** users with staging rows whose `reprocessing_locks` entry no longer exists (i.e. their reprocess is done but their drain didn't fire):

```sql
SELECT DISTINCT user_id FROM staging_normalized_transactions
WHERE user_id NOT IN (SELECT user_id FROM reprocessing_locks)
```

Then calls `drain_for_user` for each.

#### Why staged events arrive after replayed events (and why that's fine)

Replayed events publish first (during the lock); staged events publish second (after the lock releases). For the staged events, this means they appear in `normalized_transactions` AFTER events with later transaction times that were published as part of the replay. The pipeline consumer processes them in publish order, so a webhook event from time T may be processed after replayed events from times > T.

This is acceptable because:

- Pipeline layers are designed to be order-independent at the per-event level. Transfer detection's universal fetch finds candidates by time window, not arrival order; classification is per-row; currency conversion is per-row. Cross-event ordering only matters within the consumer's claim-lock semantics, which `FOR UPDATE SKIP LOCKED` handles regardless of arrival order.
- Auto-resolve handles the late-arriving partner case: a staged webhook event whose partner was already replayed will pair correctly because the universal fetch sees the committed replayed row.
- The user's UI shows "refreshing" until staging is empty (driven by the absence of `reprocessing_locks` row + an empty staging query). Brief invisibility of the most recent webhook event during reprocess is acceptable.

The intent is captured: "the staged event was always there, it's the past, we just needed to correct what came before it."

**Note for future stateful pipeline layers.** Today's pipeline layers (transfer detection, currency conversion) are per-event. A future ML classification layer using recency features ("looks like another grocery transaction this week") may produce slightly different outputs for staged events than they would have under normal flow, because at drain time the recently-replayed older events are already committed and influence the classifier's view of "recent activity." Acceptable trade-off: reprocess regenerates all derived state against current logic and current sibling rows, by design. This is the same trade-off the reprocessability invariant already documents — derived fields are explicitly allowed to differ post-replay.

#### Why this is race-free

- Webhook events for the locked user are routed to staging atomically with the lock check. They cannot reach `normalized_transactions` while the user is locked.
- Replayed events are published by the lock holder while the lock is held. They cannot collide with webhook events for the same user (those are staged).
- The pipeline consumer never sees the lock; it just consumes `normalized_transactions` at full speed.
- Other users (different `user_id`) are entirely unaffected — their webhook events flow through normalization → topic → pipeline normally, regardless of which Kafka partition they share with the reprocessing user.
- A reprocess crash releases the advisory lock automatically (session-scoped); the next reprocess attempt detects the orphaned `reprocessing_locks` row via `pg_locks` introspection (step 1: row's lock has no holder → row is stale) and DELETEs it before proceeding. There is no time window where a crashed reprocess blocks retries — detection is constant-time and definitive. Staged events for that user remain queued and drain once the new reprocess (or the periodic sweep) runs.

#### Stale lock recovery

The `reprocessing_locks` row is a routing signal + status indicator, not the exclusion mechanism. If the job is OOM-killed:

- Advisory lock releases automatically (session ends).
- `reprocessing_locks` row remains until cleaned up.
- Step 1 of the next reprocess clears it, OR manual cleanup: `DELETE FROM reprocessing_locks WHERE user_id = $1`.

During the stale-row window, webhook events keep going to staging (correct — we don't know if reprocess is genuinely stuck or just slow). The periodic sweep eventually drains them once the row is cleared.

#### Double-trigger prevention

Step 2's atomic `INSERT INTO reprocessing_locks` enforces single-flight via PK conflict. The second concurrent reprocess job for the same user fails its insert and exits cleanly without acquiring the advisory lock or touching any data.

#### Concurrent reprocesses across different users

Two reprocess jobs running at the same time for **different** users are fully independent:

- Each job acquires its own advisory lock keyed on `user_id`. Different keys, no contention.
- Each job's `reprocessing_locks` row has a different PK; no PK conflict.
- The normalization consumer's staging routing decision is per-event keyed on `user_id`; events for user X go to staging when X is locked, events for user Y flow normally if Y is not locked.
- Two `NOTIFY reprocess_complete` notifications (one per user) emit independently when each job completes. asyncpg's listener callback model handles N concurrent notifications correctly, each invoking `drain_for_user(user_id)` for the respective user.

Cross-user concurrency is supported by construction; no extra serialization is needed.

#### Single-instance normalization consumer

The staging buffer's order-preservation guarantee (`ORDER BY created_at` in the drain) assumes a **single normalization consumer instance per Kafka partition**. This is the standard Kafka per-partition single-consumer-instance guarantee — do not deliberately fork the consumer.

If horizontal scaling becomes necessary in the future, increase the partition count rather than spawning multiple consumer instances per partition. Per-user ordering is preserved because each user's events always hash to the same partition. Multiple instances per partition would cause `created_at` timestamps to be set by different machines whose clocks may differ slightly, weakening the ordering guarantee from "strict by clock" to "approximately by clock."

At our scale (3 users, low event rate), single-instance is the correct deployment forever. The constraint is documented for future scaling decisions.

#### Rate-limiting

The per-user endpoint should enforce a cooldown (e.g. one reprocess per hour per user) to prevent self-inflicted DoS. Independent of the locking mechanism.

#### The deletion window

Between DELETE (step 5) and consumer drain (step 7), the user's data is incomplete. For a 3-user family app this is seconds per user. Frontend uses the `reprocessing_locks` row to display a "refreshing" state.

### Cascade effects

The DELETE in step 5 cascades through any FK relationships that target `transactions(id)`:

- Self-FK `related_transaction_id` (`ON DELETE SET NULL`): partners outside the deleted batch get their reference nulled. In practice, all of a user's rows are deleted together so this is a no-op.
- Any table with `ON DELETE CASCADE` on `transaction_id`: rows are cleared automatically alongside the parent. (At time of writing this includes `transfer_match_anomalies`. Future tables added by other layers should also use `ON DELETE CASCADE` so reprocess doesn't need per-table cleanup logic.)

Pipeline layers regenerate their derived state during replay — reprocess does not need to know which tables exist or what their layers do.

### Idempotency

The pipeline consumer has a per-row idempotency guard (e.g. `SELECT EXISTS WHERE id = $1` early in the per-event handler). It must NOT fire during reprocessing — the rows were just deleted. Since reprocess deletes before publishing, the guard sees no existing row on replay events and processes normally. No special flag needed.

The same guard applies to staged events drained after the lock release: they're new inserts from the consumer's perspective. The drain task's at-least-once publish + idempotency guard combination handles drain retries cleanly (a republish on retry fires the guard and exits without effect).

**At-least-once + transfer detection v2 interaction.** A staged event A may be republished (drain crash between Kafka publish and staging-row delete). Sequence: A publishes → consumer claims pair (A, partner B) → drain crashes → drain restarts and republishes A → consumer's idempotency guard sees A already exists → no-op. The pair (A, B) is unaffected; the retry is idempotent. If A's true partner is also a staged event arriving later, that partner's universal fetch will find A correctly committed (regardless of whether A's first publish paired or not), so pairing is preserved across drain retries. No special handling needed beyond the idempotency guard already in place.

### Verification

The job records the highest Kafka offset it published per partition. It then waits until the consumer's committed offset reaches or exceeds that value (polled via Kafka AdminClient). Once caught up:

- Verify all IDs from the pre-delete snapshot exist in the table: `SELECT id FROM transactions WHERE user_id = $1 AND id = ANY($snapshot_ids)`
- If any IDs are missing: alert and restore from backup
- New transactions that arrived during the reprocess window (webhook events) are excluded from the check — they have IDs not in the snapshot and are expected

This catches missing rows (consumer dropped events, partial replay), not corrupted content. Stronger verification (full row hash comparison) can be added if pipeline logic ever produces silently-wrong outputs.

On verification failure, the order is: 1) restore from backup, 2) clear the `reprocessing_locks` row, 3) alert. System returns to known-good state before humans are notified.

**During step 1 (restore-from-backup), the advisory lock is still held.** Postgres advisory locks don't block writes — they only serialize against other advisory-lock acquisitions — so the restore INSERTs proceed normally. While restore is running, webhook events continue routing to staging (the `reprocessing_locks` row still exists from step 2 of the original job). After step 2 of recovery clears the lock, the LISTEN/NOTIFY drain (or the periodic sweep) replays staged events on top of the restored state.

**If restore itself fails** (corrupted backup, schema drift, FK violations) the job stops in an inconsistent state: the `reprocessing_locks` row exists, the advisory lock is released (the job's session is gone), staged events keep accumulating, and the user's `transactions` table is partially restored. **Manual intervention is required.** Recovery sequence: investigate the restore failure, manually re-restore (or accept partial state), then `DELETE FROM reprocessing_locks WHERE user_id = $1`. Once cleared, the periodic sweep drains pending staged events. If the user's data is materially wrong, they should be notified out-of-band; reprocess does not auto-page on this path because it would mask the underlying bug.

### Backup storage and retention

K8s Job pods lose local storage on termination. Backups are stored in a DB table:

```sql
CREATE TABLE reprocessing_backups (
    id         UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id    UUID NOT NULL REFERENCES users(id),
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`data` contains the full snapshot (array of row objects). A few KB per user at current scale.

NOT deleted on success — bugs may surface hours later. In production, a daily K8s CronJob removes backups older than 30 days. Locally, the table grows unboundedly (negligible — reprocessing is rare and rows are small).

## When to use

- New pipeline layer added (transfer detection, ML classifier)
- Existing layer logic changed (description guard updated, rate fallback improved)
- Bug fix that affected stored results
- NOT for routine backfill (use `run_transactions_backfill.py` for fetching new data from Monobank)

## What this is NOT

- Not a reconciliation job (doesn't selectively re-pair transfers)
- Not a Monobank backfill (doesn't call external APIs)
- Not a migration (doesn't run SQL transformations)

It's a "backup, nuke, and replay from stored truth" operation. The stored row IS the source of truth; the pipeline layers are deterministic transformations applied on top.

## Decoupling boundary: reprocessing vs. business logic

This ADR describes an **operational mechanism**. It must NOT contain knowledge of any specific pipeline layer's internals. Conversely, pipeline layers must NOT contain knowledge of reprocessing.

The contract:

- **Pipeline layers** (currency conversion, transfer detection, classification, future layers) write outputs as they normally do — to the `transactions` row and to their own sub-namespace under `metadata.layer.<name>`. They are pure business logic; they know nothing about reprocess.
- **Normalizers** (one per source: `monobank`, `manual`, future `pumb`/`revolut`) write source-original metadata under `metadata.source`. They know nothing about reprocess.
- **The reprocess job** has one structural rule: drop `metadata.layer`, preserve `metadata.source`. The wrapping convention does the work that a strip list would otherwise have to do — adding a new layer or a new source requires zero changes to reprocess.
- **Reprocess introduces** the `reprocessing_locks`, `reprocessing_backups`, and `staging_normalized_transactions` tables, plus the LISTEN/NOTIFY drain in the normalization consumer. None of this is required for normal pipeline operation; pipeline layers can be developed, tested, and deployed without any reprocess infrastructure being present.

The wrapping convention is the **only** coupling point between reprocess and the rest of the system. There are no layer-specific code paths inside reprocess, and no reprocess-aware code paths inside any layer.

## Deployment order for layer changes

When a pipeline layer changes its output (new metadata fields, changed enum values, new anomaly types), the deploy/reprocess sequence is:

1. **Deploy the new layer code.** The layer emits the new outputs going forward (under its own `metadata.layer.<name>` sub-namespace). Existing rows still have old-format outputs. No reprocess change is needed — the wrapping convention means reprocess already drops the entire `metadata.layer` sub-tree on replay.
2. **Run reprocess** for affected users. CASCADE-clears layer-derived rows (e.g. `transfer_match_anomalies`); reprocess strips `metadata.layer` before reconstruction; the layer regenerates everything against fresh state.
3. **Apply schema cleanup migrations** (e.g. drop unused enum values that the new layer no longer emits). These run last because step 2 may still emit the old values for rows that haven't been reprocessed yet.

Skipping or reordering steps risks rows referencing dropped enum values (step 3 before step 2 leaves stale references that fail validation).

---

## Testing

After implementation, create comprehensive unit and integration regression test suites (similar in scope to the currency conversion test suites). Specific test cases to be determined during implementation with fresh context.
