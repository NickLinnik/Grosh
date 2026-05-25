# ADR: Transaction Reprocessing

How a user's transactions are re-run through the enrichment pipeline without re-fetching from the bank API.

---

## Problem

The enrichment pipeline evolves over time — new layers are added (transfer detection, ML classification, rate improvements). When a layer is introduced or its logic changes, existing transactions in the database were processed under the old rules. We need a way to re-run the full pipeline on historical data without:

- Re-fetching from Monobank API (rate-limited, slow, unnecessary).
- Writing per-feature migration scripts (diverge from runtime code path).
- Losing data during the process.

## Decision

A **reprocessing job** that:

1. Reads existing transaction rows from the database.
2. Reconstructs `NormalizedTransaction` from stored columns (source-agnostic — one format regardless of bank count).
3. Deletes the original rows.
4. Republishes events to the `normalized_transactions` Kafka topic.
5. The enrichment consumer processes them fresh through all downstream layers (transfer detection, rates, classification).

This exercises the same code path as normal pipeline consumption. Normalization is **upstream** (in the normalization service's consumer of `raw_transactions.*`) and is NOT re-exercised — if normalization logic has a bug, the fix is re-fetching from bank APIs (a sibling operation, not a reprocess variant). See `adr-consumer-pipeline-architecture.md` for the two-service topology.

## Why topic round-trip (not direct handler call)

- **Same code path:** no special "reprocess" mode in the enrichment consumer. Events flow through the standard layer sequence.
- **Source-agnostic:** the reprocess job publishes `NormalizedTransaction` — one format. No per-source reconstruction logic, no linear growth with bank count.
- **Decoupling:** the job needs only DB read access + a Kafka producer. It imports nothing from the enrichment service.
- **Ordering:** the job publishes in `(time, id)` order within a single producer — Kafka preserves this within a partition. Transfer detection is order-independent by design (first leg inserts, second leg finds and pairs regardless of arrival order), so ordering is for reproducibility rather than correctness. The producer uses `enable.idempotence=true` to prevent reordering on retries.

---

## Schema

### `reprocessing_locks` — routing signal + status indicator

```sql
CREATE TABLE reprocessing_locks (
    user_id    UUID PRIMARY KEY REFERENCES users(id),
    locked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Two roles:

- **Routing signal** for the normalization consumer's staging-vs-publish decision.
- **Status indicator** for frontend/observability.

The row's **presence** is the lock (no `status` column). Mutual exclusion between two reprocess attempts is via `pg_advisory_lock` (session-scoped, acquired by the pod) plus the row's PRIMARY KEY constraint (enforced by the API). Co-owned: ingestion INSERTs, normalization DELETEs. Neither service UPDATEs — the row has no mutable state.

### `reprocessing_backups` — pre-delete snapshots

```sql
CREATE TABLE reprocessing_backups (
    id         UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id    UUID NOT NULL REFERENCES users(id),
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Stores pre-delete snapshots. Survives pod termination (unlike local filesystem in K8s Jobs). A few KB per user at current scale.

Retention: a `pg_cron` job (`reprocessing_backups_cleanup`) deletes rows older than 30 days, runs daily at 03:00 UTC. The migration that registers it (`0015`) opens with a TZ guard (`RAISE EXCEPTION` if server `TimeZone != 'UTC'`) so a misconfigured deploy fails the migration loudly instead of silently scheduling the job in the wrong window. `infra/docker-compose.yml`'s postgres service carries `TZ: UTC` to satisfy the guard locally.

### `staging_normalized_transactions` — operational queue

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

Holds normalized events for users who are mid-reprocess. The normalization consumer routes events here when the user has a `reprocessing_locks` row; the staging drain task republishes them to `normalized_transactions` after the lock is released.

No FK on `user_id` — this is an operational queue, not a relational entity. Rows live for seconds-to-minutes during a reprocess.

### Metadata wrapping convention (prerequisite)

The stored `metadata` JSONB has two top-level wrappers — **no flat keys**:

```jsonc
metadata = {
  "source": { ... },   // everything from the bank (or manual entry source)
  "layer":  { ... }    // everything written by pipeline layers
}
```

- **`metadata.source`** — written by the normalizer when constructing the `NormalizedTransaction`. Contains the bank's original metadata fields (for Monobank: `counter_name`, `counter_edrpou`, `comment`, `receipt_id`). Reprocess preserves this byte-identical.
- **`metadata.layer`** — written by pipeline layers downstream of normalization. Each layer writes under its own sub-namespace: `metadata.layer.rate` (currency conversion), `metadata.layer.transfer` (transfer detection), etc. Reprocess strips this entire sub-tree before reconstruction; layers regenerate their content on replay.

**Reprocess strip rule (the entire mechanism):**

```python
metadata_for_replay = {
    "source": stored_row.metadata.get("source", {})
    # metadata.layer dropped entirely — layers will regenerate
}
```

One rule, one branch. No enumeration of layer names. No enumeration of source fields.

**Why wrapping over allow-list / block-list.** An allow-list (preserve specific source keys) couples reprocess to every source — new bank → update reprocess. A block-list (drop specific layer keys) couples reprocess to every layer — new layer → update reprocess. Wrapping pushes the responsibility to the writer: normalizers write under `source`, layers write under `layer`. Reprocess knows one rule forever. Adding new banks, new layers, or new metadata fields requires zero changes to reprocess.

**Layer namespace contract.** Each pipeline layer that emits metadata writes its output as a single sub-key under `metadata.layer`, named after the layer. The enrichment orchestrator's `_merge_metadata` function does a structured merge (`metadata.layer.<namespace> = layer_output`) rather than a flat `dict.update()`. This means:

- Layers have isolated, non-colliding namespaces by construction.
- Each layer can be reasoned about independently; one layer cannot read or overwrite another's metadata accidentally.
- New layers slot in by registering a new namespace under `layer.*` — no coordination with reprocess or other layers.

---

## Event reconstruction

The reprocessing job reconstructs `NormalizedTransaction` from stored DB columns and publishes to `normalized_transactions`. **Source-agnostic** — one reconstruction function regardless of how many banks exist. The information-loss rule (see `adr-consumer-pipeline-architecture.md`) guarantees all meaningful normalized fields are persisted.

The authoritative `NormalizedTransaction` shape and `TransactionRow.to_normalized()` (the inverse mapping) live in `shared/src/grosh_shared/domain/normalized.py`. Each field maps directly to the stored DB column of the same name, with one exception (`metadata`):

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
| `metadata`                 | `{"source": stored.metadata.source}` — `metadata.layer` dropped | See "Metadata wrapping" above |

**Note:** `NormalizedTransaction` does **not** carry `special_category`. That field is set by the enrichment pipeline's transfer detection layer on the persisted row, never on the event. Reprocessing therefore re-runs detection from scratch — see "Preserved vs. re-derived fields" below.

The account's base `currency_code` is also not on `NormalizedTransaction`. The pipeline re-resolves it from the `accounts` table at write time using `account_id`.

---

## Preserved vs. re-derived fields

The reprocessability invariant: deleting a user's transactions and replaying through stored data alone produces the same DB state, modulo a small set of fields that are deterministically recomputed by the pipeline.

**Preserved** (read from DB, passed through the reconstructed event unchanged, persisted unchanged on the new row):

- `id`, `source`, `source_id`, `user_id`, `account_id`
- `time`, `amount_cents`, `operation_amount_cents`, `operation_currency_code`
- `description`, `mcc`, `cashback_amount_cents`, `balance_cents`, `hold`
- `direction` — immutable money-flow direction set by the normalizer; **never re-derived** on replay
- `counterparty_iban`, `rate_source`
- `metadata.source` — bank-original fields, preserved byte-identical

**Re-derived by pipeline on replay** (CASCADE-cleared on delete, regenerated as the event flows through pipeline layers):

- `special_category` — set by whatever pipeline layer claims the row (today: transfer detection).
- `amount_uah_cents` / `amount_usd_cents` / `amount_eur_cents` — currency conversion re-runs against the **current** rate tables. A backfill of historical rates between the original ingestion and the reprocess will surface as updated amounts. Intentional: reprocessing is the mechanism by which rate corrections propagate.
- `related_transaction_id` — set by the layer that claims a pair (currently transfer detection).
- `metadata.layer.*` — every layer's sub-namespace is regenerated by that layer on replay.
- `transfer_match_anomalies` rows — `ON DELETE CASCADE` on `transaction_id` clears them at delete; the pipeline regenerates them as it encounters anomalous pairings during replay.
- `currency_code` (the account's base currency) — re-resolved from the `accounts` table at write time using `account_id`.

**Re-derived state is regenerated against current state, not historical state.** Each pipeline layer's output reflects: (a) the layer's current code, (b) the current rate tables / vocabulary / model parameters, and (c) the set of sibling rows present in the database at replay time. This means a transaction that originally claimed a particular partner may claim a different partner (or none) post-reprocess if detection logic improved. Similarly, `metadata.layer.transfer.pair.iban_evidence` for a paired transfer reflects the algorithm's decision against the **current** sibling rows, not the rows that existed at original ingestion. The original classification is not preserved — reprocess is the mechanism by which classification corrections propagate; retaining historical decisions would defeat the purpose. If "before/after reprocess" comparison is needed for debugging, capture the snapshot before triggering reprocess (this is what `reprocessing_backups` already provides).

**Reprocessability test:** capture a row's full state pre-reprocess, run the job, compare post-reprocess. All "Preserved" fields must match exactly. "Re-derived" fields are allowed to change.

**Boundary call-out.** Normalization is **not** re-exercised by reprocessing. If the normalizer mapped a bank field wrong, the stored row is already wrong-normalized and reprocessing will reproduce the same error. Fixing normalization bugs requires re-fetching from the bank API — a separate operational procedure, out of scope here.

---

## Lock ownership

The triggering API owns lock creation, not the job pod. Two concurrent triggers cannot both pass a stale "is there a lock?" check and submit duplicate jobs.

| Operation                                              | Owner                                          |
|--------------------------------------------------------|------------------------------------------------|
| INSERT `reprocessing_locks` row                        | **Ingestion API** inside the trigger transaction, before submitting the K8s Job |
| `pg_advisory_lock(...)`                                | Reprocess pod at job start (session-scoped)    |
| DELETE `reprocessing_locks` row                        | Reprocess pod on completion / failure recovery |
| `pg_advisory_unlock(...)` + `NOTIFY reprocess_complete` | Reprocess pod                                  |

### Why API owns the INSERT

If the pod owned both the INSERT and the advisory lock, a race window existed: between the API submitting the K8s Job and the pod starting, a second `POST /reprocess` call could submit a duplicate Job. The pod-side `INSERT ... ON CONFLICT` would catch the duplicate (the second pod would fail to acquire and exit), but the failure manifested as a K8s Job in `Failed` state rather than a clean HTTP 409 at the API boundary.

API-owned INSERT eliminates the race entirely: the API holds an open transaction containing the INSERT, attempts the Job submission, and either commits both (Job dispatched + lock row visible) or rolls back both (409 returned, no lock, no Job). The pod's lock-row assertion on startup becomes a defense-in-depth check — if the row is absent (e.g. operator manually deleted it, or the API's commit was rolled back), the pod exits cleanly with code 0 rather than touching data.

### 409 response is informative

Because the API holds the row write, the 409 path can look up and return the currently-running Job ID by label-selector query against the K8s API. The client gets `Reprocessing already in progress for user {user_id}. Existing job: {job_id}. Poll {status_url} for progress.` rather than a bare conflict. If the lock row exists but no live Job is found (crashed pod, stale row), the response degrades to `The previous job may have crashed; contact an administrator to clear the lock.` — the operator-clearing-the-row workflow from "Stale lock recovery" still applies.

### RBAC

Migration `0012` enforces the ownership split at the DB layer:

```sql
GRANT INSERT ON reprocessing_locks TO grosh_ingestion;
REVOKE INSERT ON reprocessing_locks FROM grosh_consumer;
```

`DELETE` on `reprocessing_locks` remains with `grosh_consumer` (the pod owns release). Both roles retain `SELECT`. This is **one of two** documented exceptions to CLAUDE.md's "single writer per table" invariant; the other is `transactions` (enrichment INSERTs/UPDATEs, normalization DELETEs on reprocess — see `adr-consumer-pipeline-architecture.md`).

The exception is safe because `reprocessing_locks` has no mutable state — the row exists or it doesn't; neither service UPDATEs it.

---

## Trigger endpoints

Two endpoints, both on the ingestion service (alongside the existing backfill trigger):

- **Per-user:** `POST /v1/users/{user_id}/reprocess` — auth `caller.id == user_id OR caller.role == admin`. Empty body. Returns 202 with `JobTriggerResponse`.
- **Admin bulk:** `POST /v1/admin/reprocess` — admin-only. Body: `{"user_ids": list[UUID] | null, "force": bool = false}` (`null` = all current users). Returns 202 with `BulkReprocessResponse`.

### Per-user trigger flow

1. Verify authorization → 403 with `code: INSUFFICIENT_PERMISSIONS` otherwise.
2. Apply per-user rate-limit (1/hour) via atomic UPDATE-RETURNING on `users.last_reprocess_started_at`:
   ```sql
   UPDATE users
   SET last_reprocess_started_at = now()
   WHERE id = $1
     AND (last_reprocess_started_at IS NULL
          OR last_reprocess_started_at < now() - interval '1 hour')
   RETURNING last_reprocess_started_at
   ```
   0 rows → 429 with `code: RATE_LIMITED` and `detail` naming when the next attempt becomes eligible.
3. Begin an asyncpg transaction on a connection from the ingestion pool.
4. Attempt `INSERT INTO reprocessing_locks (user_id) VALUES ($1)`. On `unique_violation` (`23505`): query the K8s API for an active reprocess Job via label selector `grosh.app/job-kind=reprocess,grosh.app/user-id={uuid}`, rollback, and return 409 with `code: REPROCESS_LOCKED` and the running job's identity in `detail`.
5. Build the `V1Job` programmatically. Labels: `app.kubernetes.io/managed-by=grosh-ingestion`, `grosh.app/job-kind=reprocess`, `grosh.app/user-id={uuid}`. Job name `grosh-reprocess-<short-user-id>-<unix-ts>`. `USER_IDS_JSON='["<user-uuid>"]'`, `envFrom: grosh-secrets`, `ttlSecondsAfterFinished: 3600`.
6. Call `BatchV1Api.create_namespaced_job` with a 10-second socket timeout (`K8S_JOB_SUBMIT_TIMEOUT_SECONDS` module constant). The kubernetes client does NOT wrap urllib3 timeouts as `ApiException`; the dispatcher catches both `ApiException` AND `urllib3.exceptions.TimeoutError` and re-raises as `K8sDispatchError`. On any failure the transaction rolls back (lock-row insert undone, rate-limit timestamp reverted), and the endpoint returns 502 with `code: JOB_SUBMISSION_FAILED`.
7. Commit. Return 202 with `JobTriggerResponse(job_id, status_url="/v1/users/{user_id}/reprocess/{job_id}")`.

The rate-limit UPDATE, lock-row INSERT, and Job submission are wrapped in one `async with conn.transaction():` block. The transaction commits only if all three succeed. A failed attempt does not falsely consume the user's hourly quota.

### Admin bulk trigger flow

1. Verify caller is admin → 403 otherwise.
2. Snapshot the target user list:
   - `user_ids` is `null` → `SELECT id FROM users` (single snapshot; new users created after this query are NOT included — deliberate semantic).
   - `user_ids` is a list → use verbatim.
3. Begin a transaction. For each target user, attempt the rate-limit UPDATE then the lock INSERT. If `force == true`, the rate-limit UPDATE drops the time-window predicate: `UPDATE users SET last_reprocess_started_at = now() WHERE id = $1 RETURNING ...`. Rate-limited users land in `skipped` with `reason: RATE_LIMITED`; already-locked users land in `skipped` with `reason: REPROCESS_LOCKED`.
4. If `len(targets) == 0`: rollback, return 202 with `BulkReprocessResponse(job_id=None, status_url=None, skipped=<full list>)`. Client branches on `job_id is None` to skip polling.
5. Otherwise: build the `V1Job` with `USER_IDS_JSON=json.dumps([str(u) for u in targets])`. Labels: `app.kubernetes.io/managed-by=grosh-ingestion`, `grosh.app/job-kind=reprocess`. **No `grosh.app/user-id` label** (the job spans multiple users). Job name `grosh-reprocess-admin-<unix-ts>`.
6. Submit via `BatchV1Api.create_namespaced_job`. On failure: rollback all rate-limit + lock inserts, return 502.
7. Commit. Return 202 with `BulkReprocessResponse(job_id, status_url="/v1/admin/reprocess/{job_id}", skipped=<list>)`.

`force: true` is an admin manual-override. It bypasses the rate-limit check but **still consumes the user's hourly slot** (the UPDATE still writes `now()`). Subsequent force triggers within the hour are also not rate-limited — by design; admin discretion is the constraint.

---

## Pod-side state machine

The K8s Job entrypoint (`python -m grosh_normalization.reprocess_main`) is intentionally thin (~50 lines). It parses env, opens **one dedicated `asyncpg.connect()`** (NOT from a pool — advisory locks are session-scoped and a pool acquire would auto-release on return), and delegates per-user work to the orchestrator. The connection is closed in a `finally` block so the advisory lock is released on abnormal exit.

For each `user_id` in `USER_IDS_JSON`:

1. **Assert lock exists** — `SELECT 1 FROM reprocessing_locks WHERE user_id = $1`. If absent, log `"Reprocessing lock not found for user_id={uuid}; exiting cleanly"` and skip the user. If every user is skipped, the pod exits 0 and the K8s Job ends in `status: succeeded` (the API never submitted us, or the operator cleared the row).
2. **Acquire advisory lock** — `pg_advisory_lock(hashtext('reprocess:' || user_id::text))` on the session connection. Defense in depth against concurrent pod restarts.
3. **Snapshot to backup** — `INSERT INTO reprocessing_backups (user_id, data) VALUES ($1, $snapshot_jsonb)`.
4. **Read rows, reconstruct events** — `SELECT * FROM transactions WHERE user_id = $1 ORDER BY time, id`. Strip `metadata.layer` per the strip rule.
5. **Delete** — `DELETE FROM transactions WHERE user_id = $1`. CASCADE clears `transfer_match_anomalies`; the self-FK `related_transaction_id` is set to `NULL` on remaining rows of the same user (no-op in practice since all of the user's rows are deleted together).
6. **Publish replay events** directly to `normalized_transactions` (bypassing staging — the reprocess job is the lock holder; staging is for non-reprocess events).
7. **Wait for consumer drain** — monitor the enrichment consumer's committed offset until it reaches the highest published offset (polled via Kafka AdminClient).
8. **Verify** — `SELECT id FROM transactions WHERE user_id = $1 AND id = ANY($snapshot_ids)`. All snapshot IDs must be present. New transactions that arrived during the reprocess window (webhook events drained from staging) are excluded — they have IDs not in the snapshot.
9. **Release** — atomically:
   ```sql
   BEGIN;
     DELETE FROM reprocessing_locks WHERE user_id = $1;
     SELECT pg_advisory_unlock(hashtext('reprocess:' || user_id::text));
     NOTIFY reprocess_complete, $1::text;
   COMMIT;
   ```
   The `NOTIFY` wakes the normalization service's drain task.

### Verification failure

If verification fails (snapshot IDs missing from the table):

1. **Restore from backup.** The advisory lock is still held; webhook events continue routing to staging (the `reprocessing_locks` row still exists). The restore INSERTs proceed normally — Postgres advisory locks don't block writes.
2. **Clear the lock row.** Release the advisory lock, NOTIFY drain task.
3. **Alert.** The system returns to known-good state before humans are notified.

**If restore itself fails** (corrupted backup, schema drift, FK violations), the job stops in an inconsistent state: the lock row exists, the advisory lock is released (session is gone), staged events keep accumulating, and the user's `transactions` table is partially restored. **Manual intervention is required.** Recovery: investigate, manually re-restore (or accept partial state), then `DELETE FROM reprocessing_locks WHERE user_id = $1`. Once cleared, the periodic sweep drains pending staged events. If the user's data is materially wrong, notify them out-of-band; reprocess does not auto-page on this path because it would mask the underlying bug.

---

## Concurrency: staging buffer + drain

A race exists between the reprocess job and the system's normal ingestion path:

- A webhook event could be inserted after the snapshot read but deleted by the subsequent bulk DELETE — losing data.
- A webhook event for the reprocessing user could interleave with replayed events, fragmenting a coherent replay into a mix of stale + fresh + replayed rows.
- Webhook events for **other users** sharing the same Kafka partition as the reprocessing user could queue up behind the replay batch, suffering tail latency unrelated to their own data.

The design solves all three with two pieces:

- A **session-scoped advisory lock** held by the reprocess pod for the entire job duration (acquire → snapshot → delete → publish → wait → release).
- A **staging buffer table** on the normalization service side. While a user is locked, the normalization consumer routes that user's normalized events into `staging_normalized_transactions` instead of `normalized_transactions`. After the lock is released, a drain task replays the staged events in order.

Together these decouple the reprocess job from the enrichment consumer entirely. The enrichment consumer never blocks on reprocess; co-tenant users on the same Kafka partition are unaffected.

### Normalization consumer: staging routing

After normalizing a raw event into a `NormalizedTransaction`:

```python
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

The check + write happens in one transaction so the routing decision is consistent: by the time the transaction commits, either the staging row exists OR the Kafka publish has been initiated. Kafka producer commits separately, but the at-least-once contract is preserved by the existing idempotency guard in the enrichment consumer.

### Drain task

Triggered two ways:

1. **`LISTEN reprocess_complete`** — the normalization consumer subscribes on a long-lived asyncpg listener connection at startup. When the reprocess job's `NOTIFY reprocess_complete, '<user_id>'` fires, the listener callback runs.
2. **Periodic sweep, every 60 seconds** — fallback for missed notifications (listener connection blip, consumer restart). Scans for all users with staging rows whose `reprocessing_locks` entry no longer exists.

Drain logic:

```python
async def drain_for_user(user_id: UUID):
    rows = await conn.fetch(
        "SELECT id, payload FROM staging_normalized_transactions "
        "WHERE user_id = $1 ORDER BY created_at",
        user_id,
    )
    for row in rows:
        await producer.send("normalized_transactions", key=user_id, value=row["payload"])
        await conn.execute(
            "DELETE FROM staging_normalized_transactions WHERE id = $1",
            row["id"],
        )
```

Publish-then-delete is the at-least-once order: a crash between publish and delete causes a re-publish on retry, idempotency-guarded by the enrichment consumer's `SELECT EXISTS WHERE id = $1` check.

### Drain age observability

The sweep loop logs the age of the oldest staged row on every iteration:

```
staging_drain.oldest_staged_age_seconds=<int> staging_drain.staged_row_count=<int>
```

If the age exceeds 300 seconds (5 minutes), the log line is emitted at WARN level. Otherwise DEBUG. When the table is empty, the line is `staging_drain.staged_row_count=0` at DEBUG (no age field — the sweep is healthy when there's nothing to do). The threshold is a module-level constant.

### Why staged events arrive after replayed events

Replayed events publish first (during the lock); staged events publish second (after the lock releases). For staged events this means they appear in `normalized_transactions` AFTER replayed events with later transaction times. The enrichment consumer processes them in publish order.

Acceptable because:

- Pipeline layers are designed to be order-independent at the per-event level. Transfer detection's universal fetch finds candidates by time window, not arrival order. Classification is per-row. Currency conversion is per-row.
- Auto-resolve handles the late-arriving partner case: a staged webhook event whose partner was already replayed pairs correctly because the universal fetch sees the committed replayed row.
- The user's UI shows "refreshing" until staging is empty.

The intent is captured: "the staged event was always there, it's the past — we just needed to correct what came before it."

**Note for future stateful pipeline layers.** Today's pipeline layers are per-event. A future ML classification layer using recency features ("looks like another grocery transaction this week") may produce slightly different outputs for staged events than they would have under normal flow, because at drain time the recently-replayed older events are already committed and influence the classifier's view of recent activity. Acceptable trade-off: reprocess regenerates all derived state against current logic and current sibling rows, by design.

### Why this is race-free

- Webhook events for the locked user are routed to staging atomically with the lock check. They cannot reach `normalized_transactions` while the user is locked.
- Replayed events are published by the lock holder while the lock is held. They cannot collide with webhook events for the same user.
- The enrichment consumer never sees the lock; it just consumes `normalized_transactions` at full speed.
- Other users (different `user_id`) are entirely unaffected.
- A reprocess pod crash releases the advisory lock automatically (session-scoped). The orphaned `reprocessing_locks` row is cleared by the next API trigger (PK conflict → 409 surfaces the stale state) or by manual admin DELETE.

### Stale lock recovery

The lock row is a routing signal + status indicator, not the exclusion mechanism. If the pod is OOM-killed:

- Advisory lock releases automatically (session ends).
- `reprocessing_locks` row remains until cleaned up.
- Webhook events for the user keep going to staging (correct — we don't know if reprocess is genuinely stuck or just slow).
- The next API trigger sees the row, queries K8s for a live Job (label selector), and returns 409 with a degraded message if no Job is found. An operator manually clears the row; the periodic sweep drains the queued staging events on the next tick.

### Single-instance normalization service

The staging buffer's order-preservation guarantee (`ORDER BY created_at` in the drain) assumes a **single normalization consumer instance per Kafka partition**. Standard Kafka per-partition single-consumer-instance guarantee — do not deliberately fork.

If horizontal scaling becomes necessary in the future, increase the partition count rather than spawning multiple instances per partition. Per-user ordering is preserved because each user's events always hash to the same partition.

---

## Cascade effects

The DELETE in the pod's step 5 cascades through any FK relationships that target `transactions(id)`:

- Self-FK `related_transaction_id` (`ON DELETE SET NULL`): partners outside the deleted batch get their reference nulled. In practice, all of a user's rows are deleted together so this is a no-op.
- Any table with `ON DELETE CASCADE` on `transaction_id`: rows are cleared automatically. Currently this includes `transfer_match_anomalies`. Future tables added by other layers should also use `ON DELETE CASCADE` so reprocess doesn't need per-table cleanup logic.

Pipeline layers regenerate their derived state during replay — reprocess does not need to know which tables exist or what their layers do.

---

## Idempotency

The enrichment consumer has a per-row idempotency guard (`SELECT EXISTS WHERE id = $1` early in the per-event handler). It must NOT fire during reprocessing — the rows were just deleted. Since reprocess deletes before publishing, the guard sees no existing row on replay events and processes normally. No special flag needed.

The same guard applies to staged events drained after the lock release: they're new inserts from the consumer's perspective. The drain task's at-least-once publish + idempotency guard combination handles drain retries cleanly (a republish on retry fires the guard and exits without effect).

**At-least-once + transfer detection interaction.** A staged event A may be republished (drain crash between Kafka publish and staging-row delete). Sequence: A publishes → consumer claims pair (A, partner B) → drain crashes → drain restarts and republishes A → consumer's idempotency guard sees A already exists → no-op. The pair (A, B) is unaffected; the retry is idempotent. If A's true partner is also a staged event arriving later, that partner's universal fetch will find A correctly committed (regardless of whether A's first publish paired or not), so pairing is preserved across drain retries.

---

## When to use

- New pipeline layer added (transfer detection update, ML classifier deployment).
- Existing layer logic changed (description guard updated, rate fallback improved).
- Bug fix that affected stored results.
- NOT for routine backfill — use the Monobank historical backfill (`POST /v1/monobank/accounts/{id}/backfill`) for fetching new data from the bank API.

## What this is NOT

- Not a reconciliation job (doesn't selectively re-pair transfers).
- Not a Monobank backfill (doesn't call external APIs).
- Not a migration (doesn't run SQL transformations).

It's a "backup, nuke, and replay from stored truth" operation. The stored row IS the source of truth; the pipeline layers are deterministic transformations applied on top.

---

## Decoupling boundary

This ADR describes an **operational mechanism**. It must NOT contain knowledge of any specific pipeline layer's internals. Conversely, pipeline layers must NOT contain knowledge of reprocessing.

The contract:

- **Pipeline layers** (currency conversion, transfer detection, classification, future layers) write outputs as they normally do — to the `transactions` row and to their own sub-namespace under `metadata.layer.<name>`. They are pure business logic; they know nothing about reprocess.
- **Normalizers** (`monobank`, `manual`, future `pumb`/`revolut`) write source-original metadata under `metadata.source`. They know nothing about reprocess.
- **The reprocess job** has one structural rule: drop `metadata.layer`, preserve `metadata.source`. The wrapping convention does the work that a strip list would otherwise have to do.
- **Reprocess introduces** the `reprocessing_locks`, `reprocessing_backups`, and `staging_normalized_transactions` tables, plus the LISTEN/NOTIFY drain in the normalization consumer. None of this is required for normal pipeline operation; pipeline layers can be developed, tested, and deployed without any reprocess infrastructure being present.

The wrapping convention is the **only** coupling point between reprocess and the rest of the system. There are no layer-specific code paths inside reprocess, and no reprocess-aware code paths inside any layer.

---

## Deployment order for layer changes

When a pipeline layer changes its output (new metadata fields, changed enum values, new anomaly types):

1. **Deploy the new layer code.** The layer emits new outputs going forward (under its own `metadata.layer.<name>` sub-namespace). Existing rows still have old-format outputs.
2. **Run reprocess** for affected users. CASCADE-clears layer-derived rows; reprocess strips `metadata.layer` before reconstruction; the layer regenerates everything against fresh state.
3. **Apply schema cleanup migrations** (e.g. drop unused enum values that the new layer no longer emits). These run last because step 2 may still emit the old values for rows that haven't been reprocessed yet.

Skipping or reordering steps risks rows referencing dropped enum values.
