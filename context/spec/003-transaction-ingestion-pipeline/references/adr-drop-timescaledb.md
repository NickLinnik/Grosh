# ADR: Drop TimescaleDB

Status: **Draft** — decided, not yet implemented.

---

## Problem

The project uses TimescaleDB for three things:

1. **Hypertable** on `transactions` (time-based chunk partitioning)
2. **Continuous aggregate** `monthly_aggregates` (pre-computed monthly rollups with real-time overlay)
3. **Hypertable + retention policy** on `revoked_tokens` (auto-purge expired JWTs)

After debugging the continuous aggregate (watermark footgun, invisible data after backfill, RLS disable/enable dance) and auditing the value of the hypertable against our actual access patterns, the cost/benefit is clearly negative.

**Costs we're paying:**

| Cost | Impact |
|------|--------|
| Docker image: 5.4 GB (`timescaledb-ha:pg16`) vs ~450 MB (`pgvector/pgvector:pg16`) | 12x image size, slower pulls, more disk on Hetzner CX31 |
| Composite PK `(id, time)` on `transactions` | Every FK, dedup, and `ON CONFLICT` needs both columns |
| `related_transaction_id` can't be a real FK | Referencing a composite PK from a bare UUID is impossible |
| Future tables (`transaction_embeddings`, `splits`) inherit the FK tax | Compound references everywhere |
| Continuous aggregate watermark model | Backfilled data invisible until refresh covers it |
| No joins, no `array_agg` in continuous aggregates | Can't enrich rollups with category/account names |
| RLS disable/enable dance for CAGG DDL | Fragile migration ceremony |
| Postgres version lag | TimescaleDB lags upstream by months (pg16 vs pg17) |
| `start_offset: 10 years` hack | Brute-force rebuild loop disguised as incremental materialization |

**Value we're getting:**

| Feature | Actual value at our scale |
|---------|--------------------------|
| Chunk exclusion on time-range queries | Zero — `INDEX (user_id, time DESC)` is strictly more selective |
| Compression | Negative — we UPDATE transactions (reclassify, split, mark as transfer) |
| Retention policy on `revoked_tokens` | Genuine but replaceable with `pg_cron` (2 lines of SQL) |
| `time_bucket()` | Syntactic sugar over `date_trunc()` |

---

## Decision

**Drop TimescaleDB entirely.** Replace with plain PostgreSQL + `pgvector` + `pg_cron`.

### 1. `transactions` becomes a regular table

```sql
CREATE TABLE transactions (
    id  UUID  PRIMARY KEY DEFAULT gen_random_uuid(),
    ...
);
```

**What this unlocks:**
- `REFERENCES transactions(id)` for `related_transaction_id`, `transaction_embeddings`, future splits
- `ON CONFLICT (id) DO NOTHING` for dedup (no composite key needed)
- `UNIQUE (account_id, source_id)` as a proper business-key dedup index
- Standard ORM patterns (single-column PK)
- No chunk propagation overhead on schema changes

**Conflict handling:** the consumer uses `ON CONFLICT (id) DO NOTHING` as the primary idempotency guard. The `UNIQUE (account_id, source_id)` constraint is a safety net — a violation there indicates a real bug (same bank transaction ingested with two different UUIDs). The consumer does NOT add `ON CONFLICT (account_id, source_id) DO NOTHING` — constraint violations on this index should crash loudly so the bug is visible.

**What we keep:**
- `INDEX (user_id, time DESC)` — the real performance driver for per-user time-range queries
- `INDEX (user_id, account_id, time DESC)` — per-account queries
- All RLS policies unchanged

### 2. Drop continuous aggregate entirely

The `monthly_aggregates` view is deleted. Monthly (and all other time-window) aggregations become compute-on-read queries.

**Replacement — aggregation API endpoint:**

```
GET /transactions/aggregates
    ?currency=UAH,USD,EUR           (optional, comma-separated, default: all three)
    &from=2025-01-01                (optional, default: all history)
    &to=2025-12-31                  (optional, default: now)
    &bucket=month                   (optional: day | week | month | quarter | year, default: month)
    &fields=income,expense,delta    (optional, comma-separated, default: all three)
```

> **Update (2026-05-06, Slice 15e):** `currency` and `fields` were originally drafted as comma-separated and implemented that way in Slices 15/15c. Slice 15e migrated them to **repeated-key + `StrEnum`** (`?currency=UAH&currency=USD&fields=income&fields=delta`) — the project-wide convention now codified in `CLAUDE.md`. Reasoning: native FastAPI parsing, proper OpenAPI array schema, typed generated SDK clients, and elimination of hand-rolled `_parse_csv_param`-style helpers. The example block above is preserved as the original ADR proposal; the live API reflects the post-15e form.

Response:
```json
{
  "bucket": "month",
  "items": [
    {
      "period_start": "2025-01-01T00:00:00Z",
      "currencies": {
        "UAH": {
          "total_income_cents": 520000,
          "total_expense_cents": 480000,
          "delta_cents": 40000,
          "converted_pct": 100.0
        },
        "USD": {
          "total_income_cents": 12000,
          "total_expense_cents": 11500,
          "delta_cents": 500,
          "converted_pct": 96.0
        }
      }
    }
  ]
}
```

The `currencies` dict contains only the requested currencies. Default (no `currency` param) returns all three. This lets the frontend fetch everything for a dashboard in one round-trip.

`converted_pct` is the percentage of transactions in the bucket that have a non-NULL value for that currency's `amount_*_cents` column. When < 100%, the frontend can link to the transaction list with a filter to show the unconverted ones: `GET /transactions?from=...&to=...&unconverted_currency=USD`.

> **Update (2026-05-06, Slice 15d):** The `unconverted_currency` query param described here was implemented in Slice 15, hardened in Slice 15c (silent-skip → 422, integration tests), then removed in Slice 15d before any frontend was built. Reasoning: the param made one endpoint operate in two semantically different modes ("list my transactions" vs. "show me data-quality holes"), and the right drill-down design (server filter / dedicated endpoint / client-side highlighting / `conversion_status` column) depends on frontend access patterns we don't yet have. `converted_pct` itself stays as the load-bearing quality metric. The drill-down decision is deferred — revisit when the frontend exists and a real need appears.

**Implementation — single SQL query (per currency):**

```sql
SELECT
    date_trunc($bucket, time) AS period_start,
    COALESCE(SUM(amount_{currency}_cents) FILTER (WHERE transaction_type = 'income'),  0) AS total_income_cents,
    COALESCE(SUM(amount_{currency}_cents) FILTER (WHERE transaction_type = 'expense'), 0) AS total_expense_cents,
    COALESCE(SUM(amount_{currency}_cents) FILTER (WHERE transaction_type = 'income'),  0)
  - COALESCE(SUM(amount_{currency}_cents) FILTER (WHERE transaction_type = 'expense'), 0) AS delta_cents,
    ROUND(100.0 * COUNT(amount_{currency}_cents) / COUNT(*), 1) AS converted_pct
FROM transactions
WHERE user_id = $1
  AND transaction_type IN ('income', 'expense')
  AND time >= COALESCE($from, '-infinity'::timestamptz)
  AND time < COALESCE($to, 'infinity'::timestamptz)
GROUP BY period_start
ORDER BY period_start
```

**Design notes:**

- **Scope:** the `WHERE` clause always filters to `income` + `expense` (transfers and checks are excluded from financial aggregation by definition). The SQL always computes all three values (income, expense, delta) plus conversion coverage.
- **`converted_pct`**: `COUNT(column)` counts non-NULL values; `COUNT(*)` counts all rows. The ratio gives the percentage of transactions successfully converted into the requested currency. When < 100%, the frontend links to the existing transaction list endpoint with `?unconverted_currency=USD&from=...&to=...` to show which ones failed.
- **`fields` parameter** (default: `['income', 'expense', 'delta']`): controls which of the computed values are included in the JSON response. This is a presentation concern — the query runs the same regardless. Allows the frontend to request only `delta` for sparklines or only `income,expense` for bar charts.
- **Future direction:** if a new type like `refund` is added that should count toward delta, the solution is either a `direction` column on transactions (`inflow`/`outflow`) or adding it to the `WHERE IN (...)` list with appropriate FILTER bucketing. Not a v1 concern.
- **`COALESCE($from, '-infinity')` / `COALESCE($to, 'infinity')`**: avoids `OR IS NULL` which defeats index range scans. The planner pushes the constant into the index scan cleanly.

`converted_pct` exposes conversion completeness without inflating the response with potentially thousands of IDs. The frontend drills down via the standard transaction list endpoint with `?unconverted_currency=X` to get the actual rows. This supersedes the old `null_rate_count` workaround forced by continuous aggregate limitations.

**Timezone handling:** `date_trunc` operates in the session timezone. To produce buckets aligned with the user's local sense of "January," the query uses `AT TIME ZONE`:

```sql
date_trunc($bucket, time AT TIME ZONE $user_tz) AS period_start
```

The user's timezone is stored in `user_settings` (default: `'UTC'`, auto-detected from browser on first load). This ensures a transaction at 23:30 local time on Jan 31st falls in January, not February.

**Multi-currency in one query:** the per-currency SQL above is shown for clarity. In practice, all requested currencies are computed in a single query:

```sql
SELECT
    date_trunc($bucket, time AT TIME ZONE $user_tz) AS period_start,
    -- UAH
    COALESCE(SUM(amount_uah_cents) FILTER (WHERE transaction_type = 'income'),  0) AS uah_income_cents,
    COALESCE(SUM(amount_uah_cents) FILTER (WHERE transaction_type = 'expense'), 0) AS uah_expense_cents,
    ROUND(100.0 * COUNT(amount_uah_cents) / COUNT(*), 1) AS uah_converted_pct,
    -- USD
    COALESCE(SUM(amount_usd_cents) FILTER (WHERE transaction_type = 'income'),  0) AS usd_income_cents,
    COALESCE(SUM(amount_usd_cents) FILTER (WHERE transaction_type = 'expense'), 0) AS usd_expense_cents,
    ROUND(100.0 * COUNT(amount_usd_cents) / COUNT(*), 1) AS usd_converted_pct,
    -- EUR
    COALESCE(SUM(amount_eur_cents) FILTER (WHERE transaction_type = 'income'),  0) AS eur_income_cents,
    COALESCE(SUM(amount_eur_cents) FILTER (WHERE transaction_type = 'expense'), 0) AS eur_expense_cents,
    ROUND(100.0 * COUNT(amount_eur_cents) / COUNT(*), 1) AS eur_converted_pct
FROM transactions
WHERE user_id = $1
  AND transaction_type IN ('income', 'expense')
  AND time >= COALESCE($from, '-infinity'::timestamptz)
  AND time < COALESCE($to, 'infinity'::timestamptz)
GROUP BY period_start
ORDER BY period_start
```

One index scan, one grouping pass, all currencies. The repo maps columns into the nested `currencies` dict, omitting unrequested currencies.

With `INDEX (user_id, time DESC)`, this scans only the requesting user's rows. At 50 tx/month × 10 years = 6000 rows, any aggregation pattern completes in under 1ms.

**Why this is better than the continuous aggregate:**
- Correct by construction — no watermark, no staleness after backfill or reclassification
- Supports any bucket size (day, week, month, quarter, year) without multiple views
- Supports arbitrary date ranges and single-currency selection
- Supports joins (category breakdowns, account filtering) trivially
- Supports `array_agg`, window functions, CTEs in future extensions
- Zero migration ceremony (no RLS disable/enable, no refresh policies)

**Future path if aggregation becomes slow:**
If per-user query latency ever becomes a problem (it won't at <100K rows per user), the correct solution is a trigger-maintained `monthly_summaries` table — atomic, transactional, instant on write, no batch window. This is the event-driven approach. Not planned for v1.

### 3. `revoked_tokens` becomes a regular table with `pg_cron` TTL

```sql
CREATE TABLE revoked_tokens (
    jti        UUID        PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_revoked_tokens_expires_at ON revoked_tokens (expires_at);
```

TTL via `pg_cron`:
```sql
-- pg_cron extension must be created in the database named in cron.database_name.
-- Since cron.database_name = 'grosh', CREATE EXTENSION runs here (not in 'postgres').
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule_in_database(
    'purge-revoked-tokens',
    '*/5 * * * *',
    $$DELETE FROM revoked_tokens WHERE expires_at < now()$$,
    'grosh'
);
```

`schedule_in_database` explicitly targets the `grosh` database — no ambiguity about execution context. Every 5 minutes, Postgres deletes expired rows. The table holds at most ~20 rows at any time (one per logout within the 15-minute access token TTL). `pg_cron` is a ~2 MB extension — negligible overhead.

### 4. Docker image

Switch from `timescale/timescaledb-ha:pg16` (5.4 GB) to a custom image:

```dockerfile
FROM pgvector/pgvector:pg16
RUN apt-get update && apt-get install -y postgresql-16-cron && rm -rf /var/lib/apt/lists/*
```

~450 MB total. Ships `pgvector` (needed for ML classifier) and `pg_cron` (needed for TTL).

`pg_cron` requires `shared_preload_libraries` set before postmaster starts. Pass via compose command args (avoids baking config into the image):

```yaml
# docker-compose.yml
services:
  postgres:
    image: grosh-postgres:local
    command: >
      postgres
        -c shared_preload_libraries=pg_cron
        -c cron.database_name=grosh
```

Sanity check after deploy: `SELECT * FROM cron.job;` from the `grosh` database should list the purge job. If empty, the `-c` flags weren't applied.

---

## Local data preservation

The local dev DB has ~40 months of backfilled transactions and currency rates. Re-backfilling from Monobank takes half a day (rate-limited). Strategy:

1. **Verify no constraint violations** before dumping:
   ```sql
   -- New PK is (id) — check no duplicate IDs exist
   SELECT id, COUNT(*) FROM transactions GROUP BY id HAVING COUNT(*) > 1;
   -- New unique is (account_id, source_id) — check no duplicates
   SELECT account_id, source_id, COUNT(*) FROM transactions GROUP BY account_id, source_id HAVING COUNT(*) > 1;
   ```
   Both should return empty. If duplicates exist: inspect manually, keep the row with the latest `created_at`, delete the other.
2. `pg_dump --data-only --format=plain -t transactions -t currency_rates -t accounts -t bank_integrations -f dump.sql` (text-format SQL dump)
3. `docker compose down -v` (nuke the volume — **mandatory**, not optional; old DB has CAGG/hypertable state that new migrations won't clean up)
4. Apply modified migrations on fresh DB
5. `psql -d grosh -f dump.sql` (restore data into new schema)

One-time manual procedure before merge.

---

## Migration changes

All migrations are on an unmerged branch — modify in place (no new migration needed).

### 0004 (transaction pipeline)

- Remove `CREATE EXTENSION IF NOT EXISTS timescaledb`
- Remove `SELECT create_hypertable('transactions', 'time')`
- Change PK: `PRIMARY KEY (id, time)` → `PRIMARY KEY (id)`
- Change dedup index: `UNIQUE (account_id, source_id, time)` → `UNIQUE (account_id, source_id)`
- Add self-FK: `related_transaction_id UUID REFERENCES transactions(id) ON DELETE SET NULL`

### 0005 (monthly aggregates)

- Delete entirely. The aggregation API replaces it.
- Update 0006's `down_revision` to point to `"0004"` (skip the deleted file in Alembic's chain).

### 0009 (revoked tokens)

- Remove `SELECT create_hypertable('revoked_tokens', 'expires_at')`
- Remove `SELECT add_retention_policy(...)`
- Add `CREATE EXTENSION IF NOT EXISTS pg_cron`
- Add `SELECT cron.schedule_in_database('purge-revoked-tokens', '*/5 * * * *', $$DELETE FROM revoked_tokens WHERE expires_at < now()$$, 'grosh')`
- Change PK: add `PRIMARY KEY (jti)` explicitly (was implicit via hypertable)

---

## Interaction with other ADRs

### Consumer pipeline architecture (`adr-consumer-pipeline-architecture.md`)

No direct TimescaleDB dependencies. Minor simplification: `ON CONFLICT (id, time)` → `ON CONFLICT (id)` in the persistence layer.

### Transfer detection (`adr-transfer-detection.md`)

- `related_transaction_id` becomes a proper FK: `REFERENCES transactions(id) ON DELETE SET NULL`
- Anomaly table FK: `transaction_id UUID REFERENCES transactions(id) ON DELETE CASCADE` — was already correct (anomaly table was never a hypertable)
- Claim lock queries unchanged (they query by filters, not by PK)
- Transfer detection index: remove `time` from PK-dependent constraints

### Transaction reprocessing (`adr-transaction-reprocessing.md`)

- "Why not a shadow table" section: remove TimescaleDB-specific bullet points (`create_hypertable()` setup, continuous aggregates bound by OID)
- Consumer idempotency guard: `ON CONFLICT (id) DO NOTHING` instead of `ON CONFLICT (id, time) DO NOTHING`
- Cascade behavior of `related_transaction_id` is now enforced by the DB (real FK) rather than application-level cleanup

---

## What we lose

Nothing meaningful:
- `time_bucket()` → `date_trunc()` (identical for standard calendar periods)
- Chunk exclusion → already irrelevant (per-user index is more selective)
- Compression → incompatible with our UPDATE-heavy workload
- Retention policy → replaced by `pg_cron` (same effect, simpler mechanism)

---

## Testing

After migration changes:
1. Run full Alembic migration suite on a fresh database
2. Insert transactions spanning multiple years, verify aggregation query returns correct results for all bucket sizes
3. Verify `related_transaction_id` FK constraint works (insert pair, delete one, confirm SET NULL)
4. Verify `pg_cron` purge job executes (insert expired token, wait 5 min, confirm deletion)
5. Verify dedup index prevents duplicate `(account_id, source_id)` combinations
