"""CI invariant: every user-scoped table has RLS enabled with at least one policy.

Prevents the most common production RLS failure mode: a contributor adds
a table with a `user_id` column and forgets to wire up `ENABLE ROW LEVEL
SECURITY` + a `CREATE POLICY` statement. Without this gate, the new table
silently allows cross-user reads/writes by every authenticated session.

What this test does NOT catch:

1. Partitioned tables whose parent declares `user_id` but whose partition
   leaves diverge in RLS state (the test reads `pg_class` for the named
   table only).
2. Tables that scope per-user by a column other than `user_id` (`owner_id`,
   `subject_id`, `member_id`, etc.) — the column-name heuristic does not
   enumerate them.
3. Compound-key tables where `user_id` is part of a composite primary key
   but the policy predicate does not match.
4. Tables that should be globally readable — those belong in `RLS_EXEMPT`
   with a non-empty justification, not be missed by the test.

This is the common-case safety gate, not an exhaustive RLS audit. Code
review remains the safety net for non-standard table shapes.
"""

import asyncpg
import pytest

# Tables that intentionally lack RLS or policies despite having a user_id
# column. Each entry must carry a non-empty justification string. Empty
# justifications fail the test below — adding an exemption forces the
# author to state why.
RLS_EXEMPT: dict[str, str] = {
    "refresh_tokens": (
        "Auth-layer token. Lookup is gated by the (token_hash UNIQUE) index "
        "and grosh_api writes/reads under its own role; no end-user query "
        "ever touches this table directly. RLS would add no boundary."
    ),
    "reprocessing_locks": (
        "Operational lock co-owned by ingestion (INSERT) and normalization "
        "(DELETE). Both services use grosh_ingestion / grosh_consumer with "
        "no user-session context. Lock-ownership is per-user via PRIMARY "
        "KEY (user_id); application-layer routing enforces user scoping. "
        "See CLAUDE.md data-ownership matrix."
    ),
    "reprocessing_backups": (
        "Operational snapshot written by the normalization reprocess job "
        "under grosh_consumer (BYPASSRLS). Never queried under a "
        "user-session context. Retention is enforced by the pg_cron job "
        "in migration 0015."
    ),
    "staging_normalized_transactions": (
        "Drain queue between normalization and enrichment. Written/read "
        "only by grosh_consumer (BYPASSRLS). No end-user query path. "
        "Per-user routing is enforced by the staging drain service."
    ),
    "network_members": (
        "Family-network membership table. RLS will be designed during "
        "Phase 4 (Family Network) — sharing model is not yet implemented. "
        "Current schema is unreachable in production until Phase 4 ships."
    ),
}


@pytest.mark.asyncio
async def test_every_user_scoped_table_has_rls_with_policy(
    db_pool: asyncpg.Pool,
) -> None:
    """Fails if any public.* table with a user_id column lacks RLS or policies."""
    async with db_pool.acquire() as conn:
        # Allowlist hygiene check: every exemption must justify itself.
        empty_justifications = [
            table for table, reason in RLS_EXEMPT.items() if not reason.strip()
        ]
        assert not empty_justifications, (
            "RLS_EXEMPT entries must carry a non-empty justification. "
            f"Empty entries: {empty_justifications}"
        )

        # Enumerate every public.* table with a column named user_id.
        rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND column_name = 'user_id'
            ORDER BY table_name
            """
        )
        user_scoped_tables = [r["table_name"] for r in rows]

        offenders: list[str] = []
        for table in user_scoped_tables:
            if table in RLS_EXEMPT:
                continue
            row = await conn.fetchrow(
                """
                SELECT
                    c.relrowsecurity,
                    (
                        SELECT count(*)
                        FROM pg_policies p
                        WHERE p.schemaname = 'public'
                          AND p.tablename = c.relname
                    )::int AS policy_count
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = $1
                  AND c.relkind = 'r'
                """,
                table,
            )
            if row is None:
                offenders.append(f"{table} (missing from pg_class — schema drift?)")
                continue
            if not row["relrowsecurity"]:
                offenders.append(f"{table} (no RLS)")
            elif row["policy_count"] == 0:
                offenders.append(f"{table} (no policies)")

        assert not offenders, (
            "Tables with a `user_id` column must have RLS enabled and at least "
            "one policy. Either fix the table via a migration, or add it to "
            "RLS_EXEMPT with a non-empty justification.\n"
            "Offenders:\n  - " + "\n  - ".join(offenders)
        )
