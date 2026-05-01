"""Monthly continuous aggregates on the transactions hypertable

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-10

Creates a TimescaleDB continuous aggregate `monthly_aggregates` that rolls up
income, expense, and net-delta totals per user per calendar month, with separate
columns for UAH, USD, and EUR (denormalized from the amount_*_cents columns added
in 0004). Adds an automated refresh policy (1-hour schedule, 3-month lookback) and
enables real-time aggregation so queries merge materialized buckets with recent
un-materialized rows.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | Sequence[str] | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Continuous aggregate
    # ------------------------------------------------------------------
    # > **What is a TimescaleDB continuous aggregate?**
    # > A materialized view that TimescaleDB refreshes incrementally —
    # > only newly changed time buckets are recomputed, not the whole table.
    # > This makes month-level rollups fast even on millions of rows.
    #
    # TimescaleDB rejects CREATE MATERIALIZED VIEW (continuous) on any
    # hypertable that has Row-Level Security enabled, because the background
    # refresh worker runs without a session context and cannot satisfy row
    # policies. The workaround is to disable RLS for the duration of the DDL
    # and then re-enable it. The aggregate itself is safe: application queries
    # against monthly_aggregates are always filtered by user_id in the WHERE
    # clause, enforced at the API layer; the view only exposes rolled-up
    # totals that already include user_id as a grouping key.
    # TimescaleDB also checks for RLS when enabling real-time aggregation
    # (ALTER MATERIALIZED VIEW ... SET timescaledb.materialized_only = false).
    # Keep RLS disabled for the entire TimescaleDB DDL block and re-enable
    # after all three operations complete.
    op.execute("ALTER TABLE transactions DISABLE ROW LEVEL SECURITY;")

    op.execute("""
        CREATE MATERIALIZED VIEW monthly_aggregates
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 month', time) AS month,
            user_id,
            -- UAH
            COALESCE(SUM(amount_uah_cents)
                FILTER (WHERE transaction_type = 'income'),  0) AS total_income_uah_cents,
            COALESCE(SUM(amount_uah_cents)
                FILTER (WHERE transaction_type = 'expense'), 0) AS total_expense_uah_cents,
            COALESCE(SUM(amount_uah_cents)
                FILTER (WHERE transaction_type = 'income'),  0)
          - COALESCE(SUM(amount_uah_cents)
                FILTER (WHERE transaction_type = 'expense'), 0) AS delta_uah_cents,
            -- USD
            COALESCE(SUM(amount_usd_cents)
                FILTER (WHERE transaction_type = 'income'),  0) AS total_income_usd_cents,
            COALESCE(SUM(amount_usd_cents)
                FILTER (WHERE transaction_type = 'expense'), 0) AS total_expense_usd_cents,
            COALESCE(SUM(amount_usd_cents)
                FILTER (WHERE transaction_type = 'income'),  0)
          - COALESCE(SUM(amount_usd_cents)
                FILTER (WHERE transaction_type = 'expense'), 0) AS delta_usd_cents,
            -- EUR
            COALESCE(SUM(amount_eur_cents)
                FILTER (WHERE transaction_type = 'income'),  0) AS total_income_eur_cents,
            COALESCE(SUM(amount_eur_cents)
                FILTER (WHERE transaction_type = 'expense'), 0) AS total_expense_eur_cents,
            COALESCE(SUM(amount_eur_cents)
                FILTER (WHERE transaction_type = 'income'),  0)
          - COALESCE(SUM(amount_eur_cents)
                FILTER (WHERE transaction_type = 'expense'), 0) AS delta_eur_cents,
            -- NULL rate counts (data quality: transactions missing conversion)
            COUNT(*) FILTER (WHERE amount_uah_cents IS NULL) AS null_uah_count,
            COUNT(*) FILTER (WHERE amount_usd_cents IS NULL) AS null_usd_count,
            COUNT(*) FILTER (WHERE amount_eur_cents IS NULL) AS null_eur_count
        FROM transactions
        WHERE transaction_type NOT IN ('transfer', 'check')
        GROUP BY month, user_id
        WITH NO DATA;
    """)

    # ------------------------------------------------------------------
    # Real-time aggregation
    # ------------------------------------------------------------------
    # > **What is real-time aggregation?**
    # > When enabled, TimescaleDB transparently unions the materialized
    # > buckets with un-materialized recent rows at query time, so the view
    # > always returns up-to-date results without a manual REFRESH call.
    op.execute(
        "ALTER MATERIALIZED VIEW monthly_aggregates"
        " SET (timescaledb.materialized_only = false);"
    )

    # ------------------------------------------------------------------
    # Refresh policy
    # ------------------------------------------------------------------
    # > **What is a continuous aggregate policy?**
    # > A TimescaleDB background job that periodically calls REFRESH on the
    # > view for the window [now() - start_offset, now() - end_offset].
    # > The watermark advances on each refresh. Buckets before the watermark
    # > are served only from the materialized store — backfilled data with
    # > old timestamps stays invisible until a refresh covers those buckets.
    # > start_offset = 10 years ensures the hourly refresh always reaches
    # > any realistic backfill depth. At ~3-user scale this is negligible.
    # > end_offset = 1 hour avoids refreshing in-flight writes.
    op.execute("""
        SELECT add_continuous_aggregate_policy('monthly_aggregates',
            start_offset      => INTERVAL '10 years',
            end_offset        => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour');
    """)

    op.execute("ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;")


def downgrade() -> None:
    # Policy must be removed before the view can be dropped.
    op.execute(
        "SELECT remove_continuous_aggregate_policy('monthly_aggregates', if_exists => true);"
    )
    # RLS must be disabled during DROP for the same reason as CREATE.
    op.execute("ALTER TABLE transactions DISABLE ROW LEVEL SECURITY;")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS monthly_aggregates;")
    op.execute("ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;")
