"""Add last_polled_at to currency_rates; create rate_source_config table

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-17

Extends the SCD Type 2 currency_rates table with a last_polled_at column that
lets the consumer distinguish "rate unchanged between polls" from "system was
down during this window". Creates rate_source_config — a small global config
table that defines the fallback chain and maximum staleness tolerance for each
rate source.

> **What is SCD Type 2 staleness detection?**
> SCD Type 2 (Slowly Changing Dimension) keeps history by closing old rows
> (valid_to = now()) and inserting a new one when a value changes. last_polled_at
> is updated in place when the rate stays the same between polls. If there is a
> gap larger than max_staleness_seconds between last_polled_at and a transaction's
> time, the consumer knows the polling loop was down and can fall back to the next
> source in the chain.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | Sequence[str] | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Add last_polled_at to currency_rates
    # ------------------------------------------------------------------
    # Add with a DEFAULT so the NOT NULL constraint is satisfied immediately
    # for existing rows, then backfill from valid_from so historical rows
    # reflect the timestamp when the rate was first observed (best approximation).
    op.execute("""
        ALTER TABLE currency_rates
            ADD COLUMN last_polled_at TIMESTAMPTZ NOT NULL DEFAULT now();
    """)
    op.execute("UPDATE currency_rates SET last_polled_at = valid_from;")

    # ------------------------------------------------------------------
    # rate_source_config
    # ------------------------------------------------------------------
    # > **What is the fallback chain?**
    # > The consumer tries each source in priority order. If the highest-priority
    # > source (monobank) has a gap older than its max_staleness_seconds, the
    # > consumer falls back to the next source (nbu). The self-referencing FK
    # > lets us express arbitrary-depth chains as a linked list.
    op.execute("""
        CREATE TABLE rate_source_config (
            source               TEXT PRIMARY KEY,
            fallback_source      TEXT REFERENCES rate_source_config(source),
            max_staleness_seconds INT  NOT NULL
        );
    """)

    # nbu has no fallback; insert it first to satisfy the FK when monobank
    # references it. 90000s = 25 hours (NBU publishes daily rates).
    op.execute("""
        INSERT INTO rate_source_config (source, fallback_source, max_staleness_seconds)
        VALUES ('nbu', NULL, 90000);
    """)

    # monobank falls back to nbu. 7200s = 2 hours.
    op.execute("""
        INSERT INTO rate_source_config (source, fallback_source, max_staleness_seconds)
        VALUES ('monobank', 'nbu', 7200);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS rate_source_config;")
    op.execute("ALTER TABLE currency_rates DROP COLUMN IF EXISTS last_polled_at;")
