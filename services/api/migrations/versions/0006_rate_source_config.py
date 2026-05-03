"""Create rate_source_config table and add currency_rates lookup indexes

Revision ID: 0006
Revises: 0004
Create Date: 2026-04-17

Creates rate_source_config — a small global config table that defines the
fallback chain for each rate source. Each source may specify a fallback_source
(the next source to try when this one lacks a rate) and base_currencies (the
currencies this source prices everything against, used for pivot selection in
multi-hop conversions).

> **What is the fallback chain?**
> The consumer tries each source in priority order. If the highest-priority
> source (monobank) has no rate for a given pair, the consumer falls back to
> the next source (nbu). The self-referencing FK lets us express arbitrary-depth
> chains as a linked list.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | Sequence[str] | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # rate_source_config
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE rate_source_config (
            source                TEXT PRIMARY KEY,
            fallback_source       TEXT REFERENCES rate_source_config(source),
            base_currencies       TEXT[] NOT NULL DEFAULT '{}'
        );
    """)

    # nbu has no fallback; insert it first to satisfy the FK when monobank
    # references it.
    op.execute("""
        INSERT INTO rate_source_config
            (source, fallback_source, base_currencies)
        VALUES ('nbu', NULL, '{UAH}');
    """)

    # monobank falls back to nbu.
    op.execute("""
        INSERT INTO rate_source_config
            (source, fallback_source, base_currencies)
        VALUES ('monobank', 'nbu', '{UAH}');
    """)

    # ------------------------------------------------------------------
    # Indexes for rate lookup queries
    # ------------------------------------------------------------------
    # Full index for closest-rate fallback searches across all historical rates.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_currency_rates_lookup
            ON currency_rates (currency_from, currency_to, source, valid_from);
    """)

    # Partial index covering only poll-based rows (last_polled_at IS NOT NULL).
    # Used by the consumer's freshness check and proximity ranking.
    op.execute("""
        CREATE INDEX idx_currency_rates_polled
            ON currency_rates (currency_from, currency_to, source, last_polled_at)
         WHERE last_polled_at IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_currency_rates_polled;")
    op.execute("DROP INDEX IF EXISTS idx_currency_rates_lookup;")
    op.execute("DROP TABLE IF EXISTS rate_source_config;")
