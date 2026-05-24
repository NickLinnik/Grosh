"""Create staging_normalized_transactions table for reprocessing buffer

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-10

Holds normalized events for users who are mid-reprocess. The normalization
consumer routes events here when the user has a reprocessing_locks row; a
drain task replays them to normalized_transactions after the lock is released.

No FK on user_id — this is an operational queue, not a relational entity.
Rows live for seconds-to-minutes during a reprocess and are explicitly
drained, never long-lived enough to need referential integrity to users.

No RLS — internal-only table written by the consumer, which bypasses RLS.

See adr-transaction-reprocessing.md "Concurrency" for the full design.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0009"
down_revision: str | Sequence[str] | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE staging_normalized_transactions (
            id          UUID        PRIMARY KEY DEFAULT uuidv7(),
            user_id     UUID        NOT NULL,
            payload     JSONB       NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE INDEX idx_staging_user_created
            ON staging_normalized_transactions (user_id, created_at);
    """)

    op.execute("""
        GRANT SELECT, INSERT, DELETE
            ON staging_normalized_transactions
            TO grosh_consumer;
    """)


def downgrade() -> None:
    op.execute("""
        REVOKE SELECT, INSERT, DELETE
            ON staging_normalized_transactions
            FROM grosh_consumer;
    """)

    op.execute("""
        DROP INDEX IF EXISTS idx_staging_user_created;
    """)

    op.execute("""
        DROP TABLE IF EXISTS staging_normalized_transactions;
    """)
