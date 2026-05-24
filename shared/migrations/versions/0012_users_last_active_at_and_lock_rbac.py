"""Add users.last_active_at and invert reprocessing_locks INSERT ownership

Revision ID: 0012
Revises: 0011

Two changes bundled into one migration because they are part of the same
behavioral redesign (Slice 22 — lock-ownership inversion):

1. users.last_active_at — nullable TIMESTAMPTZ column for activity tracking.
   No default, no trigger; updated by explicit writes only (auth hook, Slice 24).

2. RBAC flip on reprocessing_locks INSERT:
   - GRANT INSERT to grosh_ingestion  (ingestion API now owns lock creation)
   - REVOKE INSERT from grosh_consumer (consumer now asserts, does not insert)
   DELETE on reprocessing_locks stays with grosh_consumer — it releases the
   lock on successful reprocess completion (verified in migration 0007).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012"
down_revision: str | Sequence[str] | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE users
            ADD COLUMN last_active_at TIMESTAMPTZ NULL
        """
    )

    op.execute(
        """
        GRANT INSERT
            ON reprocessing_locks
            TO grosh_ingestion
        """
    )

    op.execute(
        """
        REVOKE INSERT
            ON reprocessing_locks
            FROM grosh_consumer
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE INSERT
            ON reprocessing_locks
            FROM grosh_ingestion
        """
    )

    op.execute(
        """
        GRANT INSERT
            ON reprocessing_locks
            TO grosh_consumer
        """
    )

    op.execute(
        """
        ALTER TABLE users
            DROP COLUMN last_active_at
        """
    )
