"""Add last_reprocess_started_at to users table for per-user reprocess rate-limiting.

The column is written by the ingestion service (grosh_ingestion role) via an
atomic UPDATE ... WHERE to claim or deny reprocess slots. The API service
(grosh_api role) never writes it. The co-ownership rationale matches the pattern
established in 0012/0013: ingestion owns rate-limit state for operations it
triggers, even though users is otherwise an API-service table.

revision = '0016'
down_revision = '0015'
"""

from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE users
            ADD COLUMN last_reprocess_started_at TIMESTAMPTZ NULL
    """)

    op.execute("""
        GRANT UPDATE (last_reprocess_started_at)
            ON users
            TO grosh_ingestion
    """)


def downgrade() -> None:
    op.execute("""
        REVOKE UPDATE (last_reprocess_started_at)
            ON users
            FROM grosh_ingestion
    """)

    op.execute("""
        ALTER TABLE users
            DROP COLUMN last_reprocess_started_at
    """)
