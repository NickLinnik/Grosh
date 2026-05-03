"""Create revoked_tokens table for JWT revocation tracking

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-01

Stores revoked JWT IDs (jti) so the API can reject tokens that have been
invalidated before their natural expiry. Expired rows are purged every
5 minutes by a pg_cron scheduled job.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0009"
down_revision: str | Sequence[str] | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Table
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE revoked_tokens (
            jti        UUID        PRIMARY KEY,
            expires_at TIMESTAMPTZ NOT NULL
        );
    """)

    # ------------------------------------------------------------------
    # Index for fast cleanup queries by the pg_cron purge job
    # ------------------------------------------------------------------
    op.execute("""
        CREATE INDEX idx_revoked_tokens_expires_at
            ON revoked_tokens (expires_at);
    """)

    # ------------------------------------------------------------------
    # pg_cron extension + scheduled purge every 5 minutes
    # ------------------------------------------------------------------
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")

    op.execute("""
        SELECT cron.schedule_in_database(
            'purge-revoked-tokens',
            '*/5 * * * *',
            $$DELETE FROM revoked_tokens WHERE expires_at < now()$$,
            'grosh'
        );
    """)

    # ------------------------------------------------------------------
    # Grants — grosh_api needs INSERT (on revocation), DELETE (on logout
    # cleanup if needed), and SELECT (for validation checks)
    # ------------------------------------------------------------------
    op.execute("""
        GRANT SELECT, INSERT, DELETE
            ON revoked_tokens
            TO grosh_api;
    """)


def downgrade() -> None:
    op.execute("""
        SELECT cron.unschedule(jobid)
        FROM cron.job
        WHERE jobname = 'purge-revoked-tokens';
    """)
    op.execute("DROP TABLE IF EXISTS revoked_tokens;")
    op.execute("DROP EXTENSION IF EXISTS pg_cron;")
