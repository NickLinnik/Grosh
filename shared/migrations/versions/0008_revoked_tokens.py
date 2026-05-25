"""Create revoked_tokens table for JWT revocation tracking

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-01

Stores revoked JWT IDs (jti) so the API can reject tokens that have been
invalidated before their natural expiry. Expired rows are purged every
5 minutes by a pg_cron scheduled job.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008"
down_revision: str | Sequence[str] | None = "0007"
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
    #
    # pg_cron can only be installed in the database named in
    # cron.database_name (postgresql.conf). In test databases this fails
    # with an explicit error. We wrap the block so migrations run cleanly
    # in any database — the cron job simply won't be registered outside
    # the production database, which is acceptable (test DBs are ephemeral
    # and have no long-running background worker to run it anyway).
    # ------------------------------------------------------------------
    op.execute("""
        DO $$
        BEGIN
            CREATE EXTENSION IF NOT EXISTS pg_cron;
            PERFORM cron.schedule_in_database(
                'purge-revoked-tokens',
                '*/5 * * * *',
                'DELETE FROM revoked_tokens WHERE expires_at < now()',
                current_database()
            );
        EXCEPTION
            WHEN others THEN
                -- pg_cron not available in this database (e.g. test DB).
                -- Log a notice and continue; the table still exists and
                -- expired rows will simply not be auto-purged.
                RAISE NOTICE
                    'pg_cron not installed: %. Skipping scheduled purge.',
                    SQLERRM;
        END
        $$;
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
        DO $$
        BEGIN
            PERFORM cron.unschedule(jobid)
            FROM cron.job
            WHERE jobname = 'purge-revoked-tokens';
        EXCEPTION
            WHEN others THEN
                NULL;
        END
        $$;
    """)
    op.execute("DROP TABLE IF EXISTS revoked_tokens;")
    op.execute("""
        DO $$
        BEGIN
            DROP EXTENSION IF EXISTS pg_cron;
        EXCEPTION
            WHEN others THEN
                NULL;
        END
        $$;
    """)
