"""Register pg_cron job to delete reprocessing_backups rows older than 30 days

Revision ID: 0015
Revises: 0014

Adds a daily pg_cron job that prunes the reprocessing_backups table — the
table holds snapshots from per-user reprocess operations, retained for
operational debugging only. Without a retention sweep the table grows
linearly with reprocess invocations.

A TZ guard runs BEFORE the pg_cron registration. pg_cron schedule strings
are interpreted in the server's TimeZone setting; if the server is not on
UTC, the job would fire at the wrong wall-clock time. The guard runs in
its own DO block (no surrounding EXCEPTION handler) so the
RAISE EXCEPTION propagates straight out and fails the migration loudly.
The pg_cron block keeps its own EXCEPTION WHEN others handler so test DBs
without the pg_cron extension emit a NOTICE and continue.

Compose-side prerequisite: infra/docker-compose.yml sets PGTZ=UTC on the
postgres service so local-dev migrations pass the guard. Production K8s
runs Postgres with UTC by default.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0015"
down_revision: str | Sequence[str] | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. TZ guard — runs in its own DO block with no EXCEPTION handler.
    #    If the connection's TimeZone resolves to a non-zero UTC offset,
    #    RAISE EXCEPTION propagates straight out and aborts the migration.
    #    Nesting this inside the pg_cron block below would let
    #    `WHEN others` swallow it.
    #
    #    We check the resolved offset (EXTRACT timezone) rather than the
    #    TZ-name string because asyncpg connections canonicalize 'UTC' to
    #    'Etc/UTC' on the wire — both names resolve to a 0 offset and are
    #    semantically equivalent for pg_cron scheduling. Any non-UTC zone
    #    yields a non-zero offset and fails the guard.
    # ------------------------------------------------------------------
    op.execute("""
        DO $$
        BEGIN
            IF EXTRACT(timezone FROM now()) <> 0 THEN
                RAISE EXCEPTION
                    'pg_cron job reprocessing_backups_cleanup requires server TZ=UTC, got % (offset % seconds)',
                    current_setting('TimeZone'),
                    EXTRACT(timezone FROM now());
            END IF;
        END
        $$;
    """)

    # ------------------------------------------------------------------
    # 2. pg_cron extension + scheduled job.
    #    pg_cron can only be installed in the database named in
    #    cron.database_name (postgresql.conf). In test databases without
    #    pg_cron the EXCEPTION WHEN others handler catches and logs a
    #    NOTICE; the table itself is created by the earlier reprocess
    #    migration and retention is simply not auto-applied.
    # ------------------------------------------------------------------
    op.execute("""
        DO $$
        BEGIN
            CREATE EXTENSION IF NOT EXISTS pg_cron;
            PERFORM cron.schedule_in_database(
                'reprocessing_backups_cleanup',
                '0 3 * * *',
                'DELETE FROM reprocessing_backups WHERE created_at < now() - interval ''30 days''',
                current_database()
            );
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE
                    'pg_cron not installed: %. Skipping scheduled cleanup.',
                    SQLERRM;
        END
        $$;
    """)


def downgrade() -> None:
    op.execute("""
        DO $$
        BEGIN
            PERFORM cron.unschedule(jobid)
            FROM cron.job
            WHERE jobname = 'reprocessing_backups_cleanup';
        EXCEPTION
            WHEN others THEN
                NULL;
        END
        $$;
    """)
