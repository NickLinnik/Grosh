"""Create revoked_tokens hypertable for JWT revocation tracking

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-01

Stores revoked JWT IDs (jti) so the API can reject tokens that have been
invalidated before their natural expiry. Implemented as a TimescaleDB
hypertable on expires_at so that expired rows are automatically purged by
the retention policy (1 hour) without a manual cleanup job.
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
            jti        UUID        NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        );
    """)

    # ------------------------------------------------------------------
    # Convert to hypertable (partitioned by expires_at)
    # TimescaleDB DDL requires RLS to be disabled on the source table.
    # revoked_tokens has no RLS policies, so no bracket is needed here.
    # ------------------------------------------------------------------
    op.execute("SELECT create_hypertable('revoked_tokens', 'expires_at');")

    # ------------------------------------------------------------------
    # Retention policy — auto-drop chunks older than 1 hour
    # ------------------------------------------------------------------
    op.execute("SELECT add_retention_policy('revoked_tokens', INTERVAL '1 hour');")

    # ------------------------------------------------------------------
    # Index for fast jti lookups during token validation
    # ------------------------------------------------------------------
    op.execute("""
        CREATE INDEX idx_revoked_tokens_jti
            ON revoked_tokens (jti);
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
    # ------------------------------------------------------------------
    # Remove retention policy before dropping the table so TimescaleDB
    # does not complain about a dangling policy referencing a dropped object.
    # ------------------------------------------------------------------
    op.execute("""
        SELECT remove_retention_policy('revoked_tokens', if_not_exists => true);
    """)

    op.execute("DROP TABLE IF EXISTS revoked_tokens;")
