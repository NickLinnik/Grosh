"""Add accounts.config JSONB column and bank_integrations uniqueness constraint

Revision ID: 0011
Revises: 0010
Create Date: 2026-05-20

Change 1 — accounts.config JSONB column
----------------------------------------
Adds a ``config JSONB NOT NULL DEFAULT '{}'`` column to ``accounts`` so that
source-specific fields (e.g. ``monobank_client_id`` for orphan rebind matching)
are stored in the generic JSONB bag rather than as top-level source-specific
columns.  This follows the same pattern used by ``bank_integrations.config``.

Monobank-specific context: when a Monobank integration is hard-deleted
(``integration_id`` is SET NULL by the FK constraint), orphaned account rows
need to carry ``monobank_client_id`` inside ``config`` so they can be matched
back to the same Monobank user on the next link call.  Without this field the
only surviving link is ``external_account_id``, which is not unique across
users — pairing by external ID alone could rebind the wrong user's accounts
after an admin-level hard-delete.

The partial functional index covers the orphan-lookup query:
    WHERE user_id = $1
      AND source = 'monobank'
      AND config->>'monobank_client_id' = $2
      AND integration_id IS NULL

Change 2 — bank_integrations uniqueness for Monobank client_id
--------------------------------------------------------------
Adds a partial UNIQUE index on bank_integrations to prevent duplicate rows for
the same (user_id, monobank_client_id) combination under bank='monobank'.
This makes concurrent POST /v1/monobank/link requests safe: at most one INSERT
can succeed; the second hits ON CONFLICT and is handled idempotently.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0011"
down_revision: str | Sequence[str] | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE accounts ADD COLUMN config JSONB NOT NULL DEFAULT '{}';")
    op.execute("""
        CREATE INDEX idx_accounts_monobank_client_id
            ON accounts (user_id, (config->>'monobank_client_id'))
            WHERE config->>'monobank_client_id' IS NOT NULL;
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_bank_integrations_user_monobank_client_id
            ON bank_integrations (user_id, (config->>'monobank_client_id'))
            WHERE bank = 'monobank'
                AND config->>'monobank_client_id' IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_bank_integrations_user_monobank_client_id;")
    op.execute("DROP INDEX IF EXISTS idx_accounts_monobank_client_id;")
    op.execute("ALTER TABLE accounts DROP COLUMN IF EXISTS config;")
