"""Email as CITEXT: case-insensitive uniqueness and comparisons

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-08

Promotes ``users.email`` from ``TEXT`` to ``CITEXT`` so uniqueness and
equality comparisons are case-insensitive at the database layer. This
removes the need for application code to remember ``email.lower()``
before every lookup — the convention is now enforced by the type.

Existing rows are normalized to lowercase *before* the type change so
that any case-varying duplicates surface as unique-constraint violations
up front rather than silently collapsing later.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS citext")
    # Normalize existing data so no case-varying duplicates exist before the
    # unique index becomes case-insensitive.
    op.execute("UPDATE users SET email = lower(email)")
    op.execute("ALTER TABLE users ALTER COLUMN email TYPE CITEXT")


def downgrade() -> None:
    op.execute("ALTER TABLE users ALTER COLUMN email TYPE TEXT")
    # Intentionally do not drop the citext extension — other tables may use it
