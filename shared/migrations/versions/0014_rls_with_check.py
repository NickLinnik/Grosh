"""Add WITH CHECK clauses to five user-scoped RLS policies

Revision ID: 0014
Revises: 0013

Pre-existing gap: the five policies created by migration 0007
(accounts_isolation, bank_integrations_isolation, transactions_isolation,
user_settings_isolation, categories_isolation) were declared as ALL-command
policies with only a USING clause. PostgreSQL applies the USING expression as
the implicit WITH CHECK for INSERT and UPDATE, which means a bug that writes a
row with the wrong user_id would pass silently — the check predicate is derived
from the read-filter, not from an explicit write guard.

Tactic: ALTER POLICY replaces both USING and WITH CHECK atomically as a single
catalog update with no policy-absent window, which is safer than the
DROP+CREATE pattern used when adding WITH CHECK was not anticipated in the
original policy definition.

Special case — categories: the USING clause permits system categories
(user_id IS NULL) for every authenticated user so that shared category
definitions are readable. The WITH CHECK must not allow any session to INSERT
or UPDATE a system category row (user_id IS NULL) unless the caller is an
admin. Migration 0013 introduced app.current_user_role(), which is set by the
API layer alongside app.current_user_id(). The categories WITH CHECK adds a
role guard around the IS NULL branch, closing a pre-existing INSERT
vulnerability on system categories.

Downgrade: Postgres provides no ALTER POLICY ... DROP WITH CHECK syntax, so the
downgrade restores each policy to its pre-Slice-29 USING-only form via
DROP IF EXISTS + CREATE, matching the exact original definitions from 0007.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0014"
down_revision: str | Sequence[str] | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. accounts
    # ------------------------------------------------------------------
    op.execute("""
        ALTER POLICY accounts_isolation
            ON accounts
            USING (user_id = app.current_user_id())
            WITH CHECK (user_id = app.current_user_id());
    """)

    # ------------------------------------------------------------------
    # 2. bank_integrations
    # ------------------------------------------------------------------
    op.execute("""
        ALTER POLICY bank_integrations_isolation
            ON bank_integrations
            USING (user_id = app.current_user_id())
            WITH CHECK (user_id = app.current_user_id());
    """)

    # ------------------------------------------------------------------
    # 3. transactions
    # ------------------------------------------------------------------
    op.execute("""
        ALTER POLICY transactions_isolation
            ON transactions
            USING (user_id = app.current_user_id())
            WITH CHECK (user_id = app.current_user_id());
    """)

    # ------------------------------------------------------------------
    # 4. user_settings
    # ------------------------------------------------------------------
    op.execute("""
        ALTER POLICY user_settings_isolation
            ON user_settings
            USING (user_id = app.current_user_id())
            WITH CHECK (user_id = app.current_user_id());
    """)

    # ------------------------------------------------------------------
    # 5. categories — WITH CHECK adds an admin role guard around the
    #    user_id IS NULL branch so only admins can write system categories.
    #    USING stays identical to the 0007 form (reads permit IS NULL for
    #    all users).
    # ------------------------------------------------------------------
    op.execute("""
        ALTER POLICY categories_isolation
            ON categories
            USING (
                user_id = app.current_user_id()
                OR user_id IS NULL
            )
            WITH CHECK (
                user_id = app.current_user_id()
                OR (user_id IS NULL AND app.current_user_role() = 'admin')
            );
    """)


def downgrade() -> None:
    # Postgres has no ALTER POLICY ... DROP WITH CHECK, so we restore each
    # policy to its original 0007 USING-only definition via DROP + CREATE.

    op.execute("DROP POLICY IF EXISTS accounts_isolation ON accounts;")
    op.execute("""
        CREATE POLICY accounts_isolation
            ON accounts
            USING (user_id = app.current_user_id());
    """)

    op.execute(
        "DROP POLICY IF EXISTS bank_integrations_isolation ON bank_integrations;"
    )
    op.execute("""
        CREATE POLICY bank_integrations_isolation
            ON bank_integrations
            USING (user_id = app.current_user_id());
    """)

    op.execute("DROP POLICY IF EXISTS transactions_isolation ON transactions;")
    op.execute("""
        CREATE POLICY transactions_isolation
            ON transactions
            USING (user_id = app.current_user_id());
    """)

    op.execute("DROP POLICY IF EXISTS user_settings_isolation ON user_settings;")
    op.execute("""
        CREATE POLICY user_settings_isolation
            ON user_settings
            USING (user_id = app.current_user_id());
    """)

    op.execute("DROP POLICY IF EXISTS categories_isolation ON categories;")
    op.execute("""
        CREATE POLICY categories_isolation
            ON categories
            USING (user_id = app.current_user_id() OR user_id IS NULL);
    """)
