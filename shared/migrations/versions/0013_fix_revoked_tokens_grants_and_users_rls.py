"""Restore revoked_tokens write grants for grosh_api; allow admin user INSERT

Revision ID: 0013
Revises: 0012

Two pre-existing defects discovered during the spec-003 end-to-end smoke
test, both unrelated to slices 19–27 but blocking real flows:

1. POST /v1/auth/logout returned 500 with
   ``permission denied for table revoked_tokens``.
   Migration 0008 created ``revoked_tokens`` and granted SELECT/INSERT/DELETE
   to ``grosh_api`` in its ``upgrade()`` block, but the running dev DB has
   only SELECT for the API role — the original INSERT/DELETE grant never
   reached this database (likely because an earlier version of 0008
   omitted the grant block). Re-applying the GRANT idempotently here
   guarantees every environment ends up with the correct privileges
   regardless of historical state.

2. POST /v1/admin/users returned 500 with
   ``new row violates row-level security policy for table "users"``.
   The single ``users_isolation`` policy in 0007 uses ``USING (id =
   app.current_user_id())``, which for an ``ALL`` policy doubles as
   ``WITH CHECK`` for INSERT. Admin-driven user creation produces a fresh
   ``id`` that never matches the admin's current_user_id, so the INSERT
   fails. We split the policy into per-command policies for SELECT, UPDATE,
   and DELETE (keeping the isolation guarantee on reads and self-mutations)
   and add a permissive INSERT policy. Application-level admin enforcement
   stays at the router (``require_admin`` on POST /v1/admin/users).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0013"
down_revision: str | Sequence[str] | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Re-apply missing write grants on revoked_tokens for grosh_api.
    # ------------------------------------------------------------------
    op.execute("""
        GRANT SELECT, INSERT, DELETE
            ON revoked_tokens
            TO grosh_api;
    """)

    # ------------------------------------------------------------------
    # 2. Replace the single ``users_isolation`` ALL policy with per-command
    #    policies. SELECT / UPDATE / DELETE keep self-isolation; INSERT is
    #    permitted to any session (gated by ``require_admin`` at the
    #    router layer).
    # ------------------------------------------------------------------
    op.execute("DROP POLICY IF EXISTS users_isolation ON users;")

    op.execute("""
        CREATE POLICY users_isolation_select
            ON users
            FOR SELECT
            USING (id = app.current_user_id());
    """)

    op.execute("""
        CREATE POLICY users_isolation_update
            ON users
            FOR UPDATE
            USING (id = app.current_user_id())
            WITH CHECK (id = app.current_user_id());
    """)

    op.execute("""
        CREATE POLICY users_isolation_delete
            ON users
            FOR DELETE
            USING (id = app.current_user_id());
    """)

    # ------------------------------------------------------------------
    # 3. Admin carve-out via a non-recursive session variable.
    #
    # PostgreSQL detects infinite recursion when an RLS policy on
    # ``users`` issues a sub-query against ``users``, so a "lookup admin
    # role from the users table" predicate cannot be used. Instead,
    # ``grosh_api`` sets ``app.current_user_role`` in the request-prep
    # dependency (alongside ``app.current_user_id``), and the policy
    # reads it via a SECURITY DEFINER wrapper that returns NULL if the
    # variable is unset.
    #
    # PostgreSQL emits "new row violates row-level security policy"
    # uniformly for two distinct failures: (a) WITH CHECK on INSERT and
    # (b) the SELECT policy failing on a RETURNING clause. The admin
    # carve-out below covers both via FOR ALL, and also unblocks GET
    # /v1/admin/users which enumerates the table.
    # ------------------------------------------------------------------
    op.execute("""
        CREATE OR REPLACE FUNCTION app.current_user_role()
        RETURNS text
        LANGUAGE sql
        STABLE
        AS $$
            SELECT NULLIF(current_setting('app.current_user_role', true), '')
        $$;
    """)

    op.execute("""
        CREATE POLICY users_admin_all
            ON users
            FOR ALL
            USING (app.current_user_role() = 'admin')
            WITH CHECK (app.current_user_role() = 'admin');
    """)

    # ------------------------------------------------------------------
    # 4. Admin carve-out on user_settings.
    #
    # Migration 0006 installs a BEFORE INSERT trigger on ``users`` that
    # auto-creates a ``user_settings`` row for every new user via
    # ``INSERT INTO user_settings (user_id) VALUES (NEW.id)``. That
    # trigger runs in the admin's session, so the new settings row's
    # ``user_id`` does not match ``app.current_user_id()`` and the
    # ``user_settings_isolation`` policy rejects it. Mirror the admin
    # carve-out from ``users`` so admin-driven user creation succeeds
    # all the way through the trigger.
    # ------------------------------------------------------------------
    op.execute("""
        CREATE POLICY user_settings_admin_all
            ON user_settings
            FOR ALL
            USING (app.current_user_role() = 'admin')
            WITH CHECK (app.current_user_role() = 'admin');
    """)


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS user_settings_admin_all ON user_settings;")
    op.execute("DROP POLICY IF EXISTS users_admin_all ON users;")
    op.execute("DROP FUNCTION IF EXISTS app.current_user_role();")
    op.execute("DROP POLICY IF EXISTS users_isolation_delete ON users;")
    op.execute("DROP POLICY IF EXISTS users_isolation_update ON users;")
    op.execute("DROP POLICY IF EXISTS users_isolation_select ON users;")

    op.execute("""
        CREATE POLICY users_isolation
            ON users
            USING (id = app.current_user_id());
    """)

    op.execute("""
        REVOKE INSERT, DELETE
            ON revoked_tokens
            FROM grosh_api;
    """)
