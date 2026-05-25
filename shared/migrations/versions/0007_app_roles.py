"""Create per-service application roles with least-privilege grants

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-01

Creates three login roles (grosh_api, grosh_ingestion, grosh_consumer) and
grants each role the minimum privileges required for its service:
- grosh_api: read-all + write to users, user_settings, refresh_tokens
- grosh_ingestion: read-all + write to accounts, bank_integrations, currency_rates
- grosh_consumer: read-all + BYPASSRLS + write to transactions, transfer_match_anomalies, reprocessing_locks, reprocessing_backups

Passwords are read from environment variables at migration time so that CI and
production can inject real credentials. Dev fallbacks are provided so the migration
runs without extra setup.

Environment variables (optional — fallback values used when not set):
  GROSH_API_DB_PASSWORD         default: changeme_api
  GROSH_INGESTION_DB_PASSWORD   default: changeme_ingestion
  GROSH_CONSUMER_DB_PASSWORD    default: changeme_consumer
"""

import os
from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | Sequence[str] | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    api_password = os.environ.get("GROSH_API_DB_PASSWORD", "changeme_api")
    ingestion_password = os.environ.get(
        "GROSH_INGESTION_DB_PASSWORD", "changeme_ingestion"
    )
    consumer_password = os.environ.get(
        "GROSH_CONSUMER_DB_PASSWORD", "changeme_consumer"
    )

    # ------------------------------------------------------------------
    # Create roles (idempotent via DO block — PostgreSQL lacks IF NOT EXISTS
    # for CREATE ROLE)
    # Passwords are escaped (single quotes doubled) to handle special chars.
    # ------------------------------------------------------------------
    api_escaped = api_password.replace("'", "''")
    ingestion_escaped = ingestion_password.replace("'", "''")
    consumer_escaped = consumer_password.replace("'", "''")

    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grosh_api') THEN
                CREATE ROLE grosh_api LOGIN PASSWORD '{api_escaped}';
            END IF;
        END
        $$;
    """)

    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grosh_ingestion') THEN
                CREATE ROLE grosh_ingestion LOGIN PASSWORD '{ingestion_escaped}';
            END IF;
        END
        $$;
    """)

    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grosh_consumer') THEN
                CREATE ROLE grosh_consumer LOGIN BYPASSRLS PASSWORD '{consumer_escaped}';
            END IF;
        END
        $$;
    """)

    # ------------------------------------------------------------------
    # Common grants — all three roles
    # ------------------------------------------------------------------
    for role in ("grosh_api", "grosh_ingestion", "grosh_consumer"):
        op.execute(f"GRANT CONNECT ON DATABASE grosh TO {role};")

        op.execute(f"GRANT USAGE ON SCHEMA public TO {role};")

        op.execute(f"""
            GRANT SELECT
                ON ALL TABLES
                IN SCHEMA public
                TO {role};
        """)

        op.execute(f"""
            GRANT USAGE, SELECT
                ON ALL SEQUENCES
                IN SCHEMA public
                TO {role};
        """)

    # ------------------------------------------------------------------
    # Table-specific write grants — grosh_api
    # ------------------------------------------------------------------
    op.execute("""
        GRANT INSERT, UPDATE
            ON users
            TO grosh_api;
    """)

    op.execute("""
        GRANT INSERT, UPDATE
            ON user_settings
            TO grosh_api;
    """)

    op.execute("""
        GRANT INSERT, DELETE
            ON refresh_tokens
            TO grosh_api;
    """)

    # ------------------------------------------------------------------
    # Table-specific write grants — grosh_ingestion
    # ------------------------------------------------------------------
    op.execute("""
        GRANT INSERT, UPDATE
            ON accounts
            TO grosh_ingestion;
    """)

    op.execute("""
        GRANT INSERT, UPDATE
            ON bank_integrations
            TO grosh_ingestion;
    """)

    op.execute("""
        GRANT INSERT, UPDATE
            ON currency_rates
            TO grosh_ingestion;
    """)

    # ------------------------------------------------------------------
    # Table-specific write grants — grosh_consumer
    # ------------------------------------------------------------------
    op.execute("""
        GRANT INSERT, UPDATE, DELETE
            ON transactions
            TO grosh_consumer;
    """)

    op.execute("""
        GRANT INSERT, DELETE
            ON transfer_match_anomalies
            TO grosh_consumer;
    """)

    op.execute("""
        GRANT INSERT, DELETE
            ON reprocessing_locks
            TO grosh_consumer;
    """)

    op.execute("""
        GRANT INSERT, DELETE
            ON reprocessing_backups
            TO grosh_consumer;
    """)

    # ------------------------------------------------------------------
    # Default privileges — cover tables and sequences created in the future
    # by grosh_admin so new migrations don't require manual re-grants
    # ------------------------------------------------------------------
    op.execute("""
        ALTER DEFAULT PRIVILEGES
            FOR ROLE grosh_admin
            IN SCHEMA public
            GRANT SELECT
                ON TABLES
                TO grosh_api, grosh_ingestion, grosh_consumer;
    """)

    op.execute("""
        ALTER DEFAULT PRIVILEGES
            FOR ROLE grosh_admin
            IN SCHEMA public
            GRANT USAGE, SELECT
                ON SEQUENCES
                TO grosh_api, grosh_ingestion, grosh_consumer;
    """)

    # ------------------------------------------------------------------
    # app schema: utility functions for RLS policies.
    # Separate schema keeps application helpers out of public.
    # ------------------------------------------------------------------
    op.execute("CREATE SCHEMA IF NOT EXISTS app;")

    for role in ("grosh_api", "grosh_ingestion", "grosh_consumer"):
        op.execute(f"GRANT USAGE ON SCHEMA app TO {role};")

    op.execute("""
        CREATE FUNCTION app.current_user_id()
        RETURNS UUID
        LANGUAGE sql
        STABLE
        PARALLEL SAFE
        AS $$
            SELECT NULLIF(current_setting('app.current_user_id', true), '')::uuid;
        $$;
    """)

    for role in ("grosh_api", "grosh_ingestion", "grosh_consumer"):
        op.execute(f"GRANT EXECUTE ON FUNCTION app.current_user_id() TO {role};")

    # ------------------------------------------------------------------
    # Rewrite RLS policies to use app.current_user_id() instead of
    # raw current_setting cast. Prevents ''::uuid errors when the
    # session variable reverts to empty string between transactions.
    # ------------------------------------------------------------------
    op.execute("DROP POLICY users_isolation ON users;")
    op.execute("""
        CREATE POLICY users_isolation ON users
            USING (id = app.current_user_id());
    """)

    op.execute("DROP POLICY accounts_isolation ON accounts;")
    op.execute("""
        CREATE POLICY accounts_isolation ON accounts
            USING (user_id = app.current_user_id());
    """)

    op.execute("DROP POLICY bank_integrations_isolation ON bank_integrations;")
    op.execute("""
        CREATE POLICY bank_integrations_isolation ON bank_integrations
            USING (user_id = app.current_user_id());
    """)

    op.execute("DROP POLICY categories_isolation ON categories;")
    op.execute("""
        CREATE POLICY categories_isolation ON categories
            USING (user_id = app.current_user_id() OR user_id IS NULL);
    """)

    op.execute("DROP POLICY transactions_isolation ON transactions;")
    op.execute("""
        CREATE POLICY transactions_isolation ON transactions
            USING (user_id = app.current_user_id());
    """)

    op.execute("DROP POLICY user_settings_isolation ON user_settings;")
    op.execute("""
        CREATE POLICY user_settings_isolation ON user_settings
            USING (user_id = app.current_user_id());
    """)

    # ------------------------------------------------------------------
    # RLS policy: allow SELECT on users by email for the login path.
    # The users_isolation policy requires app.current_user_id() which is
    # NULL before login. This permissive policy ORs with it, allowing
    # lookup when app.current_user_email is set.
    # ------------------------------------------------------------------
    op.execute("""
        CREATE POLICY users_auth_lookup
            ON users
            FOR SELECT
            USING (email = NULLIF(current_setting('app.current_user_email', true), '')::citext);
    """)


def downgrade() -> None:
    # ------------------------------------------------------------------
    # Restore original RLS policies (raw current_setting cast)
    # ------------------------------------------------------------------
    op.execute("DROP POLICY IF EXISTS users_auth_lookup ON users;")

    op.execute("DROP POLICY IF EXISTS users_isolation ON users;")
    op.execute("""
        CREATE POLICY users_isolation ON users
            USING (id = current_setting('app.current_user_id', true)::uuid);
    """)

    op.execute("DROP POLICY IF EXISTS accounts_isolation ON accounts;")
    op.execute("""
        CREATE POLICY accounts_isolation ON accounts
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    op.execute(
        "DROP POLICY IF EXISTS bank_integrations_isolation ON bank_integrations;"
    )
    op.execute("""
        CREATE POLICY bank_integrations_isolation ON bank_integrations
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    op.execute("DROP POLICY IF EXISTS categories_isolation ON categories;")
    op.execute("""
        CREATE POLICY categories_isolation ON categories
            USING (user_id = current_setting('app.current_user_id', true)::uuid OR user_id IS NULL);
    """)

    op.execute("DROP POLICY IF EXISTS transactions_isolation ON transactions;")
    op.execute("""
        CREATE POLICY transactions_isolation ON transactions
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    op.execute("DROP POLICY IF EXISTS user_settings_isolation ON user_settings;")
    op.execute("""
        CREATE POLICY user_settings_isolation ON user_settings
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    # ------------------------------------------------------------------
    # Drop app schema and function
    # ------------------------------------------------------------------
    op.execute("DROP FUNCTION IF EXISTS app.current_user_id();")
    op.execute("DROP SCHEMA IF EXISTS app;")

    # ------------------------------------------------------------------
    # Revoke default privilege grants
    # ------------------------------------------------------------------
    op.execute("""
        ALTER DEFAULT PRIVILEGES
            FOR ROLE grosh_admin
            IN SCHEMA public
            REVOKE SELECT
                ON TABLES
                FROM grosh_api, grosh_ingestion, grosh_consumer;
    """)

    op.execute("""
        ALTER DEFAULT PRIVILEGES
            FOR ROLE grosh_admin
            IN SCHEMA public
            REVOKE USAGE, SELECT
                ON SEQUENCES
                FROM grosh_api, grosh_ingestion, grosh_consumer;
    """)

    # ------------------------------------------------------------------
    # Revoke all object-level privileges before dropping roles
    # ------------------------------------------------------------------
    for role in ("grosh_api", "grosh_ingestion", "grosh_consumer"):
        op.execute(f"""
            REVOKE ALL PRIVILEGES
                ON ALL TABLES
                IN SCHEMA public
                FROM {role};
        """)

        op.execute(f"""
            REVOKE ALL PRIVILEGES
                ON ALL SEQUENCES
                IN SCHEMA public
                FROM {role};
        """)

        op.execute(f"REVOKE USAGE ON SCHEMA public FROM {role};")

        op.execute(f"REVOKE CONNECT ON DATABASE grosh FROM {role};")

    # ------------------------------------------------------------------
    # Drop roles
    # ------------------------------------------------------------------
    op.execute("DROP ROLE IF EXISTS grosh_api;")
    op.execute("DROP ROLE IF EXISTS grosh_ingestion;")
    op.execute("DROP ROLE IF EXISTS grosh_consumer;")
