"""Initial schema: users, refresh_tokens, networks, network_members, RLS

Revision ID: 0001
Revises:
Create Date: 2026-04-06

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE TYPE user_role AS ENUM ('admin', 'member');")

    op.execute("""
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    op.execute("""
        CREATE TABLE users (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email           TEXT UNIQUE NOT NULL,
            password_hash   TEXT NOT NULL,
            display_name    TEXT NOT NULL,
            role            user_role NOT NULL DEFAULT 'member',
            is_active       BOOLEAN NOT NULL DEFAULT true,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TRIGGER trg_users_updated_at
        BEFORE UPDATE ON users
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    """)

    op.execute("ALTER TABLE users ENABLE ROW LEVEL SECURITY;")
    op.execute("""
        CREATE POLICY users_isolation ON users
            USING (id = current_setting('app.current_user_id', true)::uuid);
    """)

    op.execute("""
        CREATE TABLE refresh_tokens (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token_hash  TEXT UNIQUE NOT NULL,
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE networks (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name        TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE network_members (
            network_id  UUID NOT NULL REFERENCES networks(id) ON DELETE CASCADE,
            user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            role        user_role NOT NULL DEFAULT 'member',
            joined_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (network_id, user_id)
        );
    """)

    op.execute("""
        CREATE TRIGGER trg_network_members_updated_at
        BEFORE UPDATE ON network_members
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_network_members_updated_at ON network_members;"
    )
    op.execute("DROP TABLE IF EXISTS network_members;")
    op.execute("DROP TABLE IF EXISTS networks;")
    op.execute("DROP TABLE IF EXISTS refresh_tokens;")
    op.execute("DROP POLICY IF EXISTS users_isolation ON users;")
    op.execute("ALTER TABLE users DISABLE ROW LEVEL SECURITY;")
    op.execute("DROP TRIGGER IF EXISTS trg_users_updated_at ON users;")
    op.execute("DROP TABLE IF EXISTS users;")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at();")
    op.execute("DROP TYPE IF EXISTS user_role;")
