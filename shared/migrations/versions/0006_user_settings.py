"""Create user_settings table

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-22

Creates user_settings — a one-row-per-user preferences table. Holds per-user
preferences: default rate source for currency conversion and timezone for
aggregation bucketing. Row-Level Security is enabled so each user can only
read and write their own row.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | Sequence[str] | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # user_settings
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE user_settings (
            user_id             UUID        PRIMARY KEY
                                            REFERENCES users(id) ON DELETE CASCADE,
            default_rate_source TEXT        REFERENCES rate_source_config(source),
            timezone            TEXT        NOT NULL DEFAULT 'UTC',
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TRIGGER trg_user_settings_updated_at
        BEFORE UPDATE ON user_settings
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    """)

    op.execute("ALTER TABLE user_settings ENABLE ROW LEVEL SECURITY;")

    op.execute("""
        CREATE POLICY user_settings_isolation ON user_settings
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    # Seed a default row for every user that already exists.
    op.execute("""
        INSERT INTO user_settings (user_id)
        SELECT id FROM users;
    """)

    # Auto-create a row for every new user.
    op.execute("""
        CREATE FUNCTION create_user_settings() RETURNS trigger AS $$
        BEGIN
            INSERT INTO user_settings (user_id) VALUES (NEW.id);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE TRIGGER trg_users_create_settings
        AFTER INSERT ON users
        FOR EACH ROW EXECUTE FUNCTION create_user_settings();
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_user_settings_updated_at ON user_settings;")
    op.execute("DROP TRIGGER IF EXISTS trg_users_create_settings ON users;")
    op.execute("DROP FUNCTION IF EXISTS create_user_settings();")
    op.execute("DROP TABLE IF EXISTS user_settings;")
