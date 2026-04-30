"""Transaction pipeline: bank_integrations, accounts, categories, transactions hypertable

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-10

Enables TimescaleDB and pgcrypto extensions, creates the five core tables for
the transaction ingestion pipeline (bank_integrations, accounts, categories,
transactions, currency_rates), converts `transactions` into a TimescaleDB
hypertable partitioned by time, adds indexes for the primary access patterns,
and enforces Row-Level Security on all user-scoped tables. The monthly_aggregates
continuous aggregate is created in migration 0005.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Extensions
    # ------------------------------------------------------------------
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    # ------------------------------------------------------------------
    # ENUM types
    # ------------------------------------------------------------------
    op.execute("CREATE TYPE bank_source AS ENUM ('monobank');")
    op.execute(
        "CREATE TYPE transaction_type AS ENUM ('income', 'expense', 'transfer', 'check');"
    )
    op.execute("CREATE TYPE transaction_source AS ENUM ('monobank', 'manual');")
    op.execute("CREATE TYPE transaction_origin AS ENUM ('bank', 'manual', 'derived');")

    # ------------------------------------------------------------------
    # bank_integrations
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE bank_integrations (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            bank            bank_source NOT NULL,
            config          JSONB       NOT NULL DEFAULT '{}',
            status          TEXT        NOT NULL DEFAULT 'active',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TRIGGER trg_bank_integrations_updated_at
        BEFORE UPDATE ON bank_integrations
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    """)

    op.execute(
        "CREATE INDEX idx_bank_integrations_user_id ON bank_integrations (user_id);"
    )
    op.execute(
        "CREATE UNIQUE INDEX idx_bank_integrations_webhook_secret"
        " ON bank_integrations ((config->>'webhook_secret'))"
        " WHERE config->>'webhook_secret' IS NOT NULL;"
    )

    op.execute("ALTER TABLE bank_integrations ENABLE ROW LEVEL SECURITY;")
    op.execute("""
        CREATE POLICY bank_integrations_isolation ON bank_integrations
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    # ------------------------------------------------------------------
    # accounts
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE accounts (
            id              UUID               PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         UUID               NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            integration_id  UUID               REFERENCES bank_integrations(id) ON DELETE SET NULL,
            source          transaction_source NOT NULL,
            type            TEXT               NOT NULL,
            currency_code   TEXT               NOT NULL,
            masked_pan      TEXT,
            iban            TEXT,
            external_id     TEXT,
            cashback_type   TEXT,
            name            TEXT,
            is_active       BOOLEAN            NOT NULL DEFAULT true,
            created_at      TIMESTAMPTZ        NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ        NOT NULL DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TRIGGER trg_accounts_updated_at
        BEFORE UPDATE ON accounts
        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
    """)

    op.execute("ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;")
    op.execute("""
        CREATE POLICY accounts_isolation ON accounts
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)

    op.execute("CREATE INDEX idx_accounts_user_id ON accounts (user_id);")
    op.execute("CREATE INDEX idx_accounts_integration_id ON accounts (integration_id);")
    op.execute(
        "CREATE INDEX idx_accounts_iban ON accounts (iban) WHERE iban IS NOT NULL;"
    )
    op.execute(
        "CREATE UNIQUE INDEX idx_accounts_manual_name"
        " ON accounts (user_id, name, currency_code)"
        " WHERE source = 'manual' AND name IS NOT NULL;"
    )

    # ------------------------------------------------------------------
    # categories
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE categories (
            id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id     UUID        REFERENCES users(id) ON DELETE CASCADE,
            name        TEXT        NOT NULL,
            parent_id   UUID        REFERENCES categories(id) ON DELETE CASCADE,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute("ALTER TABLE categories ENABLE ROW LEVEL SECURITY;")
    # NULL user_id = system-wide category, visible to all users.
    op.execute("""
        CREATE POLICY categories_isolation ON categories
            USING (user_id = current_setting('app.current_user_id', true)::uuid OR user_id IS NULL);
    """)

    op.execute("CREATE INDEX idx_categories_user_id ON categories (user_id);")

    # ------------------------------------------------------------------
    # transactions (hypertable)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE transactions (
            id                      UUID               NOT NULL,
            source_id               TEXT               NOT NULL,
            user_id                 UUID               NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            account_id              UUID               NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            time                    TIMESTAMPTZ        NOT NULL,
            amount_cents            BIGINT             NOT NULL,
            operation_amount_cents  BIGINT,
            currency_code           TEXT               NOT NULL,
            operation_currency_code TEXT,
            amount_uah_cents        BIGINT,
            amount_usd_cents        BIGINT,
            amount_eur_cents        BIGINT,
            description             TEXT,
            mcc                     INTEGER,
            cashback_amount_cents   BIGINT             DEFAULT 0,
            balance_cents           BIGINT,
            hold                    BOOLEAN            NOT NULL DEFAULT false,
            raw_transaction_type    transaction_type   NOT NULL,
            transaction_type        transaction_type   NOT NULL,
            counterparty_iban       TEXT,
            metadata                JSONB,
            source                  transaction_source NOT NULL,
            origin                  transaction_origin NOT NULL DEFAULT 'bank',
            related_transaction_id  UUID,
            created_at              TIMESTAMPTZ        NOT NULL DEFAULT now(),
            PRIMARY KEY (id, time)
        );
    """)

    # Convert to a TimescaleDB hypertable partitioned by time.
    op.execute("SELECT create_hypertable('transactions', 'time');")

    op.execute(
        "CREATE INDEX idx_transactions_user_time ON transactions (user_id, time DESC);"
    )
    op.execute(
        "CREATE INDEX idx_transactions_user_account_time"
        " ON transactions (user_id, account_id, time DESC);"
    )
    # TimescaleDB requires the partitioning column (time) in any unique constraint.
    op.execute(
        "CREATE UNIQUE INDEX idx_transactions_dedup"
        " ON transactions (account_id, source_id, time);"
    )

    # ------------------------------------------------------------------
    # currency_rates  (SCD Type 2, global — no RLS)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE currency_rates (
            id                       BIGSERIAL   PRIMARY KEY,
            source                   TEXT        NOT NULL,
            currency_from            TEXT        NOT NULL,
            currency_to              TEXT        NOT NULL,
            rate_buy                 NUMERIC(18,8),
            rate_sell                NUMERIC(18,8),
            rate_mid                 NUMERIC(18,8) NOT NULL,
            valid_from               TIMESTAMPTZ NOT NULL DEFAULT now(),
            valid_to                 TIMESTAMPTZ,
            last_polled_at           TIMESTAMPTZ,
            update_cadence_seconds INTEGER,
            created_at               TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    # Partial index: fast current-rate lookup by source + currency pair.
    op.execute("""
        CREATE INDEX idx_currency_rates_current
            ON currency_rates (source, currency_from, currency_to, valid_from)
            WHERE valid_to IS NULL;
    """)

    # ------------------------------------------------------------------
    # RLS on transactions
    # ------------------------------------------------------------------
    # NOTE: The monthly_aggregates continuous aggregate is created in
    # migration 0005. TimescaleDB forbids creating a continuous aggregate
    # on a hypertable that has RLS enabled, so 0005 disables RLS, creates
    # the aggregate, then re-enables it. RLS is safe to enable here because
    # 0005 handles the temporary disable/re-enable dance.
    op.execute("ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;")
    op.execute("""
        CREATE POLICY transactions_isolation ON transactions
            USING (user_id = current_setting('app.current_user_id', true)::uuid);
    """)


def downgrade() -> None:
    # transactions
    op.execute("DROP INDEX IF EXISTS idx_transactions_dedup;")
    op.execute("DROP INDEX IF EXISTS idx_transactions_user_account_time;")
    op.execute("DROP INDEX IF EXISTS idx_transactions_user_time;")
    op.execute("DROP POLICY IF EXISTS transactions_isolation ON transactions;")
    op.execute("DROP TABLE IF EXISTS transactions;")

    # currency_rates
    op.execute("DROP INDEX IF EXISTS idx_currency_rates_current;")
    op.execute("DROP TABLE IF EXISTS currency_rates;")

    # categories
    op.execute("DROP INDEX IF EXISTS idx_categories_user_id;")
    op.execute("DROP POLICY IF EXISTS categories_isolation ON categories;")
    op.execute("DROP TABLE IF EXISTS categories;")

    # accounts
    op.execute("DROP INDEX IF EXISTS idx_accounts_manual_name;")
    op.execute("DROP INDEX IF EXISTS idx_accounts_iban;")
    op.execute("DROP INDEX IF EXISTS idx_accounts_integration_id;")
    op.execute("DROP INDEX IF EXISTS idx_accounts_user_id;")
    op.execute("DROP POLICY IF EXISTS accounts_isolation ON accounts;")
    op.execute("DROP TRIGGER IF EXISTS trg_accounts_updated_at ON accounts;")
    op.execute("DROP TABLE IF EXISTS accounts;")

    # bank_integrations
    op.execute("DROP INDEX IF EXISTS idx_bank_integrations_webhook_secret;")
    op.execute("DROP INDEX IF EXISTS idx_bank_integrations_user_id;")
    op.execute(
        "DROP POLICY IF EXISTS bank_integrations_isolation ON bank_integrations;"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_bank_integrations_updated_at ON bank_integrations;"
    )
    op.execute("DROP TABLE IF EXISTS bank_integrations;")

    # ENUMs
    op.execute("DROP TYPE IF EXISTS transaction_origin;")
    op.execute("DROP TYPE IF EXISTS transaction_source;")
    op.execute("DROP TYPE IF EXISTS transaction_type;")
    op.execute("DROP TYPE IF EXISTS bank_source;")

    # Extensions — intentionally not dropped; other objects may depend on them.
