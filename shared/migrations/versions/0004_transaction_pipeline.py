"""Transaction pipeline: bank_integrations, accounts, categories, transactions

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-10

Enables pgcrypto extension, creates the six core tables for the transaction
ingestion pipeline (bank_integrations, accounts, categories, transactions,
currency_rates, transfer_match_anomalies), adds indexes for the primary access
patterns, and enforces Row-Level Security on all user-scoped tables.
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
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    # ------------------------------------------------------------------
    # ENUM types
    # ------------------------------------------------------------------
    op.execute("CREATE TYPE bank_source AS ENUM ('monobank');")
    op.execute(
        "CREATE TYPE transaction_direction AS ENUM ('income', 'expense', 'zero');"
    )
    op.execute("CREATE TYPE special_category AS ENUM ('transfer');")
    op.execute("CREATE TYPE transaction_source AS ENUM ('monobank', 'manual');")
    op.execute("CREATE TYPE transaction_origin AS ENUM ('bank', 'manual');")
    op.execute("""
        CREATE TYPE transfer_anomaly_reason AS ENUM (
            'unpaired_from_description',
            'unpaired_to_description',
            'ambiguous_pair_match',
            'description_account_mismatch',
            'description_consistency_mismatch'
        );
    """)

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
            USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
    """)
    # Permissive SELECT policy for the unauthenticated Monobank webhook lookup.
    # The webhook handler can't present a JWT, so it sets
    # app.current_webhook_secret to the path-param secret; this policy returns
    # the matching active integration, alongside the user-scoped policy above
    # (RLS unions permissive policies). Writes are unaffected — no WITH CHECK.
    # Mirrors the credential-lookup pattern established by users_auth_lookup
    # in migration 0007 for the unauthenticated login flow.
    op.execute("""
        CREATE POLICY bank_integrations_webhook_lookup
            ON bank_integrations
            FOR SELECT
            USING (
                config->>'webhook_secret' = NULLIF(
                    current_setting('app.current_webhook_secret', true), ''
                )
                AND status = 'active'
            );
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
            USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
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
            USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid OR user_id IS NULL);
    """)

    op.execute("CREATE INDEX idx_categories_user_id ON categories (user_id);")

    # ------------------------------------------------------------------
    # transactions
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE transactions (
            id                      UUID               PRIMARY KEY DEFAULT gen_random_uuid(),
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
            mcc                     TEXT,
            cashback_amount_cents   BIGINT             DEFAULT 0,
            balance_cents           BIGINT,
            hold                    BOOLEAN,
            direction               transaction_direction NOT NULL,
            special_category        special_category,
            counterparty_iban       TEXT,
            rate_source             TEXT,
            metadata                JSONB,
            source                  transaction_source NOT NULL,
            origin                  transaction_origin NOT NULL DEFAULT 'bank',
            related_transaction_id  UUID               REFERENCES transactions(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED,
            created_at              TIMESTAMPTZ        NOT NULL DEFAULT now()
        );
    """)

    op.execute(
        "CREATE INDEX idx_transactions_user_time ON transactions (user_id, time DESC);"
    )
    op.execute(
        "CREATE INDEX idx_transactions_user_account_time"
        " ON transactions (user_id, account_id, time DESC);"
    )
    op.execute(
        "CREATE UNIQUE INDEX idx_transactions_dedup"
        " ON transactions (account_id, source_id);"
    )

    # ------------------------------------------------------------------
    # currency_rates  (SCD Type 2, global — no RLS)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE currency_rates (
            id                       UUID        PRIMARY KEY DEFAULT uuidv7(),
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
    op.execute("ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;")
    op.execute("""
        CREATE POLICY transactions_isolation ON transactions
            USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
    """)

    # ------------------------------------------------------------------
    # transfer_match_anomalies  (no RLS — written by consumer)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE transfer_match_anomalies (
            id              UUID                     PRIMARY KEY DEFAULT uuidv7(),
            transaction_id  UUID                     NOT NULL REFERENCES transactions(id) ON DELETE CASCADE UNIQUE,
            candidate_ids   UUID[]                   NOT NULL DEFAULT '{}',
            reason_code     transfer_anomaly_reason  NOT NULL,
            reason_detail   TEXT,
            created_at      TIMESTAMPTZ              NOT NULL DEFAULT now()
        );
    """)

    # Transfer detection v2: universal candidate fetch
    # (one partial index replaces the three v1 tier-specific indexes;
    # the two-clause amount predicate is re-checked on the candidate set
    # at query time rather than indexed — at per-user volume after the
    # partial-index restriction the candidate set is bounded to a handful
    # of rows; see references/adr-transfer-detection-v2.md §3).
    op.execute("""
        CREATE INDEX idx_transactions_transfer_universal
            ON transactions (user_id, direction, time DESC)
            WHERE mcc = '4829' AND related_transaction_id IS NULL;
    """)

    # ------------------------------------------------------------------
    # reprocessing_locks  (no RLS — written by consumer which bypasses RLS)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE reprocessing_locks (
            user_id   UUID        PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            locked_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    # ------------------------------------------------------------------
    # reprocessing_backups  (no RLS — written by consumer which bypasses RLS)
    # ------------------------------------------------------------------
    op.execute("""
        CREATE TABLE reprocessing_backups (
            id         UUID        PRIMARY KEY DEFAULT uuidv7(),
            user_id    UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            data       JSONB       NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    op.execute(
        "CREATE INDEX idx_reprocessing_backups_user_id ON reprocessing_backups (user_id);"
    )


def downgrade() -> None:
    # reprocessing
    op.execute("DROP INDEX IF EXISTS idx_reprocessing_backups_user_id;")
    op.execute("DROP TABLE IF EXISTS reprocessing_backups;")
    op.execute("DROP TABLE IF EXISTS reprocessing_locks;")

    # transfer_match_anomalies
    op.execute("DROP TABLE IF EXISTS transfer_match_anomalies;")

    # transfer detection indexes (dropped with table, but explicit for clarity)
    op.execute("DROP INDEX IF EXISTS idx_transactions_transfer_universal;")

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
        "DROP POLICY IF EXISTS bank_integrations_webhook_lookup ON bank_integrations;"
    )
    op.execute(
        "DROP POLICY IF EXISTS bank_integrations_isolation ON bank_integrations;"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_bank_integrations_updated_at ON bank_integrations;"
    )
    op.execute("DROP TABLE IF EXISTS bank_integrations;")

    # ENUMs
    op.execute("DROP TYPE IF EXISTS transfer_anomaly_reason;")
    op.execute("DROP TYPE IF EXISTS transaction_origin;")
    op.execute("DROP TYPE IF EXISTS transaction_source;")
    op.execute("DROP TYPE IF EXISTS special_category;")
    op.execute("DROP TYPE IF EXISTS transaction_direction;")
    op.execute("DROP TYPE IF EXISTS bank_source;")

    # Extensions — intentionally not dropped; other objects may depend on them.
