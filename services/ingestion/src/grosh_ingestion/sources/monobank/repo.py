"""Monobank-specific repository: webhook lookups, token crypto, integration CRUD."""

import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg
from grosh_shared.models import BankSource


@dataclass(frozen=True)
class IntegrationRef:
    id: UUID
    user_id: UUID


@dataclass(frozen=True)
class AccountRef:
    id: UUID


@dataclass(frozen=True)
class BankIntegrationRow:
    id: UUID
    user_id: UUID
    monobank_client_id: str
    config: dict
    created_at: datetime


@dataclass(frozen=True)
class AccountRow:
    id: UUID
    external_id: str
    currency_code: str


class MonobankRepo:
    async def get_active_integration_by_webhook_secret(
        self, conn: asyncpg.Connection, webhook_secret: str
    ) -> IntegrationRef | None:
        row = await conn.fetchrow(
            """
            SELECT id, user_id
            FROM bank_integrations
            WHERE config->>'webhook_secret' = $1
                AND status = 'active'
            """,
            webhook_secret,
        )
        return IntegrationRef(id=row["id"], user_id=row["user_id"]) if row else None

    async def get_account_by_external_id(
        self, conn: asyncpg.Connection, external_id: str, integration_id: UUID
    ) -> AccountRef | None:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM accounts
            WHERE external_id = $1
                AND integration_id = $2
            """,
            external_id,
            integration_id,
        )
        return AccountRef(id=row["id"]) if row else None

    async def decrypt_token(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
        encryption_key: str,
    ) -> str | None:
        row = await conn.fetchrow(
            """
            SELECT pgp_sym_decrypt(
                decode(config->>'encrypted_token', 'hex'),
                $2
            )::text AS token
            FROM bank_integrations
            WHERE id = $1
                AND status = 'active'
            """,
            integration_id,
            encryption_key,
        )
        if row is None:
            return None
        return row["token"]

    async def encrypt_token(
        self,
        conn: asyncpg.Connection,
        plaintext_token: str,
        encryption_key: str,
    ) -> bytes:
        """Encrypt a plaintext Monobank token via pgcrypto's pgp_sym_encrypt.

        Used by link() flow in MonobankLinkingService.
        Returns raw bytes (the caller hex-encodes for storage in config JSONB).
        """
        return await conn.fetchval(
            """
            SELECT pgp_sym_encrypt($1, $2)
            """,
            plaintext_token,
            encryption_key,
        )

    async def create_integration_idempotent(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        config: dict,
    ) -> tuple[UUID, bool]:
        """Insert a new Monobank bank_integrations row, handling concurrent duplicates.

        Uses ON CONFLICT DO NOTHING against the partial UNIQUE index
        ``idx_bank_integrations_user_monobank_client_id``.

        Returns (integration_id, is_new):
        - is_new=True  when a fresh row was inserted.
        - is_new=False when a concurrent insert for the same (user_id,
          monobank_client_id) already committed; the existing row's id is returned.
        """
        row = await conn.fetchrow(
            """
            INSERT INTO bank_integrations
                (user_id, bank, config, status)
            VALUES
                ($1, $2, $3::jsonb, 'active')
            ON CONFLICT (user_id, (config->>'monobank_client_id'))
                WHERE bank = 'monobank'
                    AND config->>'monobank_client_id' IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            user_id,
            str(BankSource.monobank),
            json.dumps(config),
        )
        if row is not None:
            return row["id"], True

        # Conflict: fetch the already-existing row
        existing_id = await conn.fetchval(
            """
            SELECT id
            FROM bank_integrations
            WHERE user_id = $1
                AND bank = 'monobank'
                AND status = 'active'
                AND config->>'monobank_client_id' = $2
            """,
            user_id,
            config["monobank_client_id"],
        )
        return existing_id, False

    async def decrypt_token_value(
        self,
        conn: asyncpg.Connection,
        encrypted_token_hex: str,
        encryption_key: str,
    ) -> str | None:
        """Decrypt a hex-encoded ciphertext token via pgcrypto's pgp_sym_decrypt.

        Used by reregister_webhooks() which already has the ciphertext from a
        previously-fetched config['encrypted_token']; differs from decrypt_token()
        which fetches by integration_id.
        """
        return await conn.fetchval(
            """
            SELECT pgp_sym_decrypt(
                decode($1, 'hex'),
                $2
            )::text
            """,
            encrypted_token_hex,
            encryption_key,
        )

    async def get_integration_by_client_id(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        monobank_client_id: str,
    ) -> BankIntegrationRow | None:
        """Fetch the active Monobank integration for a user by monobank_client_id.

        Returns None if no matching active integration exists.
        """
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                config,
                created_at
            FROM bank_integrations
            WHERE user_id = $1
                AND bank = 'monobank'
                AND status = 'active'
                AND config->>'monobank_client_id' = $2
            """,
            user_id,
            monobank_client_id,
        )
        if row is None:
            return None
        raw_config = row["config"]
        config = (
            json.loads(raw_config) if isinstance(raw_config, str) else dict(raw_config)
        )
        return BankIntegrationRow(
            id=row["id"],
            user_id=row["user_id"],
            monobank_client_id=monobank_client_id,
            config=config,
            created_at=row["created_at"],
        )

    async def get_any_active_integration(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> BankIntegrationRow | None:
        """Fetch any active Monobank integration for the user (client_id-agnostic).

        Used to detect the "different client_id" 409 case.
        """
        row = await conn.fetchrow(
            """
            SELECT
                id,
                user_id,
                config,
                created_at
            FROM bank_integrations
            WHERE user_id = $1
                AND bank = 'monobank'
                AND status = 'active'
            """,
            user_id,
        )
        if row is None:
            return None
        raw_config = row["config"]
        config = (
            json.loads(raw_config) if isinstance(raw_config, str) else dict(raw_config)
        )
        return BankIntegrationRow(
            id=row["id"],
            user_id=row["user_id"],
            monobank_client_id=config.get("monobank_client_id", ""),
            config=config,
            created_at=row["created_at"],
        )

    async def update_integration_token(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
        encrypted_token: bytes,
        key_version: int,
    ) -> None:
        """Rotate the encrypted token on an existing integration.

        Merges only the token-related keys into config so other fields
        (webhook_secret, webhook_url, monobank_client_id) are preserved.
        """
        await conn.execute(
            """
            UPDATE bank_integrations
            SET config = config
                || jsonb_build_object(
                    'encrypted_token', $2::text,
                    'key_version', $3::int
                )
            WHERE id = $1
            """,
            integration_id,
            bytes(encrypted_token).hex(),
            key_version,
        )

    async def delete_integration(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
    ) -> bool:
        """Hard-delete a bank_integrations row.

        Returns True if a row was deleted, False if none was found.
        Accounts linked to this integration have integration_id SET NULL via FK.
        """
        result = await conn.execute(
            """
            DELETE FROM bank_integrations
            WHERE id = $1
            """,
            integration_id,
        )
        return result == "DELETE 1"

    async def find_orphan_accounts_for_rebind(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        monobank_client_id: str,
        external_account_ids: list[str],
    ) -> list[AccountRow]:
        """Find accounts left orphaned by a prior hard-deleted integration.

        Criteria:
        - source = 'monobank'
        - user_id matches the caller
        - monobank_client_id matches (same Monobank user, not a different person)
        - integration_id IS NULL (FK was SET NULL when integration was deleted)
        - external_id is among the incoming Monobank account IDs

        Only accounts matching all criteria are rebound; accounts from a
        previously-deleted *different* Monobank user are never included.
        """
        rows = await conn.fetch(
            """
            SELECT
                id,
                external_id,
                currency_code
            FROM accounts
            WHERE user_id = $1
                AND source = 'monobank'
                AND config->>'monobank_client_id' = $2
                AND integration_id IS NULL
                AND external_id = ANY($3::text[])
            """,
            user_id,
            monobank_client_id,
            external_account_ids,
        )
        return [
            AccountRow(
                id=row["id"],
                external_id=row["external_id"],
                currency_code=row["currency_code"],
            )
            for row in rows
        ]

    async def rebind_account(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        new_integration_id: UUID,
    ) -> None:
        """Point an orphaned account at the newly-created integration."""
        await conn.execute(
            """
            UPDATE accounts
            SET integration_id = $2
            WHERE id = $1
            """,
            account_id,
            new_integration_id,
        )

    async def set_account_monobank_client_id(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        monobank_client_id: str,
    ) -> None:
        """Persist monobank_client_id into accounts.config for orphan rebind matching.

        Merges the key into the existing config JSONB so other fields are preserved.
        """
        await conn.execute(
            """
            UPDATE accounts
            SET config = config || jsonb_build_object('monobank_client_id', $2::text)
            WHERE id = $1
            """,
            account_id,
            monobank_client_id,
        )

    async def list_integrations_for_user(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> list[BankIntegrationRow]:
        """Fetch all active Monobank integrations for a user."""
        rows = await conn.fetch(
            """
            SELECT
                id,
                user_id,
                config,
                created_at
            FROM bank_integrations
            WHERE user_id = $1
                AND bank = 'monobank'
                AND status = 'active'
            ORDER BY created_at ASC
            """,
            user_id,
        )
        result = []
        for row in rows:
            raw_config = row["config"]
            config = (
                json.loads(raw_config)
                if isinstance(raw_config, str)
                else dict(raw_config)
            )
            result.append(
                BankIntegrationRow(
                    id=row["id"],
                    user_id=row["user_id"],
                    monobank_client_id=config.get("monobank_client_id", ""),
                    config=config,
                    created_at=row["created_at"],
                )
            )
        return result

    async def list_accounts_for_integration(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
    ) -> list[AccountRow]:
        """Fetch all accounts tied to a given integration."""
        rows = await conn.fetch(
            """
            SELECT
                id,
                external_id,
                currency_code
            FROM accounts
            WHERE integration_id = $1
                AND source = 'monobank'
            """,
            integration_id,
        )
        return [
            AccountRow(
                id=row["id"],
                external_id=row["external_id"],
                currency_code=row["currency_code"],
            )
            for row in rows
        ]
