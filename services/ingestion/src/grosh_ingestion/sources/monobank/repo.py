from dataclasses import dataclass
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class IntegrationRef:
    id: UUID
    user_id: UUID


@dataclass(frozen=True)
class AccountRef:
    id: UUID


class MonobankRepo:
    async def get_active_integration_by_webhook_secret(
        self, conn: asyncpg.Connection, webhook_secret: str
    ) -> IntegrationRef | None:
        row = await conn.fetchrow(
            """
            SELECT id, user_id
            FROM bank_integrations
            WHERE config->>'webhook_secret' = $1 AND status = 'active'
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
            WHERE external_id = $1 AND integration_id = $2
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

        Used by link() and relink() flows in MonobankLinkingService.
        Returns raw bytes (the caller hex-encodes for storage in config JSONB).
        """
        return await conn.fetchval(
            """
            SELECT pgp_sym_encrypt($1, $2)
            """,
            plaintext_token,
            encryption_key,
        )

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
