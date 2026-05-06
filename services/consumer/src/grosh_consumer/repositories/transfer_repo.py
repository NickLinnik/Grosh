"""Transfer detection query repository.

Owns all SQL for the three-tier transfer detection algorithm.
No business logic lives here — only DB queries.
"""

from datetime import datetime
from uuid import UUID

import asyncpg


class TransferQueryRepo:
    """SQL queries for transfer detection tiers.

    All partner-search queries use FOR UPDATE SKIP LOCKED to avoid
    racing concurrent pipeline workers claiming the same candidate.
    """

    async def exists(
        self,
        conn: asyncpg.Connection,
        tx_id: UUID,
    ) -> bool:
        """True if a transaction with this ID is already in the table."""
        val = await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1
                FROM transactions
                WHERE id = $1
            )
            """,
            tx_id,
        )
        return bool(val)

    async def find_unclaimed_partner_tier_a(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        target_account_id: UUID,
        opposite_type: str,
        time: datetime,
        window_seconds: int,
    ) -> list[asyncpg.Record]:
        """Tier A: find unclaimed partner on a known target account (IBAN match).

        Uses idx_transactions_transfer_tier_a.
        """
        return await conn.fetch(
            """
            SELECT
                id,
                account_id,
                direction,
                time,
                amount_cents,
                operation_amount_cents,
                counterparty_iban,
                description
            FROM transactions
            WHERE user_id = $1
              AND account_id = $2
              AND direction = $3
              AND mcc = '4829'
              AND related_transaction_id IS NULL
              AND time BETWEEN $4::timestamptz - ($5 * INTERVAL '1 second')
                          AND $4::timestamptz + ($5 * INTERVAL '1 second')
            FOR UPDATE SKIP LOCKED
            """,
            user_id,
            target_account_id,
            opposite_type,
            time,
            window_seconds,
        )

    async def find_unclaimed_partner_tier_b(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        account_iban: str,
        opposite_type: str,
        time: datetime,
        window_seconds: int,
    ) -> list[asyncpg.Record]:
        """Tier B: find unclaimed partner that has my account IBAN as counterparty.

        Uses idx_transactions_transfer_tier_b.
        """
        return await conn.fetch(
            """
            SELECT
                id,
                account_id,
                direction,
                time,
                amount_cents,
                operation_amount_cents,
                counterparty_iban,
                description
            FROM transactions
            WHERE user_id = $1
              AND counterparty_iban = $2
              AND direction = $3
              AND mcc = '4829'
              AND related_transaction_id IS NULL
              AND time BETWEEN $4::timestamptz - ($5 * INTERVAL '1 second')
                          AND $4::timestamptz + ($5 * INTERVAL '1 second')
            FOR UPDATE SKIP LOCKED
            """,
            user_id,
            account_iban,
            opposite_type,
            time,
            window_seconds,
        )

    async def find_unclaimed_partner_tier_c(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        operation_amount_cents: int,
        opposite_type: str,
        time: datetime,
        incoming_account_id: UUID,
        window_seconds: int,
    ) -> list[asyncpg.Record]:
        """Tier C: amount-based fallback when both IBANs are NULL.

        Uses idx_transactions_transfer_tier_c.
        """
        return await conn.fetch(
            """
            SELECT
                id,
                account_id,
                direction,
                time,
                amount_cents,
                operation_amount_cents,
                counterparty_iban,
                description
            FROM transactions
            WHERE user_id = $1
              AND amount_cents = $2
              AND direction = $3
              AND mcc = '4829'
              AND counterparty_iban IS NULL
              AND related_transaction_id IS NULL
              AND account_id != $4
              AND time BETWEEN $5::timestamptz - ($6 * INTERVAL '1 second')
                          AND $5::timestamptz + ($6 * INTERVAL '1 second')
            FOR UPDATE SKIP LOCKED
            """,
            user_id,
            operation_amount_cents,
            opposite_type,
            incoming_account_id,
            time,
            window_seconds,
        )

    async def claim_pair(
        self,
        conn: asyncpg.Connection,
        existing_tx_id: UUID,
        new_tx_id: UUID,
    ) -> None:
        """Mark an existing transaction as claimed by the new partner."""
        await conn.execute(
            """
            UPDATE transactions
            SET related_transaction_id = $2,
                special_category = 'transfer'
            WHERE id = $1
            """,
            existing_tx_id,
            new_tx_id,
        )
