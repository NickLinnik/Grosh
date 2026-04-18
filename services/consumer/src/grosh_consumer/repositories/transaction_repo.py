import json
from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.events import RawTransactionEvent
from grosh_shared.models import Currency, TransactionOrigin


class TransactionRepo:
    async def find_account_by_iban(
        self,
        conn: asyncpg.Connection,
        iban: str,
        user_id: UUID,
    ) -> UUID | None:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM accounts
            WHERE iban = $1
              AND user_id = $2
            """,
            iban,
            user_id,
        )
        return row["id"] if row is not None else None

    async def hold_exists(
        self,
        conn: asyncpg.Connection,
        tx_id: UUID,
    ) -> bool:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM transactions
            WHERE id = $1
              AND hold = true
            """,
            tx_id,
        )
        return row is not None

    async def insert(
        self,
        conn: asyncpg.Connection,
        tx_id: UUID,
        event: RawTransactionEvent,
        transaction_type: str,
        converted: dict[Currency, int | None],
        metadata: dict[str, Any] | None,
        related_transaction_id: UUID | None,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO transactions (
                id, source_id, user_id, account_id, time,
                amount_cents, operation_amount_cents, currency_code,
                amount_uah_cents, amount_usd_cents, amount_eur_cents,
                description, mcc, cashback_amount_cents, balance_cents,
                hold, transaction_type, counterparty_iban,
                metadata, source, origin, related_transaction_id
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11,
                $12, $13, $14, $15,
                $16, $17, $18,
                $19, $20, $21, $22
            )
            ON CONFLICT (id, time) DO NOTHING
            """,
            tx_id,
            event.source_id,
            event.user_id,
            event.account_id,
            event.time,
            event.amount_cents,
            event.operation_amount_cents,
            event.currency_code,
            converted.get(Currency.UAH),
            converted.get(Currency.USD),
            converted.get(Currency.EUR),
            event.description,
            event.mcc,
            event.cashback_amount_cents,
            event.balance_cents,
            event.hold,
            transaction_type,
            event.counterparty_iban,
            json.dumps(metadata) if metadata else None,
            event.source,
            TransactionOrigin.bank,
            related_transaction_id,
        )
