import json
from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.models import Currency, TransactionOrigin, TransactionSource

from grosh_consumer.models.normalized import NormalizedTransaction


class TransactionRepo:
    async def insert(
        self,
        conn: asyncpg.Connection,
        tx_id: UUID,
        tx: NormalizedTransaction,
        account_currency: str,
        special_category: str | None,
        converted: dict[Currency, int | None],
        metadata: dict[str, Any] | None,
        related_transaction_id: UUID | None,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO transactions (
                id, source_id, user_id, account_id, time,
                amount_cents, operation_amount_cents,
                currency_code, operation_currency_code,
                amount_uah_cents, amount_usd_cents, amount_eur_cents,
                description, mcc, cashback_amount_cents, balance_cents,
                hold, direction, special_category, counterparty_iban,
                rate_source, metadata, source, origin, related_transaction_id
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7,
                $8, $9,
                $10, $11, $12,
                $13, $14, $15, $16,
                $17, $18, $19, $20,
                $21, $22, $23, $24, $25
            )
            ON CONFLICT (id) DO NOTHING
            """,
            tx_id,
            tx.source_id,
            tx.user_id,
            tx.account_id,
            tx.time,
            tx.amount_cents,
            tx.operation_amount_cents,
            account_currency,
            tx.operation_currency_code,
            converted.get(Currency.UAH),
            converted.get(Currency.USD),
            converted.get(Currency.EUR),
            tx.description,
            tx.mcc,
            tx.cashback_amount_cents,
            tx.balance_cents,
            tx.hold,
            tx.direction,
            special_category,
            tx.counterparty_iban,
            tx.rate_source,
            json.dumps(metadata) if metadata else None,
            tx.source,
            TransactionOrigin.manual
            if tx.source == TransactionSource.manual
            else TransactionOrigin.bank,
            related_transaction_id,
        )
