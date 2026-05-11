import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.models import Currency, TransactionOrigin, TransactionSource

from grosh_consumer.models.normalized import NormalizedTransaction


@dataclass(frozen=True)
class TransactionRow:
    """Typed representation of a row read back from the transactions table.

    Used by the reprocess job to reconstruct NormalizedTransaction objects.
    Fields match the column names exactly; nullable DB columns use Optional types.
    """

    id: UUID
    source: str
    source_id: str
    user_id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_amount_cents: int | None
    operation_currency_code: str | None
    description: str | None
    mcc: str | None
    cashback_amount_cents: int
    balance_cents: int | None
    hold: bool | None
    direction: str
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, Any] | None

    def to_normalized(self) -> NormalizedTransaction:
        """Reconstruct a NormalizedTransaction from this stored row for reprocessing.

        metadata.layer is dropped entirely so each pipeline layer writes a fresh
        sub-namespace on replay; only metadata.source (bank-original fields) is
        preserved byte-identical. Per the ADR's "Preserved vs re-derived fields"
        boundary.
        """
        metadata_for_replay = {"source": (self.metadata or {}).get("source", {})}
        return NormalizedTransaction(
            id=self.id,
            source=self.source,
            source_id=self.source_id,
            user_id=self.user_id,
            account_id=self.account_id,
            time=self.time,
            amount_cents=self.amount_cents,
            operation_amount_cents=self.operation_amount_cents,
            operation_currency_code=self.operation_currency_code or "",
            description=self.description,
            mcc=self.mcc,
            cashback_amount_cents=self.cashback_amount_cents,
            balance_cents=self.balance_cents,
            hold=self.hold,
            direction=self.direction,
            counterparty_iban=self.counterparty_iban,
            rate_source=self.rate_source,
            metadata=metadata_for_replay,
        )


class TransactionRepo:
    async def select_for_user(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> list[TransactionRow]:
        """Return all transactions for a user ordered by (time, id) for replay."""
        rows = await conn.fetch(
            """
            SELECT
                id,
                source,
                source_id,
                user_id,
                account_id,
                time,
                amount_cents,
                operation_amount_cents,
                operation_currency_code,
                description,
                mcc,
                cashback_amount_cents,
                balance_cents,
                hold,
                direction,
                counterparty_iban,
                rate_source,
                metadata
            FROM transactions
            WHERE user_id = $1
            ORDER BY
                time,
                id
            """,
            user_id,
        )
        return [
            TransactionRow(
                id=row["id"],
                source=row["source"],
                source_id=row["source_id"],
                user_id=row["user_id"],
                account_id=row["account_id"],
                time=row["time"],
                amount_cents=row["amount_cents"],
                operation_amount_cents=row["operation_amount_cents"],
                operation_currency_code=row["operation_currency_code"],
                description=row["description"],
                mcc=row["mcc"],
                cashback_amount_cents=row["cashback_amount_cents"] or 0,
                balance_cents=row["balance_cents"],
                hold=row["hold"],
                direction=row["direction"],
                counterparty_iban=row["counterparty_iban"],
                rate_source=row["rate_source"],
                metadata=json.loads(row["metadata"])
                if isinstance(row["metadata"], str)
                else row["metadata"],
            )
            for row in rows
        ]

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
