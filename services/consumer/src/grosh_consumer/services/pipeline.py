from typing import Any

import asyncpg
from grosh_shared.models import TransactionType

from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)


class PipelineOrchestrator:
    """Runs the enrichment pipeline for a single NormalizedTransaction.

    Pipeline stages (in order):
    1. Transfer detection — promotes transaction_type to 'transfer' when the
       counterparty IBAN belongs to one of the user's own accounts.
    2. Currency conversion — converts amount_cents to UAH/USD/EUR display amounts.
    3. Persistence — inserts the enriched transaction into the database.
    """

    def __init__(
        self,
        transaction_repo: TransactionRepo,
        account_repo: AccountRepo,
        conversion: CurrencyConversionService,
    ) -> None:
        self._transaction_repo = transaction_repo
        self._account_repo = account_repo
        self._conversion = conversion

    async def run(self, conn: asyncpg.Connection, tx: NormalizedTransaction) -> None:
        account_currency = await self._account_repo.get_currency_code(
            conn, tx.account_id
        )
        transaction_type = await self._detect_transfer(conn, tx)
        conversion = await self._conversion.convert(conn, tx, account_currency)

        metadata = _merge_metadata(tx.metadata, conversion.rate_metadata)

        await self._transaction_repo.insert(
            conn,
            tx.id,
            tx,
            account_currency,
            transaction_type,
            conversion.amounts,
            metadata,
            None,
        )

    async def _detect_transfer(
        self, conn: asyncpg.Connection, tx: NormalizedTransaction
    ) -> str:
        if tx.counterparty_iban is None:
            return tx.transaction_type

        account_id = await self._account_repo.find_by_iban(
            conn, tx.counterparty_iban, tx.user_id
        )
        if account_id is not None:
            return TransactionType.transfer

        return tx.transaction_type


def _merge_metadata(
    original: dict[str, object] | None, rate_meta: dict[str, Any]
) -> dict[str, object] | None:
    if not rate_meta:
        return original
    merged: dict[str, Any] = original.copy() if original else {}
    merged.update(rate_meta)
    return merged
