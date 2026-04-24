from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.events import RawTransactionEvent
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.models import TransactionType

from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)


class TransactionHandler:
    def __init__(
        self,
        transaction_repo: TransactionRepo,
        account_repo: AccountRepo,
        conversion: CurrencyConversionService,
    ) -> None:
        self._transaction_repo = transaction_repo
        self._account_repo = account_repo
        self._conversion = conversion

    async def handle(
        self, conn: asyncpg.Connection, event: RawTransactionEvent
    ) -> None:
        transaction_type = await self._detect_transfer(conn, event)
        transaction_id, related_id = await self._resolve_hold_tx_ids(conn, event)
        conversion = await self._conversion.convert(conn, event)

        metadata = _merge_metadata(event.metadata, conversion.rate_metadata)

        await self._transaction_repo.insert(
            conn,
            transaction_id,
            event,
            transaction_type,
            conversion.amounts,
            metadata,
            related_id,
        )

    async def _detect_transfer(
        self, conn: asyncpg.Connection, event: RawTransactionEvent
    ) -> str:
        if event.counterparty_iban is None:
            return event.transaction_type

        account_id = await self._account_repo.find_by_iban(
            conn, event.counterparty_iban, event.user_id
        )
        if account_id is not None:
            return TransactionType.transfer

        return event.transaction_type

    async def _resolve_hold_tx_ids(
        self, conn: asyncpg.Connection, event: RawTransactionEvent
    ) -> tuple[UUID, UUID | None]:
        if event.hold:
            return event.id, None

        hold_exists = await self._transaction_repo.hold_exists(conn, event.id)
        if hold_exists:
            settlement_id = generate_transaction_id(
                event.source, event.source_id + ":settled"
            )
            return settlement_id, event.id

        return event.id, None


def _merge_metadata(original: dict | None, rate_meta: dict[str, Any]) -> dict | None:
    if not rate_meta:
        return original
    merged = original.copy() if original else {}
    merged.update(rate_meta)
    return merged
