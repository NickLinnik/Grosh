import logging
from typing import Any

import asyncpg

from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_consumer.services.transfer_detection import (
    TransferDetectionStrategy,
    TransferResult,
)

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Runs the enrichment pipeline for a single NormalizedTransaction.

    Pipeline stages (in order):
    1. Transfer detection — dispatches to a source-specific strategy that
       sets special_category='transfer' for paired transfers and records anomalies.
    2. Currency conversion — converts amount_cents to UAH/USD/EUR display amounts.
    3. Persistence — inserts the enriched transaction into the database.
    """

    def __init__(
        self,
        transaction_repo: TransactionRepo,
        account_repo: AccountRepo,
        conversion: CurrencyConversionService,
        anomaly_repo: AnomalyRepo,
        transfer_strategies: dict[str, TransferDetectionStrategy],
    ) -> None:
        self._transaction_repo = transaction_repo
        self._account_repo = account_repo
        self._conversion = conversion
        self._anomaly_repo = anomaly_repo
        self._transfer_strategies = transfer_strategies

    async def run(self, conn: asyncpg.Connection, tx: NormalizedTransaction) -> None:
        account_currency = await self._account_repo.get_currency_code(
            conn, tx.account_id
        )

        transfer_result = await self._run_transfer_detection(conn, tx)
        related_transaction_id = transfer_result.related_transaction_id

        conversion = await self._conversion.convert(conn, tx, account_currency)
        metadata = _merge_metadata(tx.metadata, conversion.rate_metadata)

        await self._transaction_repo.insert(
            conn,
            tx.id,
            tx,
            account_currency,
            transfer_result.special_category,
            conversion.amounts,
            metadata,
            related_transaction_id,
        )

        for anomaly in transfer_result.anomalies:
            await self._anomaly_repo.record_anomaly(
                conn,
                anomaly.transaction_id,
                anomaly.candidate_ids,
                anomaly.reason_code,
                anomaly.reason_detail,
            )

    async def _run_transfer_detection(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
    ) -> TransferResult:
        strategy = self._transfer_strategies.get(tx.source)
        if strategy is None:
            logger.info(
                "No transfer detection strategy for source=%r; skipping", tx.source
            )
            return TransferResult()
        return await strategy.detect_and_pair(conn, tx)


def _merge_metadata(
    original: dict[str, object] | None, rate_meta: dict[str, Any]
) -> dict[str, object] | None:
    if not rate_meta:
        return original
    merged: dict[str, Any] = original.copy() if original else {}
    merged.update(rate_meta)
    return merged
