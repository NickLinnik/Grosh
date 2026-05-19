import logging

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

# Sub-key under which all pipeline-layer outputs are nested.
_LAYER_KEY = "layer"


class PipelineOrchestrator:
    """Runs the enrichment pipeline for a single NormalizedTransaction.

    Pipeline stages (in order):
    1. Transfer detection — dispatches to a source-specific strategy that
       sets special_category='transfer' for paired transfers and records anomalies.
    2. Currency conversion — converts amount_cents to UAH/USD/EUR display amounts.
    3. Persistence — inserts the enriched transaction into the database.

    Metadata shape written to the row:
        {
          "source": { ... },           # written by the normalizer
          "layer": {
            "rate": { ... },           # written by currency conversion
            "transfer": { ... },       # written by transfer detection (Slice 17+)
          }
        }
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
        account = await self._account_repo.get_by_id(conn, tx.account_id)
        account_currency = account.currency_code

        transfer_result = await self._run_transfer_detection(conn, tx)
        related_transaction_id = transfer_result.related_transaction_id

        conversion = await self._conversion.convert(conn, tx, account_currency)

        layer_contributions: list[tuple[str, dict[str, object]]] = []
        if conversion.rate_metadata:
            layer_contributions.append(("rate", conversion.rate_metadata))
        if transfer_result.metadata_block is not None:
            layer_contributions.append(("transfer", transfer_result.metadata_block))

        metadata = _build_metadata(tx.metadata, layer_contributions)

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


def _build_metadata(
    source_metadata: dict[str, object] | None,
    layer_contributions: list[tuple[str, dict[str, object]]],
) -> dict[str, object] | None:
    """Assemble the stored metadata JSONB from source fields and layer outputs.

    Final shape:
        {
          "source": { ... },   # written by the normalizer, passed through verbatim
          "layer": {
            "<namespace>": { ... },   # one entry per contributing layer
          }
        }

    Source metadata passes through verbatim.  This function's only responsibility
    is merging layer outputs under the "layer" sub-key.

    Raises ValueError if two layers attempt to claim the same namespace — this
    is a programming error, not a runtime condition, so it must surface loud.
    """
    has_layers = bool(layer_contributions)

    if not has_layers:
        return source_metadata

    result: dict[str, object] = dict(source_metadata) if source_metadata else {}

    layer: dict[str, object] = {}
    for namespace, payload in layer_contributions:
        if namespace in layer:
            raise ValueError(
                f"Two pipeline layers both claim metadata namespace {namespace!r}."
                " Each layer must use a unique namespace under metadata.layer."
            )
        layer[namespace] = payload
    result[_LAYER_KEY] = layer

    return result
