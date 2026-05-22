"""Integration tests for the full EnrichmentOrchestrator with v2 transfer detection.

Test cases per consumer-transfer-detection-test-suite.md §18 (cases 225–232).
Skipped at module level until orchestrator merge wiring lands (Slice 17 task T14).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from grosh_enrichment.repositories.account_repo import AccountRepo
from grosh_enrichment.repositories.anomaly_repo import AnomalyRepo
from grosh_enrichment.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_enrichment.repositories.transaction_repo import TransactionRepo
from grosh_enrichment.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_enrichment.services.enrichment_orchestrator import EnrichmentOrchestrator
from grosh_enrichment.sources.monobank.transfer.detector import (
    MonobankTransferDetection,
)
from grosh_enrichment.sources.monobank.transfer.repo import TransferQueryRepo
from tests.helpers import make_event
from tests.integration.conftest import (
    count_anomalies,
    get_transaction,
    get_transfer_metadata,
    insert_account,
    insert_transaction,
    insert_user,
)
from tests.integration.helpers import insert_rate, insert_source_config

pytestmark = pytest.mark.asyncio

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"
MCC_GROCERY = "5411"

UAH_FOP_IBAN = "UA111000000000000000000111"
UAH_BLACK_IBAN = "UA444000000000000000000444"


def _make_pipeline() -> "EnrichmentOrchestrator":
    """Wire up a complete EnrichmentOrchestrator with v2 Monobank transfer strategy."""
    rate_repo = CurrencyRateRepo()
    conversion = CurrencyConversionService(rate_repo)
    transfer_repo = TransferQueryRepo()
    account_repo = AccountRepo()
    anomaly_repo = AnomalyRepo()
    monobank_strategy = MonobankTransferDetection(
        transfer_repo, account_repo, anomaly_repo
    )
    return EnrichmentOrchestrator(
        transaction_repo=TransactionRepo(),
        account_repo=AccountRepo(),
        conversion=conversion,
        anomaly_repo=anomaly_repo,
        transfer_strategies={"monobank": monobank_strategy},
    )


async def _seed_uah_rates(conn) -> None:
    """Seed minimal UAH currency rates so conversion succeeds for UAH transactions."""
    await insert_source_config(conn, source="monobank", fallback_source=None)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        rate_buy=0.024,
        rate_sell=0.026,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="EUR",
        rate_mid=0.023,
        rate_buy=0.022,
        rate_sell=0.024,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )


# ---------------------------------------------------------------------------
# §18.1 Successful claim + persistence + metadata wiring (case 225)
# ---------------------------------------------------------------------------


async def test_225_fop_to_card_both_legs_full_pipeline(conn):
    """Case 225: FOP→card, both legs through pipeline.

    Verify special_category and metadata.layer.transfer set on both rows.
    """
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    leg1_id = uuid4()
    leg1 = make_event(
        id=leg1_id,
        source="monobank",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
    )
    await pipeline.run(conn, leg1)

    leg1_row = await get_transaction(conn, leg1_id)
    assert leg1_row["special_category"] is None  # no partner yet

    leg2_id = uuid4()
    leg2 = make_event(
        id=leg2_id,
        source="monobank",
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )
    await pipeline.run(conn, leg2)

    leg1_row = await get_transaction(conn, leg1_id)
    leg2_row = await get_transaction(conn, leg2_id)

    assert str(leg1_row["special_category"]) == "transfer"
    assert str(leg2_row["special_category"]) == "transfer"
    assert leg1_row["related_transaction_id"] == leg2_id
    assert leg2_row["related_transaction_id"] == leg1_id

    # amount_uah_cents populated (currency conversion ran).
    assert leg1_row["amount_uah_cents"] is not None
    assert leg2_row["amount_uah_cents"] is not None

    # metadata.layer.rate present on both legs.
    import json as _json

    leg1_meta = (
        _json.loads(leg1_row["metadata"])
        if isinstance(leg1_row["metadata"], str)
        else leg1_row["metadata"]
    )
    leg2_meta = (
        _json.loads(leg2_row["metadata"])
        if isinstance(leg2_row["metadata"], str)
        else leg2_row["metadata"]
    )
    assert leg1_meta["layer"].get("rate") is not None
    assert leg2_meta["layer"].get("rate") is not None

    # metadata.layer.transfer.row present on both legs.
    leg1_meta = await get_transfer_metadata(conn, leg1_id)
    leg2_meta = await get_transfer_metadata(conn, leg2_id)
    assert leg1_meta is not None and "row" in leg1_meta
    assert leg2_meta is not None and "row" in leg2_meta

    # metadata.layer.transfer.pair identical on both legs.
    assert leg1_meta["pair"] == leg2_meta["pair"]
    assert leg1_meta["pair"]["iban_evidence"] == "bilateral"
    assert leg1_meta["pair"]["description_decisive"] is False

    assert await count_anomalies(conn, user_id) == 0


# ---------------------------------------------------------------------------
# §18.2 Non-claim paths (cases 226–227)
# ---------------------------------------------------------------------------


async def test_226_unlinked_p2p_no_related_id_no_anomaly_row_block_present(conn):
    """Case 226: unlinked P2P expense (person name, no IBAN).

    Result: plain expense; row block present; no pair.
    """
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
    )

    tx_id = uuid4()
    event = make_event(
        id=tx_id,
        source="monobank",
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=20000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=None,
        description="Олена К.",
    )
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert str(row["direction"]) == "expense"
    assert row["special_category"] is None
    assert row["related_transaction_id"] is None
    assert await count_anomalies(conn, user_id) == 0

    # metadata.layer.transfer.row must be present (MCC 4829 invariant).
    tx_meta = await get_transfer_metadata(conn, tx_id)
    assert tx_meta is not None
    assert "row" in tx_meta
    assert tx_meta.get("pair") is None


async def test_227_non_mcc4829_no_transfer_block(conn):
    """Case 227: MCC 5411 grocery → persisted normally; no metadata.layer.transfer."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
    )

    tx_id = uuid4()
    event = make_event(
        id=tx_id,
        source="monobank",
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_GROCERY,
        amount_cents=35000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=None,
        description="Сільпо",
    )
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert row["special_category"] is None

    # metadata.layer.transfer must be absent (non-4829 invariant).
    tx_meta = await get_transfer_metadata(conn, tx_id)
    assert tx_meta is None


# ---------------------------------------------------------------------------
# §18.3 Idempotency (cases 228–229b)
# ---------------------------------------------------------------------------


async def test_228_duplicate_event_second_run_is_noop(conn):
    """Case 228: duplicate event (same ID) → second pipeline.run() is a no-op."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
    )

    tx_id = uuid4()
    event = make_event(
        id=tx_id,
        source="monobank",
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_GROCERY,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=None,
        description="Groceries",
    )

    await pipeline.run(conn, event)
    await pipeline.run(conn, event)

    count = await conn.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE id = $1", tx_id
    )
    assert int(count) == 1
    assert await count_anomalies(conn, user_id) == 0


async def test_229_redelivery_after_pair_claim_no_double_claim(conn):
    """Case 229: A and B paired. Re-deliver A. Strategy exists() guard → no-op."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    leg_a_id = uuid4()
    leg_a = make_event(
        id=leg_a_id,
        source="monobank",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
    )
    await pipeline.run(conn, leg_a)

    leg_b_id = uuid4()
    leg_b = make_event(
        id=leg_b_id,
        source="monobank",
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )
    await pipeline.run(conn, leg_b)

    row_a = await get_transaction(conn, leg_a_id)
    assert str(row_a["special_category"]) == "transfer"

    # Re-deliver A — idempotency guard fires.
    await pipeline.run(conn, leg_a)

    count_a = await conn.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE id = $1", leg_a_id
    )
    assert int(count_a) == 1
    assert await count_anomalies(conn, user_id) == 0


async def test_229b_reprocess_replay_re_claims_pair(conn):
    """Case 229b: reprocess — A+B paired, deleted, re-published, re-claimed.

    Verifies idempotency: exists() reflects DB state at call time,
    not 'did this event ever exist'. ON CONFLICT DO NOTHING does not fire after delete.
    """
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    leg_a_id = uuid4()
    leg_a = make_event(
        id=leg_a_id,
        source="monobank",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
    )
    leg_b_id = uuid4()
    leg_b = make_event(
        id=leg_b_id,
        source="monobank",
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )

    # Initial ingestion: both paired.
    await pipeline.run(conn, leg_a)
    await pipeline.run(conn, leg_b)

    row_a = await get_transaction(conn, leg_a_id)
    assert str(row_a["special_category"]) == "transfer"

    # Simulate reprocess: delete both rows.
    # CASCADE clears anomalies; SET NULL clears related_id on partner.
    await conn.execute("DELETE FROM transactions WHERE id = $1", leg_a_id)
    await conn.execute("DELETE FROM transactions WHERE id = $1", leg_b_id)

    # Verify rows are gone.
    count_before = await conn.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE id = ANY($1::uuid[])",
        [leg_a_id, leg_b_id],
    )
    assert int(count_before) == 0

    # Re-publish: pipeline.run() processes both events again from scratch.
    await pipeline.run(conn, leg_a)
    await pipeline.run(conn, leg_b)

    # Final state: A and B re-paired.
    row_a2 = await get_transaction(conn, leg_a_id)
    row_b2 = await get_transaction(conn, leg_b_id)
    assert str(row_a2["special_category"]) == "transfer"
    assert str(row_b2["special_category"]) == "transfer"
    assert (
        row_a2["related_transaction_id"] == leg_b_id
        or row_b2["related_transaction_id"] == leg_a_id
    )

    # No duplicate anomaly, no orphan state.
    assert await count_anomalies(conn, user_id) == 0


# ---------------------------------------------------------------------------
# §18.4 Source dispatch (cases 230–232)
# ---------------------------------------------------------------------------


async def test_230_monobank_source_dispatches_to_strategy(conn):
    """Case 230: tx.source='monobank' → MonobankTransferDetection dispatched.

    Pairing claim succeeds.
    """
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    # Pre-existing income on black card.
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    tx_id = uuid4()
    event = make_event(
        id=tx_id,
        source="monobank",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
    )
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert str(row["special_category"]) == "transfer"


async def test_231_manual_source_no_strategy_special_category_null(conn):
    """Case 231: tx.source='manual' → no strategy registered.

    special_category=NULL; no transfer block.
    """
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
        source="manual",
    )

    tx_id = uuid4()
    event = make_event(
        id=tx_id,
        source="manual",
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert str(row["direction"]) == "expense"
    assert row["special_category"] is None
    assert row["related_transaction_id"] is None

    # No transfer block: orchestrator writes block only when strategy returns non-None.
    tx_meta = await get_transfer_metadata(conn, tx_id)
    assert tx_meta is None


async def test_232_unknown_source_no_error_pipeline_continues(conn):
    """Case 232: tx.source='revolut' (not in registry) → INFO log; no error raised."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
        source="manual",
    )

    tx_id = uuid4()
    # Use source='manual' as proxy for unknown source (valid enum; not in strategies).
    event = make_event(
        id=tx_id,
        source="manual",
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_currency_code="UAH",
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",
    )
    # Must not raise.
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert str(row["direction"]) == "income"
    assert row["special_category"] is None
    assert row["related_transaction_id"] is None
