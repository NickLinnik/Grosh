"""Integration tests for the full PipelineOrchestrator with transfer detection.

§16 of the transfer detection test specification.

These tests run PipelineOrchestrator.run() end-to-end against real Postgres.
Currency rates are seeded so CurrencyConversionService can complete without
failing. Each test uses the standard `conn` fixture (rolled-back transaction).
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from grosh_consumer.repositories.account_property_repo import AccountPropertyRepo
from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.repositories.transfer_repo import TransferQueryRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_consumer.services.pipeline import PipelineOrchestrator
from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.integration.conftest import (
    count_anomalies,
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)
from tests.integration.helpers import insert_rate, insert_source_config

pytestmark = pytest.mark.asyncio

T = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"
MCC_GROCERY = "5411"


def _make_pipeline() -> PipelineOrchestrator:
    """Wire up a complete PipelineOrchestrator with Monobank transfer strategy."""
    rate_repo = CurrencyRateRepo()
    conversion = CurrencyConversionService(rate_repo)
    transfer_repo = TransferQueryRepo()
    account_property_repo = AccountPropertyRepo()
    anomaly_repo = AnomalyRepo()
    monobank_strategy = MonobankTransferDetection(
        transfer_repo, account_property_repo, anomaly_repo
    )
    return PipelineOrchestrator(
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
# §16.1 Transfer detection + currency conversion + persistence
# ---------------------------------------------------------------------------


async def test_fop_to_card_transfer_both_legs_persisted_as_transfer(conn):
    """§144: FOP→card: both legs persisted as 'transfer', related_id cross-linked."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    # Leg 1: FOP expense — Tier A matchable (counterparty_iban = black card IBAN).
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
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )
    await pipeline.run(conn, leg1)

    # After leg 1: no partner yet → inserted as expense, special_category NULL.
    leg1_row = await get_transaction(conn, leg1_id)
    assert str(leg1_row["direction"]) == "expense"
    assert leg1_row["special_category"] is None
    assert leg1_row["related_transaction_id"] is None

    # Leg 2: black card income — Tier A finds leg1 via counterparty_iban.
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
        counterparty_iban="UA111000000000000000000000001",
        description="З гривневого рахунку ФОП",
    )
    await pipeline.run(conn, leg2)

    leg1_row = await get_transaction(conn, leg1_id)
    leg2_row = await get_transaction(conn, leg2_id)

    assert str(leg1_row["special_category"]) == "transfer"
    assert str(leg2_row["special_category"]) == "transfer"
    assert leg1_row["related_transaction_id"] == leg2_id
    assert leg2_row["related_transaction_id"] == leg1_id

    assert await count_anomalies(conn, user_id) == 0


async def test_external_p2p_persisted_as_expense_no_transfer_side_effects(conn):
    """§145: External P2P — person name description, MCC 4829, no IBAN.

    Persisted as expense. No anomaly. No related_transaction_id.
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


async def test_non_mcc_4829_transfer_detection_skipped(conn):
    """§146: MCC 5411 grocery — transfer detection skipped. Persisted as expense."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
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
    assert str(row["direction"]) == "expense"
    assert row["special_category"] is None
    assert row["related_transaction_id"] is None
    assert await count_anomalies(conn, user_id) == 0


# ---------------------------------------------------------------------------
# §16.2 Idempotency through the full pipeline
# ---------------------------------------------------------------------------


async def test_duplicate_event_second_invocation_is_noop(conn):
    """§147: Duplicate event (same ID) — second pipeline.run() is a no-op.

    ON CONFLICT (id) DO NOTHING on INSERT. Idempotency guard in transfer
    detection prevents spurious anomalies. Only one row in DB.
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
        "SELECT COUNT(*) FROM transactions WHERE id = $1",
        tx_id,
    )
    assert int(count) == 1
    assert await count_anomalies(conn, user_id) == 0


async def test_redelivery_after_successful_pair_no_duplicate_anomaly(conn):
    """§148: A and B paired. Re-deliver A. Idempotency guard sees A exists → no-op."""
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
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
        counterparty_iban="UA444000000000000000000000004",
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
        counterparty_iban="UA111000000000000000000000001",
        description="З гривневого рахунку ФОП",
    )
    await pipeline.run(conn, leg_b)

    row_a = await get_transaction(conn, leg_a_id)
    assert str(row_a["special_category"]) == "transfer"

    # Re-deliver A — idempotency guard fires, no side effects.
    await pipeline.run(conn, leg_a)

    count_a = await conn.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE id = $1", leg_a_id
    )
    assert int(count_a) == 1
    assert await count_anomalies(conn, user_id) == 0


# ---------------------------------------------------------------------------
# §16.3 Source dispatch
# ---------------------------------------------------------------------------


async def test_monobank_source_uses_monobank_strategy(conn):
    """§149: tx.source='monobank' → MonobankTransferDetection dispatched.

    Verified by observing that Tier A pairing succeeds — which only happens
    because the Monobank strategy is registered. Without it, the tx would
    be inserted as expense regardless of MCC and IBAN.
    """
    pipeline = _make_pipeline()
    await _seed_uah_rates(conn)

    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
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
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert str(row["special_category"]) == "transfer"


async def test_manual_source_transfer_detection_skipped(conn):
    """§150: tx.source='manual' → no strategy registered → transfer detection skipped.

    Transaction persisted with original direction; special_category stays NULL.
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


async def test_unknown_source_transfer_detection_skipped_no_error(conn):
    """§151: Source not in strategies registry → detection skipped, pipeline continues.

    We use source='manual' (valid DB enum) as a proxy: the pipeline registry
    only has 'monobank', so 'manual' follows the no-strategy code path.
    Transaction is persisted with original direction; special_category stays NULL.
    No error raised.
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
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",
    )
    await pipeline.run(conn, event)

    row = await get_transaction(conn, tx_id)
    assert str(row["direction"]) == "income"
    assert row["special_category"] is None
    assert row["related_transaction_id"] is None
