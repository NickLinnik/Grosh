"""Integration tests for metadata.layer.transfer block invariants.

Test cases per `references/consumer-transfer-detection-test-suite.md` §12.
Skipped at module level until claim_pair (T12) and orchestrator merge (T13/T14) land.
The corresponding implementation tasks remove this marker.
"""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from grosh_shared.normalized import NormalizedTransaction

from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_consumer.services.pipeline import PipelineOrchestrator
from grosh_consumer.sources.monobank.transfer.detector import (
    MonobankTransferDetection,
)
from grosh_consumer.sources.monobank.transfer.repo import TransferQueryRepo
from tests.integration.conftest import (
    get_transfer_metadata,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"
MCC_GROCERY = "5411"

UAH_FOP_IBAN = "UA111000000000000000000111"
UAH_BLACK_IBAN = "UA444000000000000000000444"
EXTERNAL_IBAN = "UA999000000000000000000999"


def _make_strategy():
    return MonobankTransferDetection(
        TransferQueryRepo(),
        AccountRepo(),
        AnomalyRepo(),
    )


def _make_pipeline():
    rate_repo = CurrencyRateRepo()
    conversion = CurrencyConversionService(rate_repo)
    strategy = _make_strategy()
    return PipelineOrchestrator(
        transaction_repo=TransactionRepo(),
        account_repo=AccountRepo(),
        conversion=conversion,
        anomaly_repo=AnomalyRepo(),
        transfer_strategies={"monobank": strategy},
    )


def _make_tx(
    *,
    user_id,
    account_id,
    direction,
    mcc=MCC_TRANSFER,
    cp_iban=None,
    desc=None,
    amount=5000,
):
    return NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=account_id,
        time=T,
        amount_cents=amount,
        operation_amount_cents=amount,
        operation_currency_code="UAH",
        description=desc,
        mcc=mcc,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction=direction,
        counterparty_iban=cp_iban,
        rate_source=None,
        metadata=None,
    )


# ---------------------------------------------------------------------------
# §12 Metadata block invariant (cases 192–197d)
# ---------------------------------------------------------------------------


async def test_192_non_mcc4829_no_transfer_block(conn):
    """Case 192: non-MCC-4829 row → metadata.layer.transfer entirely absent."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        mcc=MCC_GROCERY,
    )

    result = await strategy.detect_and_pair(conn, tx)
    # Strategy returns None metadata_block for non-4829.
    assert result.metadata_block is None


async def test_193_mcc4829_unpaired_row_block_row_only_no_pair(conn):
    """Case 193: MCC 4829, no candidates, no transfer prefix → row block only."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        mcc=MCC_TRANSFER,
        desc="Олена К.",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.metadata_block is not None
    assert "row" in result.metadata_block
    assert result.metadata_block.get("pair") is None


async def test_194_mcc4829_paired_both_legs_have_row_and_identical_pair(conn):
    """Case 194: successful pair → both legs have row; pair blocks are byte-equal.

    Mirrors the orchestrator pattern (detect_and_pair → INSERT row with
    metadata.layer.transfer from result.metadata_block) so the partner row
    carries its own per-leg `row` block at INSERT time, before the incoming
    leg's claim_pair UPDATE adds the shared `pair` sub-block.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()

    # Process the partner (income leg) first — no pair yet, returns row-only block.
    partner_tx = _make_tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        cp_iban=UAH_FOP_IBAN,
        desc="З гривневого рахунку ФОП",
    )
    partner_result = await strategy.detect_and_pair(conn, partner_tx)
    assert partner_result.special_category is None
    assert partner_result.metadata_block is not None
    assert "row" in partner_result.metadata_block
    assert partner_result.metadata_block.get("pair") is None
    await insert_transaction(
        conn,
        id=partner_tx.id,
        user_id=user_id,
        account_id=black_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description=partner_tx.description,
        metadata={"layer": {"transfer": partner_result.metadata_block}},
    )

    # Process the incoming (expense leg) — pairs with partner, returns row+pair block.
    incoming_tx = _make_tx(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,
        desc="На чорну картку",
    )
    incoming_result = await strategy.detect_and_pair(conn, incoming_tx)
    assert incoming_result.special_category == "transfer"
    assert incoming_result.related_transaction_id == partner_tx.id
    assert incoming_result.metadata_block is not None
    assert "row" in incoming_result.metadata_block
    assert "pair" in incoming_result.metadata_block

    # Partner row's pair sub-block written via claim_pair UPDATE; row sub-block
    # preserved from the partner's own INSERT.
    partner_meta = await get_transfer_metadata(conn, partner_tx.id)
    assert partner_meta is not None
    assert "row" in partner_meta
    assert "pair" in partner_meta

    # pair blocks must be identical on both legs (json round-trip equality).
    incoming_pair = json.dumps(incoming_result.metadata_block["pair"], sort_keys=True)
    partner_pair = json.dumps(partner_meta["pair"], sort_keys=True)
    assert incoming_pair == partner_pair


async def test_195_anomaly_row_has_row_block_no_pair(conn):
    """Case 195: claim rejected by ambiguity → row block present; no pair sub-block."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    uah_black_iban2 = "UA444000000000000000000445"
    black2_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=uah_black_iban2
    )

    # Two ambiguous candidates: both null cp_iban, generic description
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",
    )
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black2_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category is None
    assert result.metadata_block is not None
    assert "row" in result.metadata_block
    assert result.metadata_block.get("pair") is None


async def test_196_unlinked_cp_iban_row_block_written_no_pair(conn):
    """Case 196: unlinked cp_iban short-circuit → status=unlinked; no pair block."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        cp_iban=EXTERNAL_IBAN,  # external — short-circuit fires
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category is None
    assert result.metadata_block is not None
    assert "row" in result.metadata_block
    assert result.metadata_block["row"]["cp_iban_status"] == "unlinked"
    assert result.metadata_block.get("pair") is None


async def test_197_income_expense_legs_have_different_row_content(conn):
    """Case 197: paired transfer legs have distinct row sub-blocks.

    Each leg gets its own `row` block at INSERT time (orchestrator-equivalent
    pattern: detect_and_pair → INSERT with metadata.layer.transfer.row from
    result.metadata_block). The two `row` sub-blocks must differ because each
    leg has its own description and cp_iban_status.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()

    # Partner (income leg) processed first — no pair yet, returns row-only block.
    partner_tx = _make_tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        cp_iban=UAH_FOP_IBAN,
        desc="З гривневого рахунку ФОП",
    )
    partner_result = await strategy.detect_and_pair(conn, partner_tx)
    assert partner_result.metadata_block is not None
    partner_row_block = partner_result.metadata_block["row"]
    await insert_transaction(
        conn,
        id=partner_tx.id,
        user_id=user_id,
        account_id=black_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description=partner_tx.description,
        metadata={"layer": {"transfer": {"row": partner_row_block}}},
    )

    # Incoming (expense leg) — pairs with partner. Crucially, the incoming
    # carries NO cp_iban — so its row will have cp_iban_status="null", while
    # the partner's row carries cp_iban_status="honest" (its IBAN pointed at
    # the incoming's account). The per-leg row blocks must reflect each leg's
    # own flags, so they differ on cp_iban_status.
    incoming_tx = _make_tx(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",
    )
    incoming_result = await strategy.detect_and_pair(conn, incoming_tx)
    assert incoming_result.special_category == "transfer"

    incoming_row = incoming_result.metadata_block["row"]
    partner_meta = await get_transfer_metadata(conn, partner_tx.id)
    partner_row = partner_meta["row"]

    # Row blocks record each leg's own flags — they differ.
    assert incoming_row != partner_row
    assert incoming_row["cp_iban_status"] == "null"
    assert partner_row["cp_iban_status"] == "honest"


# ---------------------------------------------------------------------------
# §12.2 Defensive jsonb_set tests (cases 197b–197d)
# ---------------------------------------------------------------------------


async def test_197b_partner_metadata_null_at_claim_time(conn):
    """Case 197b: partner metadata NULL at claim time → claim_pair writes pair block."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    # Partner inserted with explicit NULL metadata.
    partner_id = await conn.fetchval(
        """
        INSERT INTO transactions (
            id, source_id, user_id, account_id, time,
            amount_cents, operation_amount_cents,
            currency_code, operation_currency_code,
            mcc, cashback_amount_cents, hold, direction,
            source, origin, metadata
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $6,
            'UAH', 'UAH',
            '4829', 0, false, 'income',
            'monobank', 'bank', NULL
        )
        RETURNING id
        """,
        uuid4(),
        "src-partner",
        user_id,
        black_id,
        T + timedelta(seconds=1),
        5000,
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id

    partner_meta = await get_transfer_metadata(conn, partner_id)
    assert partner_meta is not None
    assert "pair" in partner_meta
    assert partner_meta["pair"]["iban_evidence"] in ("bilateral", "unilateral", "none")


async def test_197c_partner_metadata_empty_object_at_claim_time(conn):
    """Case 197c: partner metadata='{}' at claim → claim_pair creates layer.transfer."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    partner_id = await conn.fetchval(
        """
        INSERT INTO transactions (
            id, source_id, user_id, account_id, time,
            amount_cents, operation_amount_cents,
            currency_code, operation_currency_code,
            mcc, cashback_amount_cents, hold, direction,
            source, origin, metadata
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $6,
            'UAH', 'UAH',
            '4829', 0, false, 'income',
            'monobank', 'bank', '{}'::jsonb
        )
        RETURNING id
        """,
        uuid4(),
        "src-partner",
        user_id,
        black_id,
        T + timedelta(seconds=1),
        5000,
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category == "transfer"

    partner_meta = await get_transfer_metadata(conn, partner_id)
    assert partner_meta is not None
    assert "pair" in partner_meta


async def test_197d_partner_metadata_with_source_only_preserved_on_claim(conn):
    """Case 197d: partner has metadata.source but no layer key.

    claim_pair creates layer.transfer.pair WITHOUT clobbering metadata.source.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    pre_existing_source = {"bank_id": "mono_123", "hold": False}
    pre_existing_meta = json.dumps({"source": pre_existing_source})

    partner_id = await conn.fetchval(
        """
        INSERT INTO transactions (
            id, source_id, user_id, account_id, time,
            amount_cents, operation_amount_cents,
            currency_code, operation_currency_code,
            mcc, cashback_amount_cents, hold, direction,
            source, origin, metadata
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $6,
            'UAH', 'UAH',
            '4829', 0, false, 'income',
            'monobank', 'bank', $7::jsonb
        )
        RETURNING id
        """,
        uuid4(),
        "src-partner",
        user_id,
        black_id,
        T + timedelta(seconds=1),
        5000,
        pre_existing_meta,
    )

    strategy = _make_strategy()
    tx = _make_tx(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category == "transfer"

    # Verify pair block is present.
    partner_meta_row = await get_transfer_metadata(conn, partner_id)
    assert partner_meta_row is not None
    assert "pair" in partner_meta_row

    # Verify metadata.source is byte-identical to what was inserted.
    full_row = await conn.fetchrow(
        "SELECT metadata FROM transactions WHERE id = $1", partner_id
    )
    raw_meta = full_row["metadata"]
    stored_meta = json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
    assert stored_meta["source"] == pre_existing_source
