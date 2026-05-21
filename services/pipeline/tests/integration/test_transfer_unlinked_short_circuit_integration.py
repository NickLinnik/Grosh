"""Integration tests for unlinked-partner IBAN short-circuit (cp_iban_status=unlinked).

Test cases per `references/consumer-transfer-detection-test-suite.md` §14.
Skipped at module level until detector.py lands (Slice 17 task T13).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tests.integration.conftest import (
    insert_account,
    insert_user,
)

pytestmark = pytest.mark.asyncio


try:
    from grosh_shared.normalized import NormalizedTransaction

    from grosh_pipeline.repositories.account_repo import AccountRepo
    from grosh_pipeline.repositories.anomaly_repo import AnomalyRepo
    from grosh_pipeline.sources.monobank.transfer.detector import (
        MonobankTransferDetection,
    )
    from grosh_pipeline.sources.monobank.transfer.repo import TransferQueryRepo
except ImportError:
    pass  # tests are skipped at module level via pytestmark

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"

UAH_BLACK_IBAN = "UA444000000000000000000444"
EXTERNAL_IBAN = "UA999000000000000000000999"


def _make_strategy():
    return MonobankTransferDetection(
        TransferQueryRepo(),
        AccountRepo(),
        AnomalyRepo(),
    )


def _tx(*, user_id, account_id, direction, cp_iban=None, desc=None):
    return NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=account_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=desc,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction=direction,
        counterparty_iban=cp_iban,
        rate_source=None,
        metadata=None,
    )


# ---------------------------------------------------------------------------
# §14 External short-circuit cases (205–208)
# ---------------------------------------------------------------------------


async def test_205_unlinked_known_description_no_anomaly_row_written(conn):
    """Case 205: unlinked cp_iban + known description → no anomaly; status=unlinked."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    tx = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        cp_iban=EXTERNAL_IBAN,
        desc="На білу картку",  # known description → opts out of anomaly
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category is None
    assert result.related_transaction_id is None
    # No anomaly: user implicitly opted out by not linking destination account.
    assert result.anomalies == []
    # Row block written with unlinked cp_iban_status.
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "unlinked"
    assert result.metadata_block.get("pair") is None


async def test_206_unlinked_prefix_but_unknown_description_vocabulary_drift_anomaly(
    conn,
):
    """Case 206: unlinked cp_iban + unknown prefix description → unpaired_from_desc."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    tx = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        cp_iban=EXTERNAL_IBAN,
        desc="З нового банку",  # "З " prefix but not in known set → vocabulary drift
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category is None
    # Vocabulary-drift signal: anomaly IS recorded despite unlinked status.
    assert any(
        a.reason_code in ("unpaired_from_description", "unpaired_to_description")
        for a in result.anomalies
    )


async def test_207_unlinked_no_transfer_prefix_no_anomaly(conn):
    """Case 207: unlinked cp_iban, no transfer prefix → no anomaly; row block set."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    tx = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        cp_iban=EXTERNAL_IBAN,
        desc="Олена К.",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.special_category is None
    assert result.anomalies == []
    # Row block still written for traceability.
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "unlinked"


async def test_208_multi_hop_desc_with_unlinked_iban_transitive_wins(conn):
    """Case 208: multi-hop desc + unlinked cp_iban → transitive rule fires first.

    The directional transitive rule (direction=expense AND multi_hop_description)
    runs before the IBAN lookup. So cp_iban_status = 'transitive', NOT 'unlinked'.
    The row does NOT short-circuit at §3a; it proceeds to the universal fetch.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    tx = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        # EXTERNAL_IBAN would be 'unlinked' if looked up; multi-hop rule fires first.
        cp_iban=EXTERNAL_IBAN,
        desc="На гривневий рахунок ФОП для переказу на картку",  # multi-hop description
    )

    result = await strategy.detect_and_pair(conn, tx)
    # Transitive rule suppresses the IBAN; status must be 'transitive', not 'unlinked'.
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "transitive"
