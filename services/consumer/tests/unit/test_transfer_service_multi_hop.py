"""Unit tests — multi-hop chain scenarios — §9 (tests 101-105)."""

import pytest

from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.unit.transfer_fixtures import (
    MCC_TRANSFER,
    T_PLUS_1S,
    UAH_BLACK_IBAN,
    UAH_FOP_IBAN,
    USD_FOP_IBAN,
    USER_ID,
    T,
    seed_accounts,
)

pytestmark = pytest.mark.asyncio


def make_service(tx_repo, acc_repo, anomaly_repo):
    return MonobankTransferDetection(
        transaction_repo=tx_repo,
        account_repo=acc_repo,
        anomaly_repo=anomaly_repo,
    )


# ---------------------------------------------------------------------------
# §9.1  USD FOP → UAH FOP → UAH card  (4 legs, 2 pairs)
# ---------------------------------------------------------------------------


async def test_101_four_legs_arrive_in_order_two_independent_pairs(
    tx_repo, acc_repo, anomaly_repo
):
    """Full multi-hop: USD FOP → UAH FOP → UAH card, legs arrive in order 1→2→3→4."""
    accounts = seed_accounts(acc_repo)
    svc = make_service(tx_repo, acc_repo, anomaly_repo)

    # Leg 1: USD FOP expense → UAH FOP (Tier A)
    leg1 = make_event(
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_FOP_IBAN,
        description="На гривневий рахунок ФОП для переказу на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    r1 = await svc.detect_and_pair(None, leg1)
    # No partner yet — inserts as expense
    assert r1.special_category is None
    assert r1.related_transaction_id is None

    # Leg 1 goes into store
    tx_repo.add_transaction(
        id=leg1.id,
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_FOP_IBAN,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    # Leg 2: UAH FOP income from USD FOP (Tier A — finds leg1)
    leg2 = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=USD_FOP_IBAN,
        description="З доларового рахунку ФОП для переказу на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    r2 = await svc.detect_and_pair(None, leg2)
    assert r2.special_category == "transfer"
    assert r2.related_transaction_id == leg1.id
    # Leg1 in store is now claimed
    assert tx_repo.get_by_id(leg1.id)["special_category"] == "transfer"

    # Leg 2 goes into store as transfer
    tx_repo.add_transaction(
        id=leg2.id,
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        special_category="transfer",
        mcc=MCC_TRANSFER,
        counterparty_iban=USD_FOP_IBAN,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        related_transaction_id=leg1.id,
    )

    # Leg 3: UAH FOP expense → UAH black card (Tier A)
    leg3 = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    r3 = await svc.detect_and_pair(None, leg3)
    # No partner yet on black card
    assert r3.special_category is None
    assert r3.related_transaction_id is None

    tx_repo.add_transaction(
        id=leg3.id,
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    # Leg 4: UAH black card income, no IBAN
    # (Tier B — finds leg3 which has counterparty_iban=UAH_BLACK_IBAN)
    leg4 = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    r4 = await svc.detect_and_pair(None, leg4)
    assert r4.special_category == "transfer"
    assert r4.related_transaction_id == leg3.id

    # Verify legs 3 and 4 are now a distinct pair from legs 1 and 2
    assert tx_repo.get_by_id(leg3.id)["special_category"] == "transfer"
    assert tx_repo.get_by_id(leg3.id)["related_transaction_id"] == leg4.id


async def test_102_intermediate_fop_legs_on_same_account_dont_cross_pair(
    tx_repo, acc_repo, anomaly_repo
):
    """UAH FOP income and expense at same timestamp — same account_id
    excludes Tier C cross-pair."""
    accounts = seed_accounts(acc_repo)
    svc = make_service(tx_repo, acc_repo, anomaly_repo)

    # UAH FOP income already stored (leg arriving from USD FOP)
    fop_income = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=USD_FOP_IBAN,
    )

    # UAH FOP expense (leg going to black card) — same account, same timestamp
    leg_expense = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await svc.detect_and_pair(None, leg_expense)

    # Tier A finds the black card account via IBAN — not the FOP income.
    # No partner on black card yet → inserts as expense.
    assert result.special_category is None
    # The FOP income was NOT claimed by the FOP expense
    assert tx_repo.get_by_id(fop_income["id"])["related_transaction_id"] is None


async def test_103_leg4_arrives_before_leg3_unpaired_then_leg3_pairs(
    tx_repo, acc_repo, anomaly_repo
):
    """Leg 4 (card income, no IBAN) arrives first → unpaired anomaly;
    leg 3 (FOP expense) arrives and pairs via Tier A."""
    accounts = seed_accounts(acc_repo)
    svc = make_service(tx_repo, acc_repo, anomaly_repo)

    # Leg 4 arrives first: card income, desc "З гривневого рахунку ФОП", no partner
    leg4 = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    r4 = await svc.detect_and_pair(None, leg4)
    # No partner yet → unpaired_from_description
    assert r4.special_category is None
    assert len(r4.anomalies) == 1
    assert r4.anomalies[0].reason_code == "unpaired_from_description"

    # Persist anomaly and leg4 in store
    await anomaly_repo.record_anomaly(
        None,
        leg4.id,
        [],
        "unpaired_from_description",
        None,
    )
    tx_repo.add_transaction(
        id=leg4.id,
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        description="З гривневого рахунку ФОП",
    )

    # Leg 3 arrives: FOP expense with counterparty_iban = UAH_BLACK_IBAN
    # → Tier A finds leg4
    leg3 = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    r3 = await svc.detect_and_pair(None, leg3)

    assert r3.special_category == "transfer"
    assert r3.related_transaction_id == leg4.id

    # Leg4's unpaired anomaly should be auto-deleted after claim
    remaining = anomaly_repo.all_anomalies()
    assert not any(a["transaction_id"] == leg4.id for a in remaining)


# ---------------------------------------------------------------------------
# §9.2  Direct FOP → card  (2 legs, asymmetric tiers)
# ---------------------------------------------------------------------------


async def test_104_fop_expense_finds_card_income_via_tier_a(
    tx_repo, acc_repo, anomaly_repo
):
    """FOP expense with IBAN; card income already stored.
    FOP arrives second → Tier A pairs."""
    accounts = seed_accounts(acc_repo)
    svc = make_service(tx_repo, acc_repo, anomaly_repo)

    # Card income stored first (no IBAN)
    card_income = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
    )

    # FOP expense arrives second with counterparty_iban → Tier A finds card income
    leg_expense = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await svc.detect_and_pair(None, leg_expense)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == card_income["id"]


# ---------------------------------------------------------------------------
# §9.3  FOP → FOP  (both legs have counterparty_iban, both Tier A capable)
# ---------------------------------------------------------------------------


async def test_105_both_fop_legs_have_iban_second_arrival_pairs(
    tx_repo, acc_repo, anomaly_repo
):
    """USD FOP expense → UAH FOP income. Second arrival uses Tier A to find first."""
    accounts = seed_accounts(acc_repo)
    svc = make_service(tx_repo, acc_repo, anomaly_repo)

    # First leg: USD FOP expense. No partner yet.
    leg1 = make_event(
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_FOP_IBAN,
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    r1 = await svc.detect_and_pair(None, leg1)
    assert r1.special_category is None

    tx_repo.add_transaction(
        id=leg1.id,
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_FOP_IBAN,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    # Second leg: UAH FOP income, counterparty_iban = USD_FOP_IBAN → Tier A finds leg1
    leg2 = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=USD_FOP_IBAN,
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    r2 = await svc.detect_and_pair(None, leg2)

    assert r2.special_category == "transfer"
    assert r2.related_transaction_id == leg1.id
    assert tx_repo.get_by_id(leg1.id)["special_category"] == "transfer"
