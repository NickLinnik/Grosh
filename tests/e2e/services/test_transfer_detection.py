"""E2E: transfer detection across the full webhook → Kafka → normalize → enrich chain.

These tests exercise integration paths that unit/integration suites can't:
- Kafka ordering and out-of-order arrival
- RLS + cross-user scoping in flight
- Two-stage consumer interaction (normalize → enrich)
- ISO 4217 numeric-to-alpha currency mapping end-to-end

Deep state-space coverage (every bucket transition, all anomaly kinds, race
conditions on the FOR UPDATE lock) stays in the enrichment service's
integration suite — running each variant through Kafka would be slow and
add no signal beyond what direct enrichment calls already prove.
"""

import asyncio
import secrets

import httpx
import pytest

from helpers.factories import (
    build_account,
    build_bank_integration,
    build_seed_admin,
    build_user,
)
from helpers.http import post_monobank_webhook
from helpers.wait import (
    wait_for_anomaly,
    wait_for_transaction,
    wait_for_transaction_count,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Case 1 — happy path, both legs arrive close together (original test).
# ---------------------------------------------------------------------------


async def test_two_webhooks_for_transfer_pair_are_linked(
    pg, client: httpx.AsyncClient
) -> None:
    """Two Monobank accounts in the same user, a transfer between them →
    both transactions land with related_transaction_id pointing at each other
    and special_category='transfer'.
    """
    uid, _, _ = await build_seed_admin(pg)
    iid, secret = await build_bank_integration(pg, user_id=uid)

    iban_a = "UA000000000000000000000000001"
    iban_b = "UA000000000000000000000000002"
    ext_a = f"acc-a-{secrets.token_hex(4)}"
    ext_b = f"acc-b-{secrets.token_hex(4)}"
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_a,
        iban=iban_a,
    )
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_b,
        iban=iban_b,
    )

    src_out = f"e2e-out-{secrets.token_hex(6)}"
    src_in = f"e2e-in-{secrets.token_hex(6)}"
    r1 = await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_a,
        source_id=src_out,
        amount=-50000,
        counter_iban=iban_b,
    )
    assert r1.status_code == 200
    r2 = await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_b,
        source_id=src_in,
        amount=50000,
        counter_iban=iban_a,
    )
    assert r2.status_code == 200

    tx_out = await wait_for_transaction(pg, source_id=src_out, timeout=20.0)
    tx_in = await wait_for_transaction(pg, source_id=src_in, timeout=20.0)

    assert tx_out is not None and tx_in is not None
    assert tx_out["special_category"] == "transfer"
    assert tx_in["special_category"] == "transfer"
    assert tx_out["related_transaction_id"] == tx_in["id"]
    assert tx_in["related_transaction_id"] == tx_out["id"]


# ---------------------------------------------------------------------------
# Case 2 — income webhook arrives BEFORE expense webhook.
# ---------------------------------------------------------------------------


async def test_out_of_order_arrival_still_links_pair(
    pg, client: httpx.AsyncClient
) -> None:
    """Income leg arrives first, expense leg arrives later.

    Exercises the late-arriving partner claim path: the first leg lands as
    plain (no candidate exists yet), the second leg picks it up via the
    universal-candidate fetch and claims it. After enrichment of leg 2,
    both rows must be linked.
    """
    uid, _, _ = await build_seed_admin(pg)
    iid, secret = await build_bank_integration(pg, user_id=uid)

    iban_a = "UA000000000000000000000000011"
    iban_b = "UA000000000000000000000000012"
    ext_a = f"acc-a-{secrets.token_hex(4)}"
    ext_b = f"acc-b-{secrets.token_hex(4)}"
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_a,
        iban=iban_a,
    )
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_b,
        iban=iban_b,
    )

    src_in = f"e2e-in-{secrets.token_hex(6)}"
    src_out = f"e2e-out-{secrets.token_hex(6)}"

    # Income leg first — no partner yet, must land as plain
    await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_b,
        source_id=src_in,
        amount=75000,
        counter_iban=iban_a,
    )
    tx_in_plain = await wait_for_transaction(pg, source_id=src_in, timeout=20.0)
    assert tx_in_plain is not None
    assert tx_in_plain["special_category"] is None
    assert tx_in_plain["related_transaction_id"] is None

    # Now the expense leg arrives — it must claim the income leg as its partner
    await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_a,
        source_id=src_out,
        amount=-75000,
        counter_iban=iban_b,
    )

    async def _both_linked() -> bool:
        out = await pg.fetchrow(
            "SELECT special_category, related_transaction_id FROM transactions "
            "WHERE source_id = $1",
            src_out,
        )
        income = await pg.fetchrow(
            "SELECT special_category, related_transaction_id FROM transactions "
            "WHERE source_id = $1",
            src_in,
        )
        return (
            out is not None
            and income is not None
            and out["special_category"] == "transfer"
            and income["special_category"] == "transfer"
            and out["related_transaction_id"] is not None
            and income["related_transaction_id"] is not None
        )

    # Use the polling helper via direct loop — the predicate returns bool
    deadline = 20.0
    interval = 0.2
    elapsed = 0.0
    while elapsed < deadline:
        if await _both_linked():
            break
        await asyncio.sleep(interval)
        elapsed += interval
    else:
        pytest.fail(f"Pair not linked within {deadline}s after second-leg arrival")

    tx_out = await pg.fetchrow(
        "SELECT id, special_category, related_transaction_id "
        "FROM transactions WHERE source_id = $1",
        src_out,
    )
    tx_in = await pg.fetchrow(
        "SELECT id, special_category, related_transaction_id "
        "FROM transactions WHERE source_id = $1",
        src_in,
    )
    assert tx_out["related_transaction_id"] == tx_in["id"]
    assert tx_in["related_transaction_id"] == tx_out["id"]


# ---------------------------------------------------------------------------
# Case 3 — unpaired expense with a transfer-flavored description.
# ---------------------------------------------------------------------------


async def test_unpaired_transfer_description_records_anomaly(
    pg, client: httpx.AsyncClient
) -> None:
    """Single MCC 4829 expense with a "На ..." description and no candidate
    partner → lands as plain transaction PLUS an `unpaired_to_description`
    row in transfer_match_anomalies.

    This exercises the count==0 anomaly branch (universal fetch returns
    nothing, description starts with "На ") and verifies the anomaly table
    is populated by the enrichment service. The counterIban is omitted so
    cp_iban_status=NULL — bypassing the unlinked short-circuit and routing
    the row through the universal fetch instead.
    """
    uid, _, _ = await build_seed_admin(pg)
    iid, secret = await build_bank_integration(pg, user_id=uid)

    ext_a = f"acc-{secrets.token_hex(4)}"
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_a,
        iban="UA000000000000000000000000021",
    )

    src = f"e2e-unpaired-{secrets.token_hex(6)}"
    r = await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_a,
        source_id=src,
        amount=-25000,
        description="На чорну картку",
    )
    assert r.status_code == 200

    tx = await wait_for_transaction(pg, source_id=src, timeout=20.0)
    assert tx is not None
    assert tx["special_category"] is None
    assert tx["related_transaction_id"] is None

    anomaly = await wait_for_anomaly(
        pg,
        transaction_id=tx["id"],
        timeout=10.0,
    )
    assert anomaly is not None
    assert anomaly["reason_code"] == "unpaired_to_description"


# ---------------------------------------------------------------------------
# Case 4 — cross-currency pair (USD account → UAH account).
# ---------------------------------------------------------------------------


async def test_cross_currency_pair_links_and_preserves_amounts(
    pg, client: httpx.AsyncClient
) -> None:
    """Transfer between a USD account and a UAH account.

    Monobank's webhook payload encodes the operation in the SOURCE account's
    currency and the booked amount in the holding account's currency. The
    normalizer takes abs() of both. After enrichment, both legs must:
    - Link to each other (transfer detection ignores currency)
    - Preserve their original amount_cents and operation_amount_cents
    - Carry the correct currency_code derived from the numeric ISO code
    """
    uid, _, _ = await build_seed_admin(pg)
    iid, secret = await build_bank_integration(pg, user_id=uid)

    iban_usd = "UA000000000000000000000000031"
    iban_uah = "UA000000000000000000000000032"
    ext_usd = f"acc-usd-{secrets.token_hex(4)}"
    ext_uah = f"acc-uah-{secrets.token_hex(4)}"
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="USD",
        external_id=ext_usd,
        iban=iban_usd,
    )
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_uah,
        iban=iban_uah,
    )

    src_out = f"e2e-usd-out-{secrets.token_hex(6)}"
    src_in = f"e2e-uah-in-{secrets.token_hex(6)}"

    # USD-side expense: amount in USD cents, operation in UAH (Monobank's "deal" currency)
    # 100 USD = 10000 cents; converted at 40 UAH/USD ≈ 400000 UAH cents
    await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_usd,
        source_id=src_out,
        amount=-10000,
        operation_amount=-400000,
        currency_code=980,  # operation currency = UAH
        counter_iban=iban_uah,
    )
    # UAH-side income: amount in UAH cents, same operation amount in UAH
    await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_uah,
        source_id=src_in,
        amount=400000,
        operation_amount=400000,
        currency_code=980,  # operation currency = UAH
        counter_iban=iban_usd,
    )

    tx_out = await wait_for_transaction(pg, source_id=src_out, timeout=20.0)
    tx_in = await wait_for_transaction(pg, source_id=src_in, timeout=20.0)
    assert tx_out is not None and tx_in is not None
    assert tx_out["special_category"] == "transfer"
    assert tx_in["special_category"] == "transfer"
    assert tx_out["related_transaction_id"] == tx_in["id"]
    assert tx_in["related_transaction_id"] == tx_out["id"]

    # Amount asymmetry preserved
    assert tx_out["amount_cents"] == 10000
    assert tx_out["currency_code"] == "USD"
    assert tx_out["operation_amount_cents"] == 400000
    assert tx_out["operation_currency_code"] == "UAH"

    assert tx_in["amount_cents"] == 400000
    assert tx_in["currency_code"] == "UAH"


# ---------------------------------------------------------------------------
# Case 5 — non-transfer MCC must NOT be classified as a transfer.
# ---------------------------------------------------------------------------


async def test_non_transfer_mcc_stays_plain(pg, client: httpx.AsyncClient) -> None:
    """A purchase at a grocery store (MCC 5411) with a counterIban must NOT
    be classified as a transfer, even if a candidate partner could in
    principle exist.

    Transfer detection only fires on MCC 4829 (the universal-fetch index
    has a `WHERE mcc = '4829'` filter). This test pins that contract end-
    to-end so a future refactor can't accidentally widen the trigger.
    """
    uid, _, _ = await build_seed_admin(pg)
    iid, secret = await build_bank_integration(pg, user_id=uid)

    iban_a = "UA000000000000000000000000041"
    iban_b = "UA000000000000000000000000042"
    ext_a = f"acc-a-{secrets.token_hex(4)}"
    ext_b = f"acc-b-{secrets.token_hex(4)}"
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_a,
        iban=iban_a,
    )
    await build_account(
        pg,
        user_id=uid,
        integration_id=iid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=ext_b,
        iban=iban_b,
    )

    src = f"e2e-grocery-{secrets.token_hex(6)}"
    await post_monobank_webhook(
        client,
        secret=secret,
        external_account=ext_a,
        source_id=src,
        amount=-15000,
        mcc=5411,  # grocery — not a transfer MCC
        counter_iban=iban_b,
        description="Сільпо",
    )

    tx = await wait_for_transaction(pg, source_id=src, timeout=20.0)
    assert tx is not None
    assert tx["mcc"] == "5411"
    assert tx["special_category"] is None
    assert tx["related_transaction_id"] is None


# ---------------------------------------------------------------------------
# Case 6 — cross-user isolation.
# ---------------------------------------------------------------------------


async def test_transfer_detection_isolates_users(pg, client: httpx.AsyncClient) -> None:
    """Two users each have a two-account pair with overlapping IBAN shapes.
    Each user's transfer must link only within its own user_id.

    This is critical because the universal-candidate fetch is per-user
    scoped (the partial index is `(user_id, direction, time DESC)`). A
    regression that drops the user_id predicate would surface here as
    cross-user mis-pairing.
    """
    uid_a, _, _ = await build_seed_admin(pg)
    uid_b, _, _ = await build_user(pg, email=f"u2-{secrets.token_hex(4)}@example.com")
    iid_a, secret_a = await build_bank_integration(pg, user_id=uid_a)
    iid_b, secret_b = await build_bank_integration(pg, user_id=uid_b)

    # Both users use the same logical IBAN pair shape. IBANs are unique
    # globally in the schema, so we vary the last digits per user.
    iban_a1 = "UA000000000000000000000000051"
    iban_a2 = "UA000000000000000000000000052"
    iban_b1 = "UA000000000000000000000000061"
    iban_b2 = "UA000000000000000000000000062"

    ext_a1 = f"u1-a-{secrets.token_hex(4)}"
    ext_a2 = f"u1-b-{secrets.token_hex(4)}"
    ext_b1 = f"u2-a-{secrets.token_hex(4)}"
    ext_b2 = f"u2-b-{secrets.token_hex(4)}"

    for uid, iid, ext, iban in [
        (uid_a, iid_a, ext_a1, iban_a1),
        (uid_a, iid_a, ext_a2, iban_a2),
        (uid_b, iid_b, ext_b1, iban_b1),
        (uid_b, iid_b, ext_b2, iban_b2),
    ]:
        await build_account(
            pg,
            user_id=uid,
            integration_id=iid,
            source="monobank",
            account_type="black",
            currency_code="UAH",
            external_id=ext,
            iban=iban,
        )

    # Fire all four webhooks "concurrently" — each user's pair must self-link
    src_a_out = f"u1-out-{secrets.token_hex(6)}"
    src_a_in = f"u1-in-{secrets.token_hex(6)}"
    src_b_out = f"u2-out-{secrets.token_hex(6)}"
    src_b_in = f"u2-in-{secrets.token_hex(6)}"

    await post_monobank_webhook(
        client,
        secret=secret_a,
        external_account=ext_a1,
        source_id=src_a_out,
        amount=-30000,
        counter_iban=iban_a2,
    )
    await post_monobank_webhook(
        client,
        secret=secret_a,
        external_account=ext_a2,
        source_id=src_a_in,
        amount=30000,
        counter_iban=iban_a1,
    )
    await post_monobank_webhook(
        client,
        secret=secret_b,
        external_account=ext_b1,
        source_id=src_b_out,
        amount=-30000,
        counter_iban=iban_b2,
    )
    await post_monobank_webhook(
        client,
        secret=secret_b,
        external_account=ext_b2,
        source_id=src_b_in,
        amount=30000,
        counter_iban=iban_b1,
    )

    # Each user should end with exactly 2 transactions
    await wait_for_transaction_count(pg, user_id=uid_a, expected=2, timeout=20.0)
    await wait_for_transaction_count(pg, user_id=uid_b, expected=2, timeout=20.0)

    rows_a = await pg.fetch(
        "SELECT id, source_id, user_id, special_category, related_transaction_id "
        "FROM transactions WHERE user_id = $1 ORDER BY source_id",
        uid_a,
    )
    rows_b = await pg.fetch(
        "SELECT id, source_id, user_id, special_category, related_transaction_id "
        "FROM transactions WHERE user_id = $1 ORDER BY source_id",
        uid_b,
    )

    # Both legs in user A's pair must reference each other, all within uid_a
    a_ids = {r["id"] for r in rows_a}
    for r in rows_a:
        assert r["special_category"] == "transfer"
        assert r["user_id"] == uid_a
        assert r["related_transaction_id"] in a_ids
        assert r["related_transaction_id"] not in {x["id"] for x in rows_b}

    # Same invariant for user B
    b_ids = {r["id"] for r in rows_b}
    for r in rows_b:
        assert r["special_category"] == "transfer"
        assert r["user_id"] == uid_b
        assert r["related_transaction_id"] in b_ids
        assert r["related_transaction_id"] not in a_ids
