"""E2E: POST /v1/manual/transactions → consumers → DB."""

import secrets

import httpx
import pytest

from helpers.factories import build_account, build_seed_admin
from helpers.http import INGESTION_BASE, bearer, log_in
from helpers.wait import wait_for_transaction

pytestmark = pytest.mark.asyncio


async def test_manual_transaction_flows_through_pipeline_to_db(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    account_id = await build_account(
        pg,
        user_id=uid,
        source="manual",
        account_type="cash",
        currency_code="USD",
        name="Pipeline Test",
    )
    token = await log_in(client, email, password)

    idem_key = f"e2e-{secrets.token_hex(8)}"
    r = await client.post(
        f"{INGESTION_BASE}/v1/manual/transactions",
        headers=bearer(token),
        json={
            "account_id": str(account_id),
            "amount_cents": 2500,
            "operation_currency_code": "USD",
            "description": "pipeline probe",
            "time": "2026-05-15T12:00:00Z",
            "direction": "income",
            "idempotency_key": idem_key,
        },
    )
    assert r.status_code == 201
    tx_id_returned = r.json()["id"]

    # Wait for consumer chain to land the row
    # Generous timeout: first message after container restart can take
    # 20-40s while consumers complete partition assignment + warm up.
    tx = await wait_for_transaction(pg, user_id=uid, timeout=60.0)
    assert tx is not None
    assert str(tx["id"]) == tx_id_returned
    assert tx["amount_cents"] == 2500
    assert tx["origin"] == "manual"
