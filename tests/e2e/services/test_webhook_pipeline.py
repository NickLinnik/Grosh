"""E2E: Monobank webhook → normalization → enrichment → DB."""

import secrets
import time

import httpx
import pytest

from helpers.factories import build_account, build_bank_integration, build_seed_admin
from helpers.http import INGESTION_BASE
from helpers.wait import wait_for_transaction

pytestmark = pytest.mark.asyncio


async def test_webhook_event_lands_in_transactions_table(
    pg, client: httpx.AsyncClient
) -> None:
    """D-003: webhook POST must publish to Kafka, consumer chain must land
    the transaction in the transactions table.

    Currently webhook returns 404 because RLS blocks the integration lookup.
    Once fixed, this test pins the full pipeline.
    """
    uid, _, _ = await build_seed_admin(pg)
    integration_id, secret = await build_bank_integration(pg, user_id=uid)
    external_id = f"acc-{secrets.token_hex(4)}"
    await build_account(
        pg,
        user_id=uid,
        integration_id=integration_id,
        source="monobank",
        account_type="black",
        currency_code="UAH",
        external_id=external_id,
    )

    source_id = f"e2e-webhook-{secrets.token_hex(8)}"
    payload = {
        "type": "StatementItem",
        "data": {
            "account": external_id,
            "statementItem": {
                "id": source_id,
                "time": int(time.time()),
                "description": "e2e webhook probe",
                "mcc": 5411,
                "originalMcc": 5411,
                "amount": -100,
                "operationAmount": -100,
                "currencyCode": 980,
                "commissionRate": 0,
                "cashbackAmount": 0,
                "balance": 1000000,
                "hold": False,
            },
        },
    }
    r = await client.post(f"{INGESTION_BASE}/monobank/webhook/{secret}", json=payload)
    assert r.status_code == 200, f"webhook returned {r.status_code}: {r.text[:300]}"

    # Wait for the consumer chain (normalization → enrichment) to land the tx.
    tx = await wait_for_transaction(pg, source_id=source_id, timeout=15.0)
    assert tx is not None
    assert tx["user_id"] == uid
    # Monobank's amount field is signed (negative = outflow); the normalizer
    # stores absolute amount_cents and encodes the sign in `direction`.
    assert tx["amount_cents"] == 100
    assert tx["direction"] == "expense"
    assert tx["currency_code"] == "UAH"
