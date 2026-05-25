"""HTTP helpers — login, authenticated requests, common URL prefixes."""

from __future__ import annotations

import time

import httpx

# Test-stack ports (see infra/docker-compose.test-stack.yml). The dev stack
# uses 8000/8001; the test stack uses 8010/8011 so both can run in parallel.
API_BASE = "http://localhost:8010"
INGESTION_BASE = "http://localhost:8011"


async def log_in(client: httpx.AsyncClient, email: str, password: str) -> str:
    """POST /v1/auth/login and return the access token. Raises on non-200."""
    r = await client.post(
        f"{API_BASE}/v1/auth/login",
        json={"email": email, "password": password},
    )
    r.raise_for_status()
    return r.json()["access_token"]


def bearer(token: str) -> dict[str, str]:
    """Build an Authorization header dict."""
    return {"Authorization": f"Bearer {token}"}


async def post_monobank_webhook(
    client: httpx.AsyncClient,
    *,
    secret: str,
    external_account: str,
    source_id: str,
    amount: int,
    operation_amount: int | None = None,
    currency_code: int = 980,
    mcc: int = 4829,
    description: str = "",
    counter_iban: str | None = None,
    balance: int = 1_000_000,
) -> httpx.Response:
    """POST a Monobank-shaped StatementItem to the ingestion webhook.

    `amount` is signed (negative = outflow), matching Monobank's payload
    contract. The normalizer takes abs() and infers direction from sign.
    `currency_code` is the ISO 4217 NUMERIC code (980=UAH, 840=USD, 978=EUR).
    """
    item: dict = {
        "id": source_id,
        "time": int(time.time()),
        "description": description or f"e2e {source_id}",
        "mcc": mcc,
        "originalMcc": mcc,
        "amount": amount,
        "operationAmount": operation_amount if operation_amount is not None else amount,
        "currencyCode": currency_code,
        "commissionRate": 0,
        "cashbackAmount": 0,
        "balance": balance,
        "hold": False,
    }
    if counter_iban is not None:
        item["counterIban"] = counter_iban
    payload = {
        "type": "StatementItem",
        "data": {"account": external_account, "statementItem": item},
    }
    return await client.post(
        f"{INGESTION_BASE}/monobank/webhook/{secret}", json=payload
    )
