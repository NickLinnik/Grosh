"""Integration tests for GET /v1/rates and GET /v1/rates/at."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
from httpx import AsyncClient

# ---------------------------------------------------------------------------
# Local helper — mirrors insert_rate_row from ingestion conftest
# ---------------------------------------------------------------------------


async def _insert_rate(
    conn: asyncpg.Connection,
    *,
    source: str,
    currency_from: str,
    currency_to: str,
    rate_mid: Decimal | str,
    valid_from: datetime,
    valid_to: datetime | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO currency_rates (
            source,
            currency_from,
            currency_to,
            rate_mid,
            valid_from,
            valid_to
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        source,
        currency_from,
        currency_to,
        Decimal(str(rate_mid)),
        valid_from,
        valid_to,
    )


# ---------------------------------------------------------------------------
# GET /v1/rates — listing, filters, cursor pagination, half-open boundary
# ---------------------------------------------------------------------------


async def test_list_rates_filters_and_pagination(
    client: AsyncClient,
    conn: asyncpg.Connection,
    admin_token: str,
) -> None:
    auth = {"Authorization": f"Bearer {admin_token}"}
    now = datetime.now(UTC).replace(microsecond=0)
    t_minus_3 = now - timedelta(days=3)
    t_minus_2 = now - timedelta(days=2)
    t_minus_1 = now - timedelta(days=1)

    # Seed 5 rows with varying source / currency pair / time spans
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="40.0",
        valid_from=t_minus_3,
        valid_to=t_minus_2,
    )
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="40.5",
        valid_from=t_minus_2,
        valid_to=t_minus_1,
    )
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="41.0",
        valid_from=t_minus_1,
        valid_to=None,
    )
    await _insert_rate(
        conn,
        source="monobank",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="41.2",
        valid_from=t_minus_1,
        valid_to=None,
    )
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="EUR",
        currency_to="UAH",
        rate_mid="44.0",
        valid_from=t_minus_1,
        valid_to=None,
    )

    # (a) Filter by source + pair → 3 rows, sorted by valid_from DESC, total=3
    resp = await client.get(
        "/v1/rates",
        params={
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
        },
        headers=auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert len(body["items"]) == 3
    assert body["next_cursor"] is None
    # Sorted descending by valid_from — newest first
    valid_froms = [item["valid_from"] for item in body["items"]]
    assert valid_froms == sorted(valid_froms, reverse=True)

    # (b) Paginate with limit=2: first page has 2 items + next_cursor
    resp = await client.get(
        "/v1/rates",
        params={
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
            "limit": 2,
        },
        headers=auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 2
    assert body["next_cursor"] is not None
    next_cursor = body["next_cursor"]

    # Second page returns the remaining 1 item
    resp2 = await client.get(
        "/v1/rates",
        params={
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
            "limit": 2,
            "cursor": next_cursor,
        },
        headers=auth,
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert len(body2["items"]) == 1
    assert body2["next_cursor"] is None
    # Page 2 must be the oldest rate (rate_mid=40.0 at t-3d) —
    # catches reversed pagination
    assert Decimal(body2["items"][0]["rate_mid"]) == Decimal("40.0")
    # Pages must not overlap
    ids_page1 = {item["id"] for item in body["items"]}
    ids_page2 = {item["id"] for item in body2["items"]}
    assert ids_page1.isdisjoint(ids_page2)

    # (c) Garbage cursor → 400 INVALID_CURSOR
    resp = await client.get(
        "/v1/rates",
        params={"cursor": "garbage"},
        headers=auth,
    )
    assert resp.status_code == 400
    assert resp.json()["code"] == "INVALID_CURSOR"

    # (d) Half-open [from, to) filter on valid_from — only rows where
    #     valid_from >= t_minus_1 AND valid_from < now
    #     (3 rows total: 2 nbu + 1 monobank)
    resp = await client.get(
        "/v1/rates",
        params={
            "from": t_minus_1.isoformat(),
            "to": now.isoformat(),
        },
        headers=auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    # Every returned row must have valid_from in [t_minus_1, now)
    for item in body["items"]:
        vf = datetime.fromisoformat(item["valid_from"])
        assert vf >= t_minus_1
        assert vf < now


# ---------------------------------------------------------------------------
# GET /v1/rates/at — SCD2 point-in-time lookup and half-open valid_to boundary
# ---------------------------------------------------------------------------


async def test_get_rates_at_scd2_boundaries(
    client: AsyncClient,
    conn: asyncpg.Connection,
    admin_token: str,
) -> None:
    auth = {"Authorization": f"Bearer {admin_token}"}
    now = datetime.now(UTC).replace(microsecond=0)
    t_minus_3 = now - timedelta(days=3)
    t_minus_2 = now - timedelta(days=2)
    t_minus_1 = now - timedelta(days=1)
    # A point 1.5 days in the past, squarely in the middle row
    t_minus_1_5 = now - timedelta(hours=36)

    # Seed 3 SCD2 rows for (nbu, USD, UAH)
    # Row 1: [t-3, t-2)
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="40.0",
        valid_from=t_minus_3,
        valid_to=t_minus_2,
    )
    # Row 2: [t-2, t-1)
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="40.5",
        valid_from=t_minus_2,
        valid_to=t_minus_1,
    )
    # Row 3: [t-1, open)
    await _insert_rate(
        conn,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid="41.0",
        valid_from=t_minus_1,
        valid_to=None,
    )

    # (a) Point in the middle of row 2 → only row 2 matches
    resp = await client.get(
        "/v1/rates/at",
        params={
            "at": t_minus_1_5.isoformat(),
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
        },
        headers=auth,
    )
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert Decimal(items[0]["rate_mid"]) == Decimal("40.5")

    # (b) Point at `now` → only the open-ended row 3 matches
    resp = await client.get(
        "/v1/rates/at",
        params={
            "at": now.isoformat(),
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
        },
        headers=auth,
    )
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert Decimal(items[0]["rate_mid"]) == Decimal("41.0")
    assert items[0]["valid_to"] is None

    # (c) Point exactly at t_minus_2 (= valid_from of row 2 = valid_to of row 1)
    #     Row 1 does NOT match because valid_to > at requires strictly greater.
    #     Row 2 DOES match because valid_from <= at is satisfied.
    resp = await client.get(
        "/v1/rates/at",
        params={
            "at": t_minus_2.isoformat(),
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
        },
        headers=auth,
    )
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert Decimal(items[0]["rate_mid"]) == Decimal("40.5")

    # (d) Four-filter combination at `now` still returns the right row
    resp = await client.get(
        "/v1/rates/at",
        params={
            "at": now.isoformat(),
            "source": "nbu",
            "currency_from": "USD",
            "currency_to": "UAH",
        },
        headers=auth,
    )
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["source"] == "nbu"
    assert items[0]["currency_from"] == "USD"
    assert items[0]["currency_to"] == "UAH"
    assert Decimal(items[0]["rate_mid"]) == Decimal("41.0")
