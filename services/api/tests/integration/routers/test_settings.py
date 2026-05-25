"""Integration tests for GET /v1/settings and PUT /v1/settings."""

import asyncpg
from httpx import AsyncClient

_TEST_PASSWORD = "test-only-not-a-secret"  # noqa: S105


async def _create_member(client: AsyncClient, admin_token: str, email: str) -> str:
    resp = await client.post(
        "/v1/admin/users",
        json={
            "email": email,
            "password": _TEST_PASSWORD,
            "display_name": email.split("@")[0],
            "role": "member",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _login(client: AsyncClient, email: str) -> str:
    resp = await client.post(
        "/v1/auth/login",
        json={"email": email, "password": _TEST_PASSWORD},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# GET /v1/settings — happy path + per-user isolation
# ---------------------------------------------------------------------------


async def test_get_settings_per_user_isolation(
    client: AsyncClient,
    conn: asyncpg.Connection,
    admin_token: str,
) -> None:
    user_a_id = await _create_member(client, admin_token, "settings-a@example.com")
    user_b_id = await _create_member(client, admin_token, "settings-b@example.com")

    # (a) user_a has a default settings row (auto-created by the users trigger).
    #     GET returns default values: default_rate_source=None, timezone='UTC'.
    token_a = await _login(client, "settings-a@example.com")
    resp = await client.get(
        "/v1/settings", headers={"Authorization": f"Bearer {token_a}"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["default_rate_source"] is None
    assert body["timezone"] == "UTC"
    # updated_at is always set (NOT NULL DEFAULT now()) — just confirm it's present
    assert body["updated_at"] is not None

    # (b) Update user_b's settings row directly in the DB (row already exists due
    #     to the create_user_settings trigger; use UPDATE not INSERT).
    await conn.execute(
        """
        UPDATE user_settings
        SET default_rate_source = 'nbu', timezone = 'Europe/Kyiv'
        WHERE user_id = $1
        """,
        user_b_id,
    )

    # (c) user_a still sees their own defaults — the WHERE user_id = $1 scope holds
    resp = await client.get(
        "/v1/settings", headers={"Authorization": f"Bearer {token_a}"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["default_rate_source"] is None
    assert body["timezone"] == "UTC"

    # (d) user_b sees their updated row
    token_b = await _login(client, "settings-b@example.com")
    resp = await client.get(
        "/v1/settings", headers={"Authorization": f"Bearer {token_b}"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["default_rate_source"] == "nbu"
    assert body["timezone"] == "Europe/Kyiv"
    assert body["updated_at"] is not None

    # Suppress unused-variable warnings — the IDs are used implicitly via the DB
    _ = user_a_id


# ---------------------------------------------------------------------------
# PUT /v1/settings — happy path + validation 422s
# ---------------------------------------------------------------------------


async def test_put_settings_happy_path_and_validation_422s(
    client: AsyncClient,
    admin_token: str,
) -> None:
    await _create_member(client, admin_token, "settings-put@example.com")
    token = await _login(client, "settings-put@example.com")
    auth = {"Authorization": f"Bearer {token}"}

    # (a) Valid update — nbu is seeded by migration 0005_rate_source_config
    resp = await client.put(
        "/v1/settings",
        json={"default_rate_source": "nbu", "timezone": "Europe/Kyiv"},
        headers=auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["default_rate_source"] == "nbu"
    assert body["timezone"] == "Europe/Kyiv"

    # (b) Unknown rate source → 422 with full RFC 7807 envelope
    resp = await client.put(
        "/v1/settings",
        json={"default_rate_source": "fake-source"},
        headers=auth,
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert "fake-source" in body["detail"]
    # RFC 7807 envelope completeness — type, title, status must all be present
    assert "type" in body
    assert "title" in body
    assert body["status"] == 422

    # (c) Invalid timezone → 422 with full RFC 7807 envelope
    resp = await client.put(
        "/v1/settings",
        json={"timezone": "Mars/Phobos"},
        headers=auth,
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert "Mars/Phobos" in body["detail"]
    assert "type" in body
    assert "title" in body
    assert body["status"] == 422

    # (d) GET after the successful PUT confirms the upsert persisted
    resp = await client.get("/v1/settings", headers=auth)
    assert resp.status_code == 200
    body = resp.json()
    assert body["default_rate_source"] == "nbu"
    assert body["timezone"] == "Europe/Kyiv"
    assert body["updated_at"] is not None
