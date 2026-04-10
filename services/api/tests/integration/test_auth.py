import os

from httpx import ASGITransport, AsyncClient

from grosh_api.main import app

# Throwaway password used by integration tests for ephemeral users. Not a
# real credential — all test data is rolled back at the end of each session.
_TEST_PASSWORD = "test-only-not-a-secret"  # noqa: S105

# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------


async def test_login_happy_path(client: AsyncClient) -> None:
    resp = await client.post(
        "/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"
    assert "refresh_token" in resp.cookies


async def test_login_wrong_password(client: AsyncClient) -> None:
    resp = await client.post(
        "/auth/login",
        json={"email": os.environ["ADMIN_EMAIL"], "password": "wrong-password"},
    )

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid email or password."


# ---------------------------------------------------------------------------
# /auth/me
# ---------------------------------------------------------------------------


async def test_me_with_valid_token(client: AsyncClient, admin_token: str) -> None:
    resp = await client.get(
        "/auth/me", headers={"Authorization": f"Bearer {admin_token}"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == os.environ["ADMIN_EMAIL"]
    assert body["role"] == "admin"


async def test_me_with_no_token(client: AsyncClient) -> None:
    resp = await client.get("/auth/me")

    assert resp.status_code == 401


async def test_me_with_tampered_token(client: AsyncClient, admin_token: str) -> None:
    # Replace the entire signature portion to guarantee invalidation
    header_payload, _, _ = admin_token.rsplit(".", 2)
    tampered = header_payload + ".invalidsignature"

    resp = await client.get("/auth/me", headers={"Authorization": f"Bearer {tampered}"})

    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# /auth/refresh
# ---------------------------------------------------------------------------


async def test_refresh_with_valid_cookie(client: AsyncClient) -> None:
    login_resp = await client.post(
        "/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )
    assert login_resp.status_code == 200
    original_refresh_cookie = login_resp.cookies.get("refresh_token")

    refresh_resp = await client.post("/auth/refresh")

    assert refresh_resp.status_code == 200
    body = refresh_resp.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"
    # The refresh cookie must be rotated to a new value
    new_refresh_cookie = refresh_resp.cookies.get("refresh_token")
    assert new_refresh_cookie is not None
    assert new_refresh_cookie != original_refresh_cookie


async def test_refresh_with_no_cookie(client: AsyncClient) -> None:
    resp = await client.post("/auth/refresh")

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Session expired. Please log in again."


# ---------------------------------------------------------------------------
# /auth/logout
# ---------------------------------------------------------------------------


async def test_logout_then_refresh_is_401(client: AsyncClient) -> None:
    await client.post(
        "/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )

    logout_resp = await client.post("/auth/logout")
    assert logout_resp.status_code == 204

    refresh_resp = await client.post("/auth/refresh")
    assert refresh_resp.status_code == 401


# ---------------------------------------------------------------------------
# /auth/logout-all
# ---------------------------------------------------------------------------


async def test_logout_all_revokes_every_session(client: AsyncClient) -> None:
    """Two simulated devices both lose access after a single /auth/logout-all."""
    creds = {
        "email": os.environ["ADMIN_EMAIL"],
        "password": os.environ["ADMIN_PASSWORD"],
    }

    # Device A: the existing fixture client
    login_a = await client.post("/auth/login", json=creds)
    assert login_a.status_code == 200
    token_a = login_a.json()["access_token"]

    # Device B: a fresh client with its own cookie jar, sharing the same app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://test"
    ) as device_b:
        login_b = await device_b.post("/auth/login", json=creds)
        assert login_b.status_code == 200

        # Device A revokes everything
        logout_all = await client.post(
            "/auth/logout-all",
            headers={"Authorization": f"Bearer {token_a}"},
        )
        assert logout_all.status_code == 204

        # Device A's refresh token is gone
        assert (await client.post("/auth/refresh")).status_code == 401
        # Device B's refresh token is also gone
        assert (await device_b.post("/auth/refresh")).status_code == 401


async def test_logout_all_requires_auth(client: AsyncClient) -> None:
    resp = await client.post("/auth/logout-all")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# /admin/users — create
# ---------------------------------------------------------------------------


async def test_admin_create_user(client: AsyncClient, admin_token: str) -> None:
    resp = await client.post(
        "/admin/users",
        json={
            "email": "newmember@example.com",
            "password": _TEST_PASSWORD,
            "display_name": "New Member",
            "role": "member",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 201
    body = resp.json()
    assert body["email"] == "newmember@example.com"
    assert body["role"] == "member"
    assert "id" in body


async def test_admin_create_user_duplicate_email(
    client: AsyncClient, admin_token: str
) -> None:
    payload = {
        "email": "duplicate@example.com",
        "password": _TEST_PASSWORD,
        "display_name": "First",
        "role": "member",
    }
    first = await client.post(
        "/admin/users",
        json=payload,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert first.status_code == 201

    second = await client.post(
        "/admin/users",
        json=payload,
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert second.status_code == 409
    assert "already exists" in second.json()["detail"]


# ---------------------------------------------------------------------------
# /admin/users — delete
# ---------------------------------------------------------------------------


async def test_admin_delete_user_soft_deletes(
    client: AsyncClient, admin_token: str
) -> None:
    create_resp = await client.post(
        "/admin/users",
        json={
            "email": "todelete@example.com",
            "password": _TEST_PASSWORD,
            "display_name": "To Delete",
            "role": "member",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert create_resp.status_code == 201
    user_id = create_resp.json()["id"]

    delete_resp = await client.delete(
        f"/admin/users/{user_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert delete_resp.status_code == 204

    login_resp = await client.post(
        "/auth/login",
        json={"email": "todelete@example.com", "password": _TEST_PASSWORD},
    )
    assert login_resp.status_code == 401


async def test_admin_self_delete_is_400(client: AsyncClient, admin_token: str) -> None:
    me_resp = await client.get(
        "/auth/me", headers={"Authorization": f"Bearer {admin_token}"}
    )
    assert me_resp.status_code == 200
    admin_id = me_resp.json()["id"]

    resp = await client.delete(
        f"/admin/users/{admin_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Cannot delete your own account."


# ---------------------------------------------------------------------------
# Admin-only enforcement
# ---------------------------------------------------------------------------


async def test_member_cannot_call_admin_endpoints(
    client: AsyncClient, admin_token: str
) -> None:
    create_resp = await client.post(
        "/admin/users",
        json={
            "email": "member@example.com",
            "password": _TEST_PASSWORD,
            "display_name": "Plain Member",
            "role": "member",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert create_resp.status_code == 201

    login_resp = await client.post(
        "/auth/login",
        json={"email": "member@example.com", "password": _TEST_PASSWORD},
    )
    assert login_resp.status_code == 200
    member_token = login_resp.json()["access_token"]

    resp = await client.post(
        "/admin/users",
        json={
            "email": "another@example.com",
            "password": _TEST_PASSWORD,
            "display_name": "Another",
            "role": "member",
        },
        headers={"Authorization": f"Bearer {member_token}"},
    )

    assert resp.status_code == 403
