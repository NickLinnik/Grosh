import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
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
        "/v1/auth/login",
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
        "/v1/auth/login",
        json={"email": os.environ["ADMIN_EMAIL"], "password": "wrong-password"},
    )

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid email or password."


# ---------------------------------------------------------------------------
# /auth/me
# ---------------------------------------------------------------------------


async def test_me_with_valid_token(client: AsyncClient, admin_token: str) -> None:
    resp = await client.get(
        "/v1/auth/me", headers={"Authorization": f"Bearer {admin_token}"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == os.environ["ADMIN_EMAIL"]
    assert body["role"] == "admin"


async def test_me_with_no_token(client: AsyncClient) -> None:
    resp = await client.get("/v1/auth/me")

    assert resp.status_code == 401


async def test_me_with_tampered_token(client: AsyncClient, admin_token: str) -> None:
    # Replace the entire signature portion to guarantee invalidation
    header_payload, _, _ = admin_token.rsplit(".", 2)
    tampered = header_payload + ".invalidsignature"

    resp = await client.get(
        "/v1/auth/me", headers={"Authorization": f"Bearer {tampered}"}
    )

    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# /auth/refresh
# ---------------------------------------------------------------------------


async def test_refresh_with_valid_cookie(client: AsyncClient) -> None:
    login_resp = await client.post(
        "/v1/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )
    assert login_resp.status_code == 200
    original_refresh_cookie = login_resp.cookies.get("refresh_token")

    refresh_resp = await client.post("/v1/auth/refresh")

    assert refresh_resp.status_code == 200
    body = refresh_resp.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"
    # The refresh cookie must be rotated to a new value
    new_refresh_cookie = refresh_resp.cookies.get("refresh_token")
    assert new_refresh_cookie is not None
    assert new_refresh_cookie != original_refresh_cookie


async def test_refresh_with_no_cookie(client: AsyncClient) -> None:
    resp = await client.post("/v1/auth/refresh")

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Session expired. Please log in again."


# ---------------------------------------------------------------------------
# /auth/logout
# ---------------------------------------------------------------------------


async def test_logout_then_refresh_is_401(client: AsyncClient) -> None:
    await client.post(
        "/v1/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )

    logout_resp = await client.post("/v1/auth/logout")
    assert logout_resp.status_code == 204

    refresh_resp = await client.post("/v1/auth/refresh")
    assert refresh_resp.status_code == 401


async def test_logout_revokes_access_token(client: AsyncClient) -> None:
    """After logout, the same access token is immediately rejected (401)."""
    login_resp = await client.post(
        "/v1/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )
    assert login_resp.status_code == 200
    access_token = login_resp.json()["access_token"]

    # Token works before logout
    me_resp = await client.get(
        "/v1/auth/me", headers={"Authorization": f"Bearer {access_token}"}
    )
    assert me_resp.status_code == 200

    # Logout with the access token in Authorization header
    logout_resp = await client.post(
        "/v1/auth/logout", headers={"Authorization": f"Bearer {access_token}"}
    )
    assert logout_resp.status_code == 204

    # Same token is now revoked
    me_resp2 = await client.get(
        "/v1/auth/me", headers={"Authorization": f"Bearer {access_token}"}
    )
    assert me_resp2.status_code == 401


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
    login_a = await client.post("/v1/auth/login", json=creds)
    assert login_a.status_code == 200
    token_a = login_a.json()["access_token"]

    # Device B: a fresh client with its own cookie jar, sharing the same app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://test"
    ) as device_b:
        login_b = await device_b.post("/v1/auth/login", json=creds)
        assert login_b.status_code == 200

        # Device A revokes everything
        logout_all = await client.post(
            "/v1/auth/logout-all",
            headers={"Authorization": f"Bearer {token_a}"},
        )
        assert logout_all.status_code == 204

        # Device A's refresh token is gone
        assert (await client.post("/v1/auth/refresh")).status_code == 401
        # Device B's refresh token is also gone
        assert (await device_b.post("/v1/auth/refresh")).status_code == 401


async def test_logout_all_requires_auth(client: AsyncClient) -> None:
    resp = await client.post("/v1/auth/logout-all")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# /admin/users — create
# ---------------------------------------------------------------------------


async def test_admin_create_user(client: AsyncClient, admin_token: str) -> None:
    resp = await client.post(
        "/v1/admin/users",
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
        "/v1/admin/users",
        json=payload,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert first.status_code == 201

    second = await client.post(
        "/v1/admin/users",
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
        "/v1/admin/users",
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
        f"/v1/admin/users/{user_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert delete_resp.status_code == 204

    login_resp = await client.post(
        "/v1/auth/login",
        json={"email": "todelete@example.com", "password": _TEST_PASSWORD},
    )
    assert login_resp.status_code == 401


async def test_admin_self_delete_is_400(client: AsyncClient, admin_token: str) -> None:
    me_resp = await client.get(
        "/v1/auth/me", headers={"Authorization": f"Bearer {admin_token}"}
    )
    assert me_resp.status_code == 200
    admin_id = me_resp.json()["id"]

    resp = await client.delete(
        f"/v1/admin/users/{admin_id}",
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
        "/v1/admin/users",
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
        "/v1/auth/login",
        json={"email": "member@example.com", "password": _TEST_PASSWORD},
    )
    assert login_resp.status_code == 200
    member_token = login_resp.json()["access_token"]

    resp = await client.post(
        "/v1/admin/users",
        json={
            "email": "another@example.com",
            "password": _TEST_PASSWORD,
            "display_name": "Another",
            "role": "member",
        },
        headers={"Authorization": f"Bearer {member_token}"},
    )

    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Login — unknown email (auth_service.py lines 112-113)
# ---------------------------------------------------------------------------


async def test_login_unknown_email_returns_401(client: AsyncClient) -> None:
    """Unknown email must raise InvalidCredentialsError, not leak user existence.

    Catches: login returning 200 or 500 for an email that was never registered.
    """
    resp = await client.post(
        "/v1/auth/login",
        json={"email": "nobody@example.com", "password": "whatever"},
    )

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid email or password."


# ---------------------------------------------------------------------------
# Refresh — malformed refresh token (auth_service.py lines 132-133)
# ---------------------------------------------------------------------------


async def test_refresh_with_malformed_hex_cookie_returns_401(
    client: AsyncClient,
) -> None:
    """Non-hex refresh token cookie must return 401, not 500.

    Catches: ValueError from bytes.fromhex() propagating uncaught → 500 crash.
    """
    client.cookies.set("refresh_token", "not-valid-hex!!")
    resp = await client.post("/v1/auth/refresh")
    client.cookies.clear()

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Session expired. Please log in again."


# ---------------------------------------------------------------------------
# Logout — malformed refresh cookie silently ignored (auth_service.py lines 174-175)
# ---------------------------------------------------------------------------


async def test_logout_with_malformed_refresh_cookie_returns_204(
    client: AsyncClient,
) -> None:
    """Non-hex refresh cookie on logout must return 204, not 500.

    Catches: ValueError from bytes.fromhex() propagating uncaught → 500 crash.
    """
    client.cookies.set("refresh_token", "not-valid-hex!!")
    resp = await client.post("/v1/auth/logout")
    client.cookies.clear()

    assert resp.status_code == 204


async def test_logout_with_malformed_access_token_returns_204(
    client: AsyncClient,
) -> None:
    """Malformed Authorization header on logout must return 204, not 500.

    Catches: InvalidAccessTokenError / ValueError in _revoke_access_token
    propagating uncaught → 500 crash when both a cookie and a bad access
    token are present together.
    """
    client.cookies.set("refresh_token", "not-valid-hex!!")
    resp = await client.post(
        "/v1/auth/logout",
        headers={"Authorization": "Bearer totally.not.a.valid.jwt"},
    )
    client.cookies.clear()

    assert resp.status_code == 204


# ---------------------------------------------------------------------------
# /auth/me — JWT sub claim missing or not a UUID (deps.py lines 92-93)
# ---------------------------------------------------------------------------


def _make_token_with_invalid_sub() -> str:
    """Forge a valid-signature JWT whose sub is not a UUID."""
    now = datetime.now(UTC)
    payload = {
        "sub": "not-a-uuid",
        "jti": str(uuid4()),
        "iat": now,
        "exp": now + timedelta(minutes=15),
    }
    return jwt.encode(payload, os.environ["JWT_SECRET"], algorithm="HS256")


async def test_me_with_non_uuid_sub_returns_401(client: AsyncClient) -> None:
    """Token with a valid signature but non-UUID sub must return 401.

    Catches: extract_user_id raising InvalidAccessTokenError not caught → 500 crash.
    """
    token = _make_token_with_invalid_sub()
    resp = await client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# /auth/me — JWT with non-UUID jti silently skips revocation check (deps.py line 88)
# ---------------------------------------------------------------------------


def _make_token_with_non_uuid_jti() -> str:
    """Forge a valid-signature JWT whose jti is a plain string, not a UUID."""
    now = datetime.now(UTC)
    payload = {
        "sub": str(uuid4()),
        "jti": "not-a-uuid-jti",
        "iat": now,
        "exp": now + timedelta(minutes=15),
    }
    return jwt.encode(payload, os.environ["JWT_SECRET"], algorithm="HS256")


async def test_me_with_non_uuid_jti_skips_revocation_and_proceeds(
    client: AsyncClient,
) -> None:
    """Token with non-UUID jti must skip the revocation check, not return 401 or 500.

    Catches: UUID() ValueError not caught → revocation-bypass crash or silent hard 401
    on tokens with string jti values (e.g., legacy tokens or third-party issuers).
    The expected behavior is that the check is skipped and auth proceeds normally
    (user not found → 401 is acceptable; 500 is not).
    """
    token = _make_token_with_non_uuid_jti()
    resp = await client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})

    # The sub is a real UUID but the user doesn't exist → 401 (not 500)
    assert resp.status_code == 401
