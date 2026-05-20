import os

from httpx import AsyncClient

# Throwaway password for ephemeral test users. All data is rolled back.
_TEST_PASSWORD = "test-only-not-a-secret"  # noqa: S105


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_member(client: AsyncClient, admin_token: str, email: str) -> str:
    """Create a member user and return their id."""
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


async def _login(client: AsyncClient, email: str, password: str) -> str:
    resp = await client.post(
        "/v1/auth/login", json={"email": email, "password": password}
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# (a) Admin caller gets paginated list; last_active_at populated after login
# ---------------------------------------------------------------------------


async def test_admin_list_users_returns_all(
    client: AsyncClient, admin_token: str
) -> None:
    resp = await client.get(
        "/v1/admin/users",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert "next_cursor" in body
    assert isinstance(body["items"], list)
    # At minimum the admin user itself is in the list
    assert len(body["items"]) >= 1
    emails = [u["email"] for u in body["items"]]
    assert os.environ["ADMIN_EMAIL"] in emails


async def test_admin_list_users_last_active_at_populated(
    client: AsyncClient, admin_token: str
) -> None:
    """Users who have logged in have last_active_at set; others have null."""
    resp = await client.get(
        "/v1/admin/users",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    admin_entry = next(
        u for u in resp.json()["items"] if u["email"] == os.environ["ADMIN_EMAIL"]
    )
    assert admin_entry["last_active_at"] is not None


# ---------------------------------------------------------------------------
# (b) Non-admin caller → 403 INSUFFICIENT_PERMISSIONS
# ---------------------------------------------------------------------------


async def test_non_admin_list_users_is_403(
    client: AsyncClient, admin_token: str
) -> None:
    await _create_member(client, admin_token, "listmember-403@example.com")
    member_token = await _login(client, "listmember-403@example.com", _TEST_PASSWORD)

    resp = await client.get(
        "/v1/admin/users",
        headers={"Authorization": f"Bearer {member_token}"},
    )

    assert resp.status_code == 403
    assert resp.json()["code"] == "INSUFFICIENT_PERMISSIONS"


# ---------------------------------------------------------------------------
# (c) limit=2 with 3 users: page 1 returns 2 + next_cursor; page 2 returns 1
# ---------------------------------------------------------------------------


async def test_pagination_two_pages(client: AsyncClient, admin_token: str) -> None:
    await _create_member(client, admin_token, "paguser-a@example.com")
    await _create_member(client, admin_token, "paguser-b@example.com")
    await _create_member(client, admin_token, "paguser-c@example.com")

    # Page 1
    page1 = await client.get(
        "/v1/admin/users",
        params={"limit": 2},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert page1.status_code == 200
    body1 = page1.json()
    assert len(body1["items"]) == 2
    assert body1["next_cursor"] is not None
    # total reflects the full DB count (admin + 3 members = 4), not just this page
    assert body1["total"] == 4

    # Page 2 using the cursor from page 1
    page2 = await client.get(
        "/v1/admin/users",
        params={"limit": 2, "cursor": body1["next_cursor"]},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert page2.status_code == 200
    body2 = page2.json()
    assert len(body2["items"]) >= 1
    # total is consistent across pages
    assert body2["total"] == 4
    # Pages must not overlap
    ids1 = {u["id"] for u in body1["items"]}
    ids2 = {u["id"] for u in body2["items"]}
    assert ids1.isdisjoint(ids2)


# ---------------------------------------------------------------------------
# (d) Invalid cursor → 400 INVALID_CURSOR
# ---------------------------------------------------------------------------


async def test_invalid_cursor_returns_400(
    client: AsyncClient, admin_token: str
) -> None:
    resp = await client.get(
        "/v1/admin/users",
        params={"cursor": "not-a-valid-cursor!!!"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 400
    assert resp.json()["code"] == "INVALID_CURSOR"


# ---------------------------------------------------------------------------
# (e) After login, last_active_at is updated
# ---------------------------------------------------------------------------


async def test_login_updates_last_active_at(
    client: AsyncClient, admin_token: str
) -> None:
    email = "activityuser@example.com"
    await _create_member(client, admin_token, email)

    # Before login the new user has no last_active_at
    list_resp = await client.get(
        "/v1/admin/users",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert list_resp.status_code == 200
    user_before = next(
        (u for u in list_resp.json()["items"] if u["email"] == email),
        None,
    )
    assert user_before is not None
    assert user_before["last_active_at"] is None

    # Log in as that user
    await _login(client, email, _TEST_PASSWORD)

    # After login last_active_at must be set
    list_resp2 = await client.get(
        "/v1/admin/users",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert list_resp2.status_code == 200
    user_after = next(
        (u for u in list_resp2.json()["items"] if u["email"] == email),
        None,
    )
    assert user_after is not None
    assert user_after["last_active_at"] is not None
