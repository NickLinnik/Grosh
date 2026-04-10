# Technical Specification: User Auth

- **Functional Specification:** `context/spec/002-user-auth/functional-spec.md`
- **Status:** Completed
- **Author(s):** Nick

---

## 1. High-Level Technical Approach

Auth lives entirely in the `services/api` service. No new services are introduced.

The implementation has four parts:
1. **Database schema** — `users`, `refresh_tokens`, `networks`, `network_members` tables managed by Alembic migrations. RLS policies applied in the same migration.
2. **FastAPI auth layer** — `POST /auth/login|refresh|logout` endpoints + a reusable `get_current_user` dependency injected into all protected routes.
3. **Admin endpoints** — `POST /admin/users`, `DELETE /admin/users/{id}` gated by a `require_admin` dependency.
4. **Frontend** — minimal `/login` page + a fetch interceptor that transparently refreshes the access token on 401.

All backend logic follows a 3-layer architecture: **Router → Service → Repository**.

---

## 2. Proposed Solution & Implementation Plan

### 2.1 New Dependencies

Added to `services/api/pyproject.toml`:

| Package | Purpose |
|---|---|
| `PyJWT>=2.8` | JWT encode/decode for access tokens |
| `passlib[bcrypt]>=1.7` | Password hash/verify abstraction (wraps bcrypt) |
| `alembic>=1.13` | Database schema migrations |
| `sqlalchemy>=2.0` | Required by Alembic for migration scaffolding (not used as ORM at runtime) |

New env vars added to `infra/.env.example`:

| Variable | Purpose |
|---|---|
| `JWT_SECRET` | HMAC secret for signing/verifying access tokens. Min 32 chars. |
| `DATABASE_URL` | asyncpg connection string: `postgresql+asyncpg://user:pass@timescaledb/grosh` |
| `ADMIN_EMAIL` | Seed: initial admin account email |
| `ADMIN_PASSWORD` | Seed: initial admin account password |

### 2.2 Database Schema

All tables managed by Alembic migrations in `services/api/migrations/`.

**`user_role` ENUM type** (created in `0001_initial_schema` before any table that uses it):
- Values: `'admin'`, `'member'`
- Adding a new value: `ALTER TYPE user_role ADD VALUE '...'` (non-locking in Postgres 12+, handled via Alembic)
- Mirrored in Python as `UserRole(str, Enum)` in `shared/src/grosh_shared/models.py` — all services use this type in Pydantic schemas and service logic

**`users` table** (mutable — has `updated_at`)

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | Default `gen_random_uuid()` |
| `email` | TEXT UNIQUE NOT NULL | Lowercased on insert |
| `password_hash` | TEXT NOT NULL | bcrypt hash |
| `display_name` | TEXT NOT NULL | |
| `role` | `user_role` NOT NULL | PostgreSQL ENUM type, default `'member'` — platform-level role |
| `is_active` | BOOLEAN NOT NULL | Default `true`; soft-delete sets to `false` |
| `created_at` | TIMESTAMPTZ NOT NULL | Default `now()` |
| `updated_at` | TIMESTAMPTZ NOT NULL | Default `now()`; updated via trigger on any row change |

**`refresh_tokens` table** (append-only — `created_at` only)

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `user_id` | UUID FK → users.id | CASCADE on delete |
| `token_hash` | TEXT UNIQUE NOT NULL | SHA-256 of the raw token — never store plaintext |
| `expires_at` | TIMESTAMPTZ NOT NULL | `now() + 30 days` on insert |
| `created_at` | TIMESTAMPTZ NOT NULL | Default `now()` |

> **Why hash the refresh token?**
> Refresh tokens are equivalent to passwords — storing plaintext means a DB leak gives an attacker long-lived sessions. SHA-256 of the raw token is stored; the raw token is returned to the client once and never persisted.

**`networks` table** (append-only — `created_at` only)

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `name` | TEXT NOT NULL | e.g. `"Family"` |
| `created_at` | TIMESTAMPTZ NOT NULL | Default `now()` |

**`network_members` table** (mutable — `role` can change, has `updated_at`)

| Column | Type | Notes |
|---|---|---|
| `network_id` | UUID FK → networks.id | |
| `user_id` | UUID FK → users.id | |
| `role` | `user_role` NOT NULL | Same PostgreSQL ENUM — network-scoped role, independent of platform role |
| `joined_at` | TIMESTAMPTZ NOT NULL | Default `now()` |
| `updated_at` | TIMESTAMPTZ NOT NULL | Default `now()`; updated via trigger |
| PK | `(network_id, user_id)` | Composite |

> **Why two `role` columns — one on `users`, one on `network_members`?**
> `users.role` is the platform role: controls who can call `/admin/*`. `network_members.role` is the network role: controls who can manage sharing within a specific network (Phase 4). A user could be a platform `member` but a network `admin` of a shared-expenses network they own. Today they'll always match; keeping them separate avoids a schema migration in Phase 4.

**RLS policies** (applied in the same migration as table creation):
- RLS enabled on `users`; policy: `USING (id = current_setting('app.current_user_id')::uuid)`.
- `refresh_tokens` exempt from RLS — auth endpoints run before the user session variable is set.
- All future user-scoped tables will follow the same `user_id = current_setting('app.current_user_id')::uuid` pattern.

**Seed data** (separate Alembic `seed` revision, runs after schema):
- One `networks` row: `{name: "Family"}`.
- One `users` row: admin account from `ADMIN_EMAIL` / `ADMIN_PASSWORD` env vars. If either is unset, the seed fails loudly — never silently creates a blank-credential account.
- One `network_members` row: admin → Family network, `role: 'admin'`.

### 2.3 Alembic Setup

```
services/api/
├── migrations/
│   ├── env.py                           # reads DATABASE_URL from env
│   ├── versions/
│   │   ├── 0001_initial_schema.py       # users, refresh_tokens, networks, network_members, RLS
│   │   └── 0002_seed.py                 # admin user + family network
│   └── script.py.mako
└── alembic.ini                          # points to migrations/, reads DATABASE_URL
```

Makefile gets a `migrate` target: `uv run alembic -c services/api/alembic.ini upgrade head`.

### 2.4 API Contracts

All endpoints return `application/json`. Errors follow `{"detail": "message"}` (FastAPI default).

| Method | Path | Auth required | Description |
|---|---|---|---|
| `POST` | `/auth/login` | No | Issue access token + refresh cookie |
| `POST` | `/auth/refresh` | Cookie | Rotate refresh token, issue new access token |
| `POST` | `/auth/logout` | Bearer + Cookie | Revoke refresh token, clear cookie |
| `POST` | `/admin/users` | Bearer (admin) | Create user account |
| `DELETE` | `/admin/users/{id}` | Bearer (admin) | Soft-delete user account |

**`POST /auth/login`**
- Request: `{email: str, password: str}`
- Response 200: `{access_token: str, token_type: "bearer"}` + `Set-Cookie: refresh_token=<token>; HttpOnly; SameSite=Lax; Max-Age=2592000`
- Response 401: `{"detail": "Invalid email or password."}`

**`POST /auth/refresh`**
- Request: cookie `refresh_token`
- Response 200: `{access_token: str, token_type: "bearer"}` + rotated cookie
- Response 401: `{"detail": "Session expired. Please log in again."}`

**`POST /auth/logout`**
- Response 204; clears cookie; deletes `refresh_tokens` row

**`POST /admin/users`**
- Request: `{email: str, password: str, display_name: str, role: "admin"|"member"}`
- Response 201: `{id: uuid, email: str, display_name: str, role: str}`
- Response 409: `{"detail": "A user with this email already exists."}`

**`DELETE /admin/users/{id}`**
- Response 204: sets `is_active = false`, deletes all `refresh_tokens` for that user
- Response 400: `{"detail": "Cannot delete your own account."}`
- Response 404: user not found

### 2.5 File Structure

```
services/api/src/grosh_api/
├── main.py                      # register routers
├── deps.py                      # get_db_conn(), get_current_user(), require_admin()
├── routers/
│   ├── auth.py                  # HTTP layer: /auth/* — parse, call service, respond
│   └── admin.py                 # HTTP layer: /admin/users — parse, call service, respond
├── services/
│   ├── auth_service.py          # business logic: verify password, issue/rotate/revoke tokens
│   └── user_service.py          # business logic: create user, soft-delete, enforce rules
└── repositories/
    ├── user_repo.py             # SQL: get_by_email(), get_by_id(), create(), set_active()
    └── token_repo.py            # SQL: create(), get_by_hash(), delete(), delete_by_user()
```

**Layer contracts:**
- Routers receive `asyncpg.Connection` via `deps.get_db_conn()`, pass it to services.
- Services receive the connection, call repositories, apply business rules, raise `HTTPException` for domain errors.
- Repositories receive the connection and return typed Pydantic models or `None`. No business logic.

### 2.6 Shared Models Update

`shared/src/grosh_shared/models.py` — two changes:
- Add `UserRole(str, Enum)` with values `admin` and `member`. Using `str, Enum` means Pydantic serializes it as a plain string — no surprises in JSON responses.
- Update `User`: replace `is_admin: bool` with `role: UserRole`; add `is_active: bool`.

Breaking change — but `consumer` and `ml` don't reference `User` yet, so zero downstream impact.

### 2.7 Frontend — Minimal Login Page

**`services/frontend/src/app/login/page.tsx`** — email + password form, calls `POST /auth/login`, stores access token in React context, redirects to `/` on success.

**`services/frontend/src/lib/api.ts`** — fetch wrapper that:
1. Attaches `Authorization: Bearer <token>` to every request.
2. On 401: calls `POST /auth/refresh` once, retries original request.
3. On second 401: clears token state, redirects to `/login` with "Your session has expired. Please log in again."

All non-`/login` routes wrapped in a client-side auth guard (redirects to `/login` if no token in context).

---

## 3. Impact and Risk Analysis

- **`shared/models.py` breaking change:** `is_admin` → `role`. Consumer and ml don't reference `User` yet — zero risk now. Must be noted for future services.
- **RLS must precede user-scoped tables:** RLS setup is migration `0001`. All future migrations that add user-scoped tables must include the corresponding policy. Document this as a convention in the migration file itself.
- **Refresh token rotation race:** On `POST /auth/refresh`, old token is deleted and new one issued atomically (single transaction). A concurrent refresh from two tabs will cause one to 401. Acceptable for a 3-user app.
- **Access token not revocable:** Stateless JWTs remain valid for 15 min after logout. Accepted tradeoff per functional spec.
- **Seed failure on missing env vars:** `ADMIN_EMAIL` / `ADMIN_PASSWORD` unset → seed raises, migration fails loudly. Correct behavior — prevents a zero-credential admin account.
- **`updated_at` trigger:** Applied via a reusable PL/pgSQL trigger function defined once in `0001_initial_schema` and attached to each mutable table. Future mutable tables should attach the same trigger.

---

## 4. Testing Strategy

**Unit tests** (`services/api/tests/unit/`):
- `auth/jwt.py` — encode/decode round-trip; expired token raises; tampered signature raises.
- `auth/password.py` — hash/verify round-trip; wrong password returns `False`.
- `services/user_service.py` — soft-delete own account raises; duplicate email raises.

**Integration tests** (`services/api/tests/integration/`, `httpx.AsyncClient` against real test DB):
- Login happy path → 200, access token, cookie set.
- Login wrong password → 401.
- Protected endpoint with valid token → 200; RLS returns only own row.
- Protected endpoint with expired token → 401.
- Refresh with valid cookie → new access token; old refresh token row gone.
- Refresh with expired cookie → 401 with session-expired message.
- Logout → cookie cleared; subsequent refresh → 401.
- Admin creates user → 201; duplicate email → 409.
- Admin soft-deletes user → 204; deleted user login → 401.
- Non-admin calls `/admin/*` → 403.
- Admin cannot delete own account → 400.

---

## 5. Deferred Hardening — Future Considerations

Items intentionally scoped out of v1 but worth revisiting when the threat model justifies the complexity. None of these are required for the 3-user family deployment today.

### 5.1 Refresh token reuse detection

**What:** Detect when a refresh token that has already been rotated is presented again — a strong signal of token theft (either an attacker replaying a stolen token or a compromised device racing a legitimate one).

**Naive implementation (rejected):** On refresh, if the hash is not found in `refresh_tokens`, revoke all of that user's tokens. Simple (~10 lines) but has a painful UX cliff: any concurrent-tab race causes every device to log out. Unacceptable for normal use.

**Production implementation:** Token families + grace window, the approach used by Auth0, Okta, Supabase, and recommended by OAuth 2.1.

1. **Schema change:** add `family_id UUID NOT NULL` and `rotated_at TIMESTAMPTZ` columns to `refresh_tokens`. On login, generate a fresh `family_id`. On rotation, the new token inherits the parent's `family_id`.
2. **Soft-delete on rotation:** instead of `DELETE`, set `rotated_at = now()` on the old row. Insert the new row with the same `family_id`.
3. **Grace window (~15–30s):** if a token is presented and `rotated_at` is within the window, return the child token idempotently instead of triggering the alarm. Covers the concurrent-tab race without false positives.
4. **Reuse alarm:** if `rotated_at` is outside the grace window, revoke the **family** (`DELETE FROM refresh_tokens WHERE family_id = $1`). Only the compromised session dies — other devices stay logged in.
5. **Cleanup job:** periodically delete rows where `rotated_at < now() - interval '30 days'`.

**Estimated size:** ~30–50 lines across `auth_service.refresh`, a new migration, and tests. Backend-only — no frontend changes needed.

**Why deferred:** Today's threat model (self-hosted, invite-only, 3 family members, httpOnly + SameSite=Lax cookies) makes token theft unlikely, and the family-level isolation RLS provides is already stronger than what reuse detection would add. Revisit when user count grows, when production deployment broadens the attack surface, or when Phase 4 sharing features raise the stakes of a single compromised session.

### 5.2 Access token revocation

**What:** Stateless JWTs are valid for the full 15-min TTL even after logout. Someone who steals an access token has a 15-min window of use regardless of what the server does.

**Approaches worth considering later:**
- **JTI denylist in Redis** with TTL matching the token — small hot-path lookup, no DB pressure.
- **Shorter access tokens** (e.g. 2 min) — narrows the window without infrastructure changes, at the cost of more refresh calls.
- **Session-bound tokens** — replace stateless JWTs with opaque session IDs. Cleanest but requires a DB lookup on every request.

**Why deferred:** The functional spec explicitly accepts this tradeoff for v1. None of the approaches above are appropriate without a concrete incident or a stricter threat model driving the decision.
