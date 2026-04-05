# Tasks: User Auth

**Spec:** `context/spec/002-user-auth/`
**Rule:** The stack must remain runnable after each slice is completed.

---

## Slice 1: Schema + migrations — users, networks, RLS

_`make migrate` runs cleanly; all tables exist; RLS is active; `make dev` still starts._

- [ ] Add `PyJWT>=2.8`, `passlib[bcrypt]>=1.7`, `alembic>=1.13`, `sqlalchemy>=2.0` to `services/api/pyproject.toml`. Run `uv sync --all-packages --all-extras`. **[Agent: python-backend]**
- [ ] Add `JWT_SECRET`, `DATABASE_URL`, `ADMIN_EMAIL`, `ADMIN_PASSWORD` to `infra/.env.example` with comments. Populate `infra/.env`. **[Agent: python-backend]**
- [ ] Add `UserRole(str, Enum)` to `shared/src/grosh_shared/models.py`. Update `User`: replace `is_admin: bool` with `role: UserRole`; add `is_active: bool`. **[Agent: python-backend]**
- [ ] Bootstrap Alembic in `services/api/migrations/`; configure `env.py` to read `DATABASE_URL` from environment; add `migrate` target to Makefile. **[Agent: postgres-database]**
- [ ] Write migration `0001_initial_schema`: create `user_role` ENUM; `users`, `refresh_tokens`, `networks`, `network_members` tables with all columns, FKs, and `updated_at` trigger function; enable RLS on `users` with policy `USING (id = current_setting('app.current_user_id')::uuid)`. **[Agent: postgres-database]**
- [ ] Write migration `0002_seed`: read `ADMIN_EMAIL`/`ADMIN_PASSWORD` from env (fail loudly if unset); insert Family network, admin user (bcrypt-hashed password), admin `network_members` row. **[Agent: postgres-database]**
- [ ] Verify: `make dev` starts; `make migrate` runs both revisions without error; confirm via `psql` that all tables exist, `user_role` ENUM exists, RLS is enabled on `users`, seed rows are present. **[Agent: general-purpose]**

---

## Slice 2: Login endpoint — issue access token + refresh cookie

_`POST /auth/login` with valid credentials returns a JWT and sets the `httpOnly` cookie._

- [ ] Create `services/api/src/grosh_api/auth/jwt.py`: `encode_access_token(user_id, role)` → signed JWT (15-min TTL, HS256); `decode_access_token(token)` → payload or raises. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/auth/password.py`: `hash_password(plain)` → bcrypt hash; `verify_password(plain, hashed)` → bool. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/deps.py`: `get_db_conn()` — asyncpg connection pool, yield connection per request; wire pool init/teardown into lifespan in `main.py`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/repositories/user_repo.py`: `get_by_email(conn, email)` → `User | None`; `get_by_id(conn, id)` → `User | None`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/repositories/token_repo.py`: `create(conn, user_id, token_hash, expires_at)`; `get_by_hash(conn, token_hash)` → row or None; `delete(conn, token_id)`; `delete_by_user(conn, user_id)`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/auth/tokens.py`: `create_refresh_token(conn, user_id)` → raw token string (SHA-256 hash stored in DB, 30-day TTL). **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/services/auth_service.py`: `login(conn, email, password)` → `(access_token, raw_refresh_token)` or raises `HTTPException 401`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/auth.py`: `POST /auth/login` — call `auth_service.login`, return `{access_token, token_type}`, set `httpOnly; SameSite=Lax` refresh cookie. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Verify: `curl -X POST http://localhost:8000/auth/login -H 'Content-Type: application/json' -d '{"email":"...","password":"..."}' -c cookies.txt` returns `{access_token: "..."}` and sets cookie; wrong password returns 401. **[Agent: general-purpose]**

---

## Slice 3: Auth middleware — protect endpoints, enforce RLS

_All endpoints except `/auth/login` and `/auth/refresh` require a valid Bearer token; DB session variable is set per request._

- [ ] Add `get_current_user(conn, token)` to `deps.py`: decode JWT → `SET LOCAL app.current_user_id = '...'` on the connection → fetch user → raise 401 if missing or inactive. **[Agent: python-backend]**
- [ ] Add `require_admin(current_user)` to `deps.py`: assert `current_user.role == UserRole.admin`, raise 403 otherwise. **[Agent: python-backend]**
- [ ] Add `GET /auth/me` (returns current user JSON) as the first protected route to smoke-test the middleware. **[Agent: python-backend]**
- [ ] Verify: valid Bearer on `GET /auth/me` returns user JSON; missing token returns 401; expired token returns 401; confirm RLS by checking that a direct `SELECT * FROM users` through the authenticated session returns only the current user's row. **[Agent: general-purpose]**

---

## Slice 4: Refresh + logout

_Full session lifecycle works end-to-end._

- [ ] Add to `auth_service.py`: `refresh(conn, raw_token)` → new `(access_token, raw_refresh_token)` atomically (delete old, insert new in one transaction); raise 401 if token not found or expired. `logout(conn, raw_token)` → delete token row. **[Agent: python-backend]**
- [ ] Add `POST /auth/refresh` and `POST /auth/logout` to `routers/auth.py`. **[Agent: python-backend]**
- [ ] Verify: refresh with valid cookie returns new access token and rotates cookie; refresh with expired/missing cookie returns 401 with `"Session expired. Please log in again."`; logout clears cookie and subsequent refresh returns 401. **[Agent: general-purpose]**

---

## Slice 5: Admin user management endpoints

_Admin can create and soft-delete users via API._

- [ ] Add to `user_repo.py`: `create(conn, email, password_hash, display_name, role)` → `User`; `set_active(conn, user_id, is_active)`; `exists_by_email(conn, email)` → bool. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/services/user_service.py`: `create_user(conn, ...)` — lowercase email, check duplicate (409), hash password, insert; `delete_user(conn, admin_id, target_id)` — guard self-delete (400), set inactive, revoke all tokens. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/admin.py`: `POST /admin/users` (201 / 409); `DELETE /admin/users/{id}` (204 / 400 / 404). Both gated by `require_admin`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Verify: `POST /admin/users` creates user (201); duplicate email returns 409; `DELETE /admin/users/{id}` soft-deletes (204); deleted user login returns 401; non-admin calling `/admin/*` returns 403; admin self-delete returns 400. **[Agent: general-purpose]**

---

## Slice 6: Unit + integration tests

_`make test` is green for the api service; CI passes._

- [ ] Write unit tests in `services/api/tests/unit/`: `test_jwt.py` (encode/decode round-trip, expired token raises, tampered signature raises); `test_password.py` (hash/verify round-trip, wrong password returns False). **[Agent: python-backend]**
- [ ] Write integration tests in `services/api/tests/integration/` using `httpx.AsyncClient` against a real test DB (transaction rollback per test): cover all acceptance criteria from `functional-spec.md §2`. **[Agent: python-backend]**
- [ ] Verify: `make test` passes for `services/api`; CI lint + test passes on push. **[Agent: general-purpose]**

---

## Slice 7: Minimal frontend login page

_A user can log in via the browser; session persists; expired session shows the correct message._

- [ ] Create React auth context (`services/frontend/src/lib/auth-context.tsx`): stores access token in memory; exposes `login()`, `logout()`, `accessToken`. **[Agent: nextjs-frontend]**
- [ ] Create fetch wrapper (`services/frontend/src/lib/api.ts`): attaches Bearer header; on 401 calls `POST /auth/refresh` once and retries; on second 401 clears token and redirects to `/login` with "Your session has expired. Please log in again." **[Agent: nextjs-frontend]**
- [ ] Create `/login` page (`services/frontend/src/app/login/page.tsx`): email + password form, calls `POST /auth/login`, stores token in auth context, redirects to `/` on success, shows error on 401. **[Agent: nextjs-frontend]**
- [ ] Add auth guard: all non-`/login` routes redirect to `/login` if no token in context. **[Agent: nextjs-frontend]**
- [ ] Verify: `make dev`; `http://localhost:3000` redirects to `/login`; login with admin credentials redirects to `/`; DevTools confirms `refresh_token` cookie is `httpOnly`; logout redirects back to `/login`. **[Agent: general-purpose]**