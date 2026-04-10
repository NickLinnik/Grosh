# Functional Specification: User Auth

- **Roadmap Item:** Infrastructure & Auth → User auth
- **Status:** Completed
- **Author:** Nick

---

## 1. Overview and Rationale (The "Why")

The platform is private and invite-only — no public registration is allowed. Before any feature can be used, each request must be authenticated so the system knows who is asking and can enforce per-user data isolation.

Without auth, all API endpoints are open and all data is shared. With auth, every user sees only their own data. Security is enforced at two layers: FastAPI validates the JWT on every request, and PostgreSQL RLS enforces isolation at the query level regardless of application logic.

The network entity is defined at this stage — even though the sharing UI comes in Phase 4 — because it shapes the data model that all subsequent features build on. Getting the schema right now avoids a costly migration later.

**Success:** A user can log in, use the app for up to 30 days without re-entering credentials, and be prompted to log in again when their session expires. The admin can provision and soft-delete accounts via API. A network and its memberships are representable in the database.

---

## 2. Functional Requirements (The "What")

### 2.1 Login

- **As a user, I want to log in with my email and password** so that I can access my personal finance data.
  - **Acceptance Criteria:**
    - [x] `POST /auth/login` accepts `{email, password}` and returns a short-lived access token (15-min TTL) in the response body and sets an `httpOnly` refresh token cookie (30-day TTL).
    - [x] Invalid credentials return HTTP 401 with a generic message: `"Invalid email or password."` — no hint about which field is wrong.
    - [x] A `/login` page exists in the frontend. On success, the user is redirected to the main feed. (UI is minimal — just functional enough to test auth end-to-end.)

### 2.2 Session Refresh

- **As a logged-in user, I want my session to stay active** so that I don't have to re-enter my password every 15 minutes.
  - **Acceptance Criteria:**
    - [x] `POST /auth/refresh` accepts the refresh token cookie and returns a new access token.
    - [x] If the refresh token is expired or invalid, the endpoint returns HTTP 401.
    - [x] The frontend transparently calls `/auth/refresh` when the access token expires — no user interaction required.
    - [x] If refresh fails (token expired after 30 days of inactivity), the user sees: "Your session has expired. Please log in again." and is redirected to `/login`.

### 2.3 Logout

- **As a user, I want to log out** so that my session is closed on this device.
  - **Acceptance Criteria:**
    - [x] `POST /auth/logout` invalidates the refresh token on the server and clears the cookie.
    - [x] After logout, any request with the old access token is still valid until its 15-min TTL expires (stateless — acceptable tradeoff for v1).
    - [x] After logout, navigating to any protected page redirects to `/login`.

### 2.4 User Management (Admin — API only)

- **As the admin, I want to create user accounts via API** so that family members can log in.
  - **Acceptance Criteria:**
    - [x] `POST /admin/users` accepts `{email, password, display_name, role}`. Role defaults to `member`.
    - [x] Returns HTTP 201 with the created user's id, email, display_name, and role.
    - [x] Duplicate email returns HTTP 409: `"A user with this email already exists."`
    - [x] Only users with `admin` role can call `/admin/*` endpoints. Others receive HTTP 403.

- **As the admin, I want to soft-delete a user account** so that a former family member can no longer log in.
  - **Acceptance Criteria:**
    - [x] `DELETE /admin/users/{id}` marks the user as inactive. They can no longer log in and their tokens are invalidated.
    - [x] The user's data remains in the database, untouched.
    - [x] The admin cannot delete their own account — returns HTTP 400: `"Cannot delete your own account."`

### 2.5 Network Data Model

- **The system must represent a family network and its members at the database level** so that Phase 4 sharing features can be built without a schema migration.
  - **Acceptance Criteria:**
    - [x] A `networks` table exists: `(id UUID PK, name TEXT, created_at TIMESTAMPTZ)`.
    - [x] A `network_members` join table exists: `(network_id UUID FK, user_id UUID FK, role TEXT CHECK IN ('admin','member'), joined_at TIMESTAMPTZ)`. A user can belong to multiple networks.
    - [x] The initial network and the admin's membership are created as part of the database seed / first-run setup.
    - [x] No API endpoints or UI expose network management in this spec — tables exist but are not surfaced yet.

### 2.6 FastAPI Auth Middleware & RLS

- **The system must enforce authentication on all protected endpoints.**
  - **Acceptance Criteria:**
    - [x] All endpoints except `POST /auth/login` and `POST /auth/refresh` require a valid `Authorization: Bearer <token>` header.
    - [x] On each authenticated request, FastAPI sets the PostgreSQL session variable `app.current_user_id` from the JWT claim before executing any query.
    - [x] RLS policies on all user-scoped tables use `current_setting('app.current_user_id')` to filter rows — enforced at the DB layer regardless of application logic.

---

## 3. Scope and Boundaries

### In-Scope
- Login, session refresh, logout endpoints (`/auth/*`)
- JWT access token (15-min TTL, in response body) + refresh token (30-day TTL, `httpOnly` cookie)
- Admin user management endpoints (`/admin/users`) — API only, no UI
- Soft-delete for user accounts
- `networks` and `network_members` tables (schema + seed data only)
- FastAPI auth middleware and PostgreSQL RLS setup
- Minimal `/login` frontend page (functional, not polished)

### Out-of-Scope
- Admin UI for user management (Phase 4 — Family Network)
- Network management API or UI (Phase 4)
- Sharing permissions between users (Phase 4)
- Password reset / email-based account setup (deferred — no SMTP yet)
- Account self-service: users cannot change their own email or password in v1
