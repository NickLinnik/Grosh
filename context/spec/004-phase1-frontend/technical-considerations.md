# Technical Specification: Frontend (Phase 1)

- **Functional Specification:** [`./functional-spec.md`](./functional-spec.md)
- **Status:** Draft
- **Author:** Nick

---

## 1. High-Level Technical Approach

Spec 004 ships the first real-user-facing surface of the Grosh frontend. The dashboard is a **single-route Next.js App Router page** rendered as a **client-side SPA** (root `'use client'` boundary, no SSR data fetching) that consumes the spec-003 REST endpoints. The implementation is a thin shell over four primitives:

- **TanStack Query v5** owns every byte of server state (accounts, transactions, aggregates, integrations, K8s job statuses). No `useEffect`+`useState` loops for data fetching; every fetch goes through a generated query hook.
- **Zustand v5** owns all client state — both auth (replacing the existing `AuthContext`) and UI state (filter chips, account-filter dropdown, display-currency selector, drill-down open row, session-scoped reprocess job pointer). One pattern across the app.
- **shadcn/ui + Tailwind CSS v4** owns visual primitives (cards, dialogs, dropdowns, badges, skeletons, toasts via `sonner`). No custom component library; we instantiate shadcn primitives and compose them.
- **`@hey-api/openapi-ts` with the TanStack Query plugin** generates the typed client + query/mutation hooks from the two backends' OpenAPI documents at build time. No hand-typed response models anywhere in `src/`.

The Phase 1 deliverable is a single `/` route + a re-export of the existing `/login` route from the auth scaffold. The dashboard surface composes five subtrees rendered top-to-bottom in a flex column: **banner row** (reprocess-in-progress only in Phase 1, per spec 004 §2.6 deferral), **account tiles strip**, **chart + KPI tile row**, **filters bar**, **infinite-scroll feed**. Each subtree owns its own TanStack Query and its own skeleton; failures in one don't block the others.

The build runs in **Node 20** containers, uses **Vitest** for unit/component tests, and gets a single **Playwright** smoke test (login → feed renders → click a transaction → drill-down opens). The frontend container's `next.config.ts` proxies `/api/*` to the two backends via Next.js `rewrites`; in dev the rewrites point at Compose service names (`http://api:8000`, `http://ingestion:8001`), in prod they point at k3s service DNS, configured via environment variables.

The existing scaffold (Next 14.2, Tailwind 3, Jest, plain React Context for auth) is **upgraded as part of this slice**, not preserved — Next 16, Tailwind 4, Vitest, Zustand. The upgrade pays for itself once and avoids a year of "we need to migrate" maintenance debt on a 3-component frontend.

---

## 2. Proposed Solution & Implementation Plan

### 2.1 Stack Pins

All version pins land in `services/frontend/package.json` at the start of the slice and stay locked through Phase 1.

| Package                        | Pin            | Role                                                                    |
|--------------------------------|----------------|-------------------------------------------------------------------------|
| `next`                         | `16.x`         | App Router, route prefetching, code-splitting, static asset pipeline    |
| `react`, `react-dom`           | `18.3.x`       | React 18 concurrent features (already on this major)                    |
| `typescript`                   | `5.x`          | Strict mode required                                                    |
| `@tanstack/react-query`        | `5.x`          | Server state, infinite queries, cache invalidation                      |
| `@tanstack/react-query-devtools` | `5.x` (dev)  | In-browser query inspector. **MUST be mounted conditionally**: `{process.env.NODE_ENV !== 'production' && <ReactQueryDevtools initialIsOpen={false} />}`. Being a devDep does not exclude it from runtime bundles if it's imported unconditionally from `Providers.tsx` — Next.js bundles any module reachable from `import` statements regardless of devDep status. Unconditional mount would expose the entire query cache (transaction amounts, account balances, descriptions) to anyone with browser-extension access in production. |
| `zustand`                      | `5.x`          | Auth store + UI state stores                                            |
| `tailwindcss`                  | `4.x`          | CSS-first theme (no postcss config), Lightning CSS                      |
| `shadcn`                       | latest CLI     | `npx shadcn@latest init` then per-component `add`                       |
| `class-variance-authority`     | `0.7.x`        | shadcn variant prop helper (already installed)                          |
| `clsx`, `tailwind-merge`       | latest         | shadcn `cn()` helper (already installed)                                |
| `lucide-react`                 | `0.400.x+`     | Icons (already installed)                                               |
| `sonner`                       | `2.x`          | Toasts (the shadcn-native toast component wraps sonner)                 |
| `recharts`                     | `2.x`          | Charts — `BarChart` + `ReferenceLine` for the diverging chart           |
| `date-fns` + `@date-fns/tz`    | `4.x`          | IANA-zone date math + relative time formatting                          |
| `@hey-api/openapi-ts`          | latest         | TypeScript types + TanStack Query hooks generated from OpenAPI          |
| `@hey-api/client-fetch`        | latest         | Runtime fetch client used by the generated hooks                        |
| `vitest`                       | `4.x`          | Unit + component tests (replaces Jest)                                  |
| `@vitejs/plugin-react`         | latest (dev)   | Vitest JSX support                                                      |
| `@testing-library/react`       | `14.x`         | Component test rendering                                                |
| `@testing-library/user-event`  | latest         | Realistic user interactions in component tests                          |
| `playwright`                   | latest         | Single E2E smoke test                                                   |
| `prettier`                     | `3.x`          | Format on save                                                          |
| `eslint-config-next`           | `16.x`         | Default Next.js ESLint rules                                            |
| `eslint-config-prettier`       | latest         | Disable ESLint rules that fight Prettier                                |

**Decisions deliberately deferred** (not installed in Phase 1; added when their first use case appears):

- **`react-hook-form` + `zod`** — there's no real form in spec 004 beyond the already-implemented login. Manual-entry forms (§2.5.3 mentions one) live in Settings, not the dashboard. We add the form stack when the first dashboard form appears.
- **Runtime API response validation (Zod schemas).** Generated OpenAPI types + TS strict catch enough at compile time. Dual-source-of-truth (OpenAPI spec + Zod schema) is the kind of "defense in depth" that drifts under real load.
- **`eslint-plugin-tailwindcss`** — class-ordering autofix is a nice-to-have, not load-bearing.

### 2.2 File Structure

Inside `services/frontend/src/`:

```
app/
  layout.tsx                  Root layout — providers + global styles
  page.tsx                    Dashboard route (the entire spec 004 surface)
  login/
    page.tsx                  Existing login page (already present)
components/
  ui/                         shadcn primitives (cli-generated; one file per component)
    button.tsx
    card.tsx
    badge.tsx
    skeleton.tsx
    dropdown-menu.tsx
    dialog.tsx
    sonner.tsx                shadcn's Toaster wrapper around sonner
    ...
  layout/
    Providers.tsx             QueryClientProvider + Toaster + (no auth provider — replaced by Zustand)
    AuthGuard.tsx              Updated to read from Zustand auth store (was: AuthContext)
  dashboard/
    Dashboard.tsx             Composes the five subtrees in §2.5 order
    BannerRow.tsx             Renders ReprocessBanner when active
    AccountTilesStrip.tsx     The §2.2 tile strip
    AccountTile.tsx           Single tile, including the cash-balance walk for cash accounts
    ChartAndKpi.tsx           Layout wrapper for chart + KPI tile
    DivergingChart.tsx        Recharts BarChart with stacked income/expense
    DivergingChartSmallMultiples.tsx Wrapper when >1 currency chip selected
    SavingsKpiTile.tsx        KPI tile with sparkline
    CurrencyChipsRow.tsx      Multi-select chip row (UAH/USD/EUR)
    FiltersBar.tsx            Type chips + account dropdown + display-currency dropdown
    TransactionFeed.tsx       Infinite-scroll feed
    TransactionRow.tsx        One row + collapsible drill-down
    TransactionRowDrillDown.tsx The expanded view from §2.5.4
    BadgeRow.tsx              The PENDING / paired / no-rate / MANUAL pills
    ReprocessBanner.tsx       The §2.7 banner with state machine
    ReprocessTriggerButton.tsx The shared §2.7 controller used by chart tooltip + row tooltip + banner Retry
hooks/
  useReprocessController.ts   The §2.7 trigger controller (state machine wrapping the mutation)
  useTimezoneBootstrap.ts     The §2.9 bootstrap effect (5s single-budget timeout, browser-zone fallback). Wrapped in `useAuthStore.runOnce("timezone-bootstrap", ...)` so subsequent dashboard remounts in the same browser-tab session don't re-issue the GET/PUT.
  useDashboardData.ts         Composite hook that wires together all the per-surface queries
  useInfiniteTransactions.ts  Wrapper around the generated useInfiniteQuery for the feed
  useAccountBalances.ts       Per-account balance walks (bank: limit=1 + cash: paginate to end)
  useChartAggregates.ts       Aggregates query, gated on settingsResolved
lib/
  api/
    generated/                Output of @hey-api/openapi-ts — DO NOT EDIT (gitignored or committed; see §2.7)
    client.ts                 Configures the hey-api fetch client (auth header injection from Zustand)
  auth-store.ts               Zustand store replacing src/lib/auth-context.tsx
  ui-store.ts                 Zustand store for filter/currency/account-filter/drill-down state
  reprocess-store.ts          Zustand store for the session-scoped active reprocess job pointer
  format.ts                   Money / date / relative-time formatters (date-fns wrappers)
  mcc.ts                      Static ISO 18245 MCC → label table for the drill-down
  parse-reprocess-detail.ts   Strict parser for 409 REPROCESS_LOCKED detail string (per §2.7)
styles/
  globals.css                 Tailwind v4 entrypoint + theme tokens
test/
  setup.ts                    Vitest setup (RTL matchers, MSW handlers)
  msw-handlers.ts             Mock service worker handlers for the API for component tests
e2e/
  smoke.spec.ts               Playwright login → feed → drill-down test
```

The existing files (`src/lib/api.ts`, `src/lib/auth-context.tsx`, `src/components/AuthGuard.tsx`) are **replaced** during the upgrade slice:

- `auth-context.tsx` → `lib/auth-store.ts` (Zustand). The cold-load refresh effect lives inside the store via a `bootstrap()` action called once from `Providers.tsx`.
- `api.ts` → the hey-api client (`lib/api/client.ts`) handles the auth-header injection by reading the Zustand store directly. The 401 → refresh → retry logic moves into a hey-api interceptor.
- `AuthGuard.tsx` keeps its existing routing logic but reads from the Zustand store via the `useAuthStore(s => s.accessToken)` selector instead of `useAuth()`.

### 2.3 Zustand Stores

Three stores. Each is a flat object — no nested slices, no devtools middleware in production builds. Selectors are used at every call site to minimize re-renders.

| Store                       | State                                                                                                | Actions                                                                                          | Why a store, not Context |
|-----------------------------|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|--------------------------|
| `useAuthStore`              | `accessToken: string \| null`, `isLoading: boolean` (true until cold-load refresh resolves), `currentUser: User \| null` (cached `/auth/me`), **`refreshPromise: Promise<string \| null> \| null`** (the actual in-flight token refresh — `null` when no refresh is happening; non-null callers `await` it instead of starting a new refresh), **`onceFlags: Set<string>`** (private — tracks `runOnce` keys that have already executed in this browser-tab session) | `login(email, pwd)`, `logout()`, `setToken(t)`, `bootstrap()` (called once on app mount), `setRefreshPromise(p)`, **`runOnce(key: string, fn: () => Promise<void>): Promise<void>`** | Replaces existing AuthContext; same data shape, no Provider tree, no ref indirection for `api.ts`. The `refreshPromise` field is a Promise, not a boolean — it must be awaitable so the §2.5 thundering-herd guarantee ("the others await the same promise") is actually implementable. |
| `useUiStore`                | `directionsVisible: {income, expense, transfers}`, `accountFilter: UUID \| null`, `displayCurrency: "UAH"\|"USD"\|"EUR"`, `chartCurrencies: Set<"UAH"\|"USD"\|"EUR">`, `openDrillDownId: UUID \| null`, **`settingsResolved: boolean`** (gates the aggregates query — see §2.6) | `toggleDirection(d)`, `setAccountFilter(uuid)`, `setDisplayCurrency(c)`, `toggleChartCurrency(c)`, `setOpenDrillDown(uuid)`, `resetFilters()`, `setSettingsResolved(b)` | UI state needs to be readable from many components without prop-drilling; doesn't belong in TanStack Query (not server state) |
| `useReprocessStore`         | `activeJob: {jobId, statusUrl} \| null`, `pendingTrigger: boolean`, **`pollAbortController: AbortController \| null`** (used to cancel the in-flight poll fetch on logout / clearTracking) | `startTracking({jobId, statusUrl})` (creates a fresh `AbortController`), `clearTracking()` (aborts + clears), `setPending(b)` | Lives across re-renders for the session; persisted to `sessionStorage` via Zustand's `persist` middleware so reloads rehydrate per spec 004 §2.7. The `AbortController` is NOT persisted — it's recreated on rehydration. |

**No `devtools` middleware** unless a debugger needs it locally — adds ~5 KB and exposes store internals to extensions. **`persist` middleware** is used only on `useReprocessStore` (keyed by `grosh:reprocess`, sessionStorage).

The `useAuthStore.bootstrap()` action wraps the existing cold-load refresh logic from `auth-context.tsx` (the `fetch('/api/auth/refresh', {credentials: 'include'})` call). It's invoked once from `Providers.tsx` inside a `useEffect(() => store.bootstrap(), [])`. Until it resolves, the store's `isLoading` flag stays `true` and `AuthGuard` renders `null` (same UX as today).

The `useAuthStore.runOnce(key, fn)` action provides the session-scoped guard the timezone bootstrap depends on (functional §2.9). Contract:
- If `onceFlags` does NOT contain `key`: add `key` to the set, run `await fn()`, and return.
- If `onceFlags` already contains `key`: return immediately (resolved void); do NOT re-run `fn`.
- The guard scope is the browser-tab session — the auth store is never persisted to storage, so a fresh tab gets a fresh `onceFlags` set. `useAuthStore.logout()` resets `onceFlags` to an empty set so the next user's session bootstrap fires correctly.
- Used by `useTimezoneBootstrap` with `key = "timezone-bootstrap"`; used in §2.5 by the cold-load refresh effect with `key = "auth-cold-load"` (this latter is what replaces the per-mount `useEffect` guard in the existing `auth-context.tsx`).
- `runOnce` is intentionally a thin guard, not a memoized-promise pattern: callers do not receive `fn`'s return value across calls; if they need data, they read it from whatever store `fn` populated.

### 2.4 Tailwind v4 Setup

Tailwind 4 uses CSS-first theme — no `tailwind.config.ts`, no `postcss.config.js`. The entire setup is:

- `services/frontend/src/styles/globals.css` imports Tailwind, declares the project's design tokens via `@theme {}` (CSS custom properties), and sets the base typography. shadcn-generated tokens (`--color-border`, `--color-background`, `--color-primary`, etc.) live in the same file under the shadcn convention.
- `services/frontend/src/app/layout.tsx` imports `globals.css` once. No other CSS imports anywhere.
- shadcn's `components.json` is created by `npx shadcn@latest init` with the v4 preset. shadcn-added components write into `src/components/ui/`.

Color tokens follow shadcn's `oklch()` palette. The diverging chart's green/red are pulled from the same tokens (`--color-chart-income`, `--color-chart-expense`) so the chart matches the row border colors in §2.5 of the functional spec.

### 2.5 Routing, Auth, and Reverse Proxy

The frontend is a Next.js App Router app with two routes: `/login` (existing) and `/` (new dashboard). Both have `'use client'` at the top of their `page.tsx`. The `app/layout.tsx` wraps children in `<Providers>` (the React Query provider + Sonner Toaster) and then `<AuthGuard>` (the redirect-on-401 wrapper).

**Reverse proxy contract.** `next.config.ts` declares two rewrites:

```
/api/v1/auth/:path*           → ${API_BASE}/v1/auth/:path*
/api/v1/transactions/:path*   → ${API_BASE}/v1/transactions/:path*
/api/v1/accounts/:path*       → ${API_BASE}/v1/accounts/:path*
/api/v1/settings/:path*       → ${API_BASE}/v1/settings/:path*
/api/v1/rates/:path*          → ${API_BASE}/v1/rates/:path*
/api/v1/admin/users/:path*    → ${API_BASE}/v1/admin/users/:path*
/api/v1/:path*                → ${INGESTION_BASE}/v1/:path*    (everything else)
```

`API_BASE` and `INGESTION_BASE` are environment variables. In dev (Compose) they resolve to `http://api:8000` and `http://ingestion:8001`. In prod (k3s) they resolve to the cluster-internal service DNS. The frontend never opens a request to an absolute URL; the browser only sees same-origin `/api/v1/...` paths.

**Implications:**
- Cookies (`grosh_refresh`, set by `/v1/auth/login`) are same-origin from the browser's perspective. `SameSite=Lax` is sufficient.
- CORS is not configured on either backend (it's not needed because the browser sees same-origin).
- The Next.js process becomes a thin proxy on the hot path. At ≤10 users this is a non-issue.
- The route table in `next.config.ts` is the single source of truth for which backend owns which endpoint family. When a new endpoint family is added (e.g. spec 003 adds an anomaly endpoint to the API service), only this file changes. A unit test under `test/next-config.test.ts` asserts that every endpoint name mentioned in `lib/api/generated/` maps to exactly one rewrite rule.

**`AuthGuard` behavior** is unchanged from the existing implementation: render `null` until `useAuthStore(s => s.isLoading)` is false, then redirect to `/login` if `accessToken` is null and the pathname is not `/login`. The redirect uses `router.replace()` (no history pollution).

**401 → refresh → retry** lives inside the generated hey-api client via an interceptor:
- If a request returns 401, the interceptor reads `useAuthStore.getState().refreshPromise`. If it's non-null, the interceptor awaits the existing promise; otherwise it sets a new promise via `setRefreshPromise(...)` and starts a single `POST /api/v1/auth/refresh`.
- On 200, store the new access token in `useAuthStore`, clear `refreshPromise` back to `null`, and replay the original request with the new token.
- On non-200 (refresh rejected), clear the auth store, run the **full logout teardown** (see below), and redirect to `/login?reason=expired`.
- Because `refreshPromise` is the actual `Promise<string | null>`, multiple in-flight 401s all `await` the same network round-trip — no thundering herd, no token rotation race.

**Logout teardown contract.** `useAuthStore.logout()` (whether called from the UI button or from the failed-refresh path above) is a multi-step teardown that runs in order:
1. `useReprocessStore.getState().clearTracking()` — aborts the in-flight reprocess poll fetch via its `AbortController` and clears the persisted session-storage entry. Prevents orphaned 5-second poll requests from hitting the backend with a freshly-revoked token.
2. `useAuthStore.setToken(null)` — clears the in-memory access token. In the same atomic store update, `setRefreshPromise(null)` clears the in-flight refresh promise (if any). Code paths still `await`-ing a now-cleared `refreshPromise` resolve normally to whatever value the in-flight fetch produced; the next 401-interceptor invocation reads `refreshPromise === null` and `accessToken === null` together, recognises the logged-out state, and short-circuits to the redirect path without starting a fresh refresh.
3. `queryClient.clear()` — wipes the entire TanStack Query cache. Critical for shared-device safety: if user A logs out and user B logs in on the same browser, user B must never see user A's cached transactions, balances, or aggregates. (Spec 004 §2.6 lists `accounts`, `transactions`, `aggregates`, `balance`, and `integrations` as user-scoped query keys; clearing the cache is the only cross-cutting way to invalidate all of them.)
4. `fetch('/api/v1/auth/logout', { method: 'POST', credentials: 'include' })` — fire-and-forget; the cookie has already been cleared on the client when the access token was nulled, so even if the network call fails the user is locally logged out.
5. `router.replace('/login')` — same redirect as the existing scaffold.

The full sequence runs synchronously through step 3 (no awaiting), then fires steps 4 and 5 in parallel.

### 2.6 Data Fetching Patterns

Every query uses a generated `@hey-api/openapi-ts` hook directly — we do not write hand-rolled `useQuery` calls. Each hook generator produces a `queryOptions` factory and a `useQuery`-like wrapper, both fully typed against the OpenAPI schema.

**Stable query-key shape** (matches the spec 004 §2.8 contract):

| Key                                                 | Owned by                | Stale time | `enabled` gating                                            | Refetch triggers                          |
|-----------------------------------------------------|-------------------------|------------|-------------------------------------------------------------|-------------------------------------------|
| `["accounts"]`                                      | `useAccountsQuery`      | 60 s       | `accessToken !== null`                                      | Tab focus, manual `refetch()`             |
| `["balance", account_id]`                           | `useAccountBalance`     | 60 s       | `accessToken !== null`                                      | Refetch after reprocess `succeeded`       |
| `["transactions", filters]`                         | `useInfiniteTransactions` | 0 (always background revalidate) | `accessToken !== null`                       | Filter change, currency change, reprocess `succeeded`, tab focus |
| `["aggregates", { currencies, from, to }]`          | `useChartAggregates`    | 30 s       | **`accessToken !== null && settingsResolved === true`**     | Currency chip toggle, reprocess `succeeded` |
| `["settings"]`                                      | `useSettingsQuery`      | 300 s      | `accessToken !== null`                                      | Manual                                    |
| `["integrations"]`                                  | `useIntegrationsQuery`  | 60 s       | `accessToken !== null`                                      | Tab focus                                 |
| `["reprocess-job", job_id]`                         | `useReprocessJobStatus` | 0 (5 s `refetchInterval`) | `activeJob !== null`                              | Banner polling loop                  |

The aggregates `enabled` flag enforces the §2.9 timezone-bootstrap gate — the chart never fetches with the persisted-but-stale UTC default before the bootstrap effect resolves. All other queries gate only on `accessToken` (they don't depend on the timezone).

**Filter changes use Zustand → query-key.** A component reads `filters` from `useUiStore` via a selector and passes them to the generated `useInfiniteTransactions(filters)` hook. TanStack Query's by-value key hashing means the key changes when filters change, which transparently triggers a refetch from page 1. No `invalidateQueries` call needed for filter changes.

**Reprocess `succeeded` invalidation.** When `useReprocessJobStatus` transitions to `succeeded`, the controller calls:
```
queryClient.invalidateQueries({queryKey: ["accounts"]})
queryClient.invalidateQueries({queryKey: ["balance"]})  // all balances
queryClient.invalidateQueries({queryKey: ["transactions"]})  // all filter combinations
queryClient.invalidateQueries({queryKey: ["aggregates"]})  // all currency combinations
```
Then `useReprocessStore.clearTracking()` removes the banner and the session-storage entry.

**Tab-focus refetch** is TanStack Query's default `refetchOnWindowFocus: true`. We leave it on globally.

### 2.7 OpenAPI Code Generation

`@hey-api/openapi-ts` runs as a one-shot generator. Configuration lives in `services/frontend/openapi-ts.config.ts`:
- **Input:** two sources — `http://localhost:8000/openapi.json` and `http://localhost:8001/openapi.json` (dev), or local snapshot files (CI). The configuration merges both schemas into one client.
- **Plugins:** `@hey-api/client-fetch` (the runtime client), `@hey-api/sdk` (function-style operations), `@tanstack/react-query` (TanStack Query plugin — generates `queryOptions` factories and `useQuery`/`useMutation` wrappers).
- **Output:** `src/lib/api/generated/`. This directory is **committed to git** (not gitignored) so PRs show schema drift in the diff and so CI doesn't need a running backend to type-check. A separate `make regen-frontend-api` target re-runs the generator against live services.

**CI gate:** the GitHub Actions workflow regenerates the client against the running test backends and fails the build if the committed `generated/` directory diverges. This prevents the frontend from silently drifting from the backend contract.

**Auth-header interceptor.** The generated client accepts a `headers` callback. `lib/api/client.ts` configures it once:
```
client.setConfig({
  baseUrl: '',  // same-origin; rewrites in next.config.ts dispatch
  headers: () => {
    const token = useAuthStore.getState().accessToken
    return token ? { Authorization: `Bearer ${token}` } : {}
  },
  credentials: 'include',  // for the refresh cookie
})
```
The 401 interceptor is registered with `client.interceptors.response.use(...)`.

### 2.8 Component Breakdown — Notes on the Tricky Ones

Most components are mechanical compositions of shadcn primitives. Three need explicit design notes because they encode load-bearing functional-spec logic.

**`ReprocessTriggerButton.tsx` + `useReprocessController.ts`** — implements the §2.7 controller exhaustively. The hook returns `{trigger, isPending, state}`. The button (or any other affordance, e.g. a chart-bar tooltip "Run reprocess" link) calls `trigger()`. The hook:
1. Atomically sets `useReprocessStore.setPending(true)` (which disables every mounted instance of the button — they all subscribe to the same store).
2. Calls the generated `postReprocessUser` mutation.
3. On `202`: stores `{jobId, statusUrl}` in `useReprocessStore.startTracking(...)`; the banner mounts via its subscription to that store.
4. On `409`: parses the `detail` field via `lib/parse-reprocess-detail.ts` (a pure function with a unit test). On parse success, behaves identically to `202`. On parse failure, logs `console.warn({event: 'reprocess_409_parse_failed', ...})` and starts tracking with `statusUrl: null` (banner renders the fallback "refresh the page" copy from §2.7).
5. On `429`: `toast.error(response.detail)` — verbatim from the backend per §2.7 (no client-side parsing).
6. On `502`: `toast.error("Couldn't start reprocess — Kubernetes is unreachable. Retry in a moment.")`.
7. On other 5xx: `toast.error("Couldn't start reprocess — try again.")`.
8. On network error: `toast.error("Network error — couldn't reach the server.")`.
9. Always clears `setPending(false)` in a `finally` so the button re-enables on every outcome that isn't `202` / `409`.

**`AccountTile.tsx` cash-balance walk.** The tile reads `account.source`. For bank accounts, it calls `useAccountBalance(account_id)` which wraps the generated `useListTransactions({account_id, limit: 1})` and returns the first item's `balance_cents`. For cash accounts it calls `useCashBalance(account_id)` which uses TanStack Query's `useInfiniteQuery` against `listTransactions` with `account_id={uuid}&exclude_category=transfer&limit=200`, accumulating `amount_cents * directionSign(direction)` across all pages until `next_cursor === null`.

**`directionSign(direction)`:** `+1` when `direction === 'income'`, `-1` when `direction === 'expense'`, `0` when `direction === 'zero'`. Zero-direction rows (balance-only events) contribute nothing — bank-supplied balance adjustments are bank-account semantics; on cash accounts a `zero` direction would represent a no-op entry and should not move the balance.

**Transfer exclusion.** The walk passes `exclude_category=transfer` so internal-transfer legs (which carry a real `amount_cents` but are *not* income/expense from the household's perspective) don't double-count. Without this, a ₴1000 transfer out of a cash account would subtract ₴1000 from the cash balance, then the matched transfer-in leg on the destination account would also subtract — both sides of an internal move would distort the balance.

Result is memoized in the query cache as `["balance", account_id]`. If any page errors, the hook returns `{ partial: true, balance_cents, error }`; the tile renders the partial sum with the amber "incomplete" indicator and a `<Button>` that calls `refetch()`.

**`DivergingChart.tsx`** — Recharts setup:
- `<ResponsiveContainer>` wrapping `<BarChart data={items} stackOffset="sign">`.
- `<XAxis dataKey="period_start" tickFormatter={shortMonth}>` and `<YAxis>`.
- `<ReferenceLine y={0} stroke="currentColor" strokeOpacity={0.3} />` — the zero line.
- `<Bar dataKey="income_cents" stackId="net" fill="var(--color-chart-income)">` and `<Bar dataKey="expense_cents_negated" stackId="net" fill="var(--color-chart-expense)">`. (Expense is pre-negated client-side before passing to Recharts so it stacks downward.)
- `<Tooltip content={<DivergingChartTooltip />}>` — custom component pulling month label, income, expense, net, and `converted_pct` from the active payload.
- Bar-corner `converted_pct < 100` badge is rendered as a custom `<Cell>` overlay via a `<Customized>` render prop, not as a separate DOM tree — Recharts requires the badge to sit inside the SVG to align with bar geometry.
- For multi-currency view, `DivergingChartSmallMultiples.tsx` renders N independent `<DivergingChart>` instances in a CSS grid (`grid-template-columns: repeat(${N}, minmax(0, 1fr))`), each fed only its own currency's data slice. Y-axes are independent per chart (deliberate — UAH and USD magnitudes aren't comparable).

### 2.9 Testing Strategy

Three tiers. The goal is "catch the things that break in production," not coverage percentage.

**Unit tests (Vitest, `*.test.ts` colocated with source).** Pure-function tests for:
- `lib/parse-reprocess-detail.ts` — canonical format → both fields parsed; missing `Existing job: ` → returns `null`; missing `Poll ` → returns `null`; extra whitespace → tolerated.
- `lib/format.ts` — every money/date/relative-time formatter, including edge cases (zero, negative, very-large numbers, DST boundaries via `@date-fns/tz`).
- `lib/mcc.ts` — known MCCs resolve to labels, unknown returns the code as-is.
- Zustand store actions — auth `setToken`, UI `toggleDirection` produces the right next state.

**Component tests (Vitest + RTL + MSW, `*.test.tsx`).** For each tricky component:
- `ReprocessBanner.test.tsx` — covers all five states: pending, running, succeeded (toast + clear), failed (red banner + retry), 503 backoff. MSW returns synthetic job statuses; `vi.useFakeTimers()` advances the 5s poll interval.
- `FiltersBar.test.tsx` — toggling chips produces the right `useInfiniteTransactions` query keys (including the §2.5.1 "income off + expense off + transfers on → ?direction=zero" edge case).
- `TransactionRow.test.tsx` — drill-down opens, badges render per the response shape, paired-row navigation works.
- `AccountTile.test.tsx` — bank vs cash account derivation, partial-error recovery on the cash walk.
- `DivergingChart.test.tsx` — converted_pct badge appears, single-currency vs small-multiples switching.

**E2E (Playwright, `e2e/smoke.spec.ts`).** One test, one job: catch auth-flow breakage before deploy.
- Boot the full Compose stack (`docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up -d` in CI).
- Wait for `/api/v1/auth/login` to respond.
- POST a seed user via the test-only fixture (already used by API integration tests).
- Drive a real browser: navigate to `/`, get redirected to `/login`, enter credentials, get redirected back to `/`, wait for the feed to render at least one row, click the first row, assert the drill-down opens.

**Visual regression and accessibility audits** are explicitly out-of-scope for Phase 1. A future Storybook + Chromatic pass is fine; we don't need it for ≤10 users.

### 2.10 Configuration Requirements

Environment variables consumed by the frontend container:

| Variable           | Required | Dev value                  | Prod value                          | Purpose                                |
|--------------------|----------|----------------------------|-------------------------------------|----------------------------------------|
| `API_BASE`         | yes      | `http://api:8000`          | `http://api.grosh.svc.cluster.local:8000` | `next.config.ts` rewrites destination for auth/transactions/accounts/settings/rates/admin |
| `INGESTION_BASE`   | yes      | `http://ingestion:8001`    | `http://ingestion.grosh.svc.cluster.local:8001` | Rewrites destination for everything else |
| `NEXT_TELEMETRY_DISABLED` | yes (set to `1`) | `1` | `1` | Disables Vercel's anonymous telemetry — we self-host |

There is **no** `NEXT_PUBLIC_*` variable for backend hostnames. The browser must never see them — they're consumed only inside `next.config.ts` at server start. This keeps backend topology private to the cluster.

### 2.11 Build, Bundle, and Deploy

- `next build` runs with `output: 'standalone'` (already in `next.config.js`) so the Docker image ships the minimal Node runtime + static assets + `server.js`.
- Tailwind v4's Lightning CSS runs at build time; no separate `postcss` step.
- The OpenAPI client generator runs before `next build` in CI (and in `npm run build:full` locally if needed).
- The Docker image stays multi-stage (existing `Dockerfile`): `builder` produces the standalone bundle, `dev` mounts source for hot reload, the final stage runs `node server.js`.
- No service-worker, no PWA manifest, no static export.

---

## 3. Impact and Risk Analysis

### System Dependencies

- **`services/api` OpenAPI** — `GET /v1/openapi.json` must be served at CI time and at `npm run regen` time. Spec 003 §2.10.4 already requires a CI completeness check on this schema.
- **`services/ingestion` OpenAPI** — same. Both services already publish OpenAPI per spec 003's framework choice.
- **Spec 003 endpoint stability** — the generated client mirrors the OpenAPI schema. Any breaking change to a request or response shape requires the frontend to re-generate; CI will catch this as a `generated/` diff.
- **Reverse-proxy contract** — the dev-environment Compose service names (`api`, `ingestion`) and the prod k3s service DNS names must match the `API_BASE` / `INGESTION_BASE` env vars. Wrong values → all API calls 404 with no useful error in the browser console (Next.js logs the rewrite failure server-side).
- **Spec 003 §2.6's `MonobankIntegrationResponse` schema gap** — the integration-health banner from §2.6 of spec 004 is deferred until the schema follow-up lands. Verified in code; tracked in spec 004 §3.

### Potential Risks & Mitigations

| Risk                                                                                  | Likelihood | Impact | Mitigation                                                                                                                                                                                                                              |
|---------------------------------------------------------------------------------------|------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Next.js 14 → 16 upgrade breaks the existing login page                                | Medium     | Medium | The upgrade is the first task of the slice; the login flow is integration-tested by the Playwright smoke before any new code lands. If a Next 16 breaking change is hit, the existing `app/login/page.tsx` is the only file that needs touching. |
| Tailwind v3 → v4 migration leaves orphan `tailwind.config.ts` or `postcss.config.js`  | Low        | Low    | Both files do not currently exist in the scaffold; v4 setup is greenfield. The migration is "add `globals.css` + `@import 'tailwindcss';`" not "rewrite an existing config."                                                            |
| OpenAPI generator produces hooks that don't compose well with Zustand auth interceptor | Medium    | Medium | The interceptor design is verified by a component test before any feature work begins. If `@hey-api/openapi-ts` can't host the interceptor cleanly, fall back to wrapping the generated fetch client in `lib/api/client.ts`.            |
| Recharts diverging-bar custom badge overlay positioning drifts on responsive resize    | Medium     | Low    | The bar-corner badge is implemented via `<Customized>` SVG and unit-tested at three viewport widths. If positioning is fragile, we drop the SVG badge in favor of a CSS-positioned absolute overlay tied to the `ResponsiveContainer`'s reported dimensions. |
| Cash-balance walk on a cash account with 10K+ rows blocks the UI                       | Very low   | Medium | At family scale, this is implausible (spec 004 §2.2 explicitly caps the design at ~2000 rows before re-evaluating). The walk runs in a TanStack Query background fetch (non-blocking); the tile renders an incremental sum while pages load. |
| `sessionStorage` quota exceeded on the reprocess job pointer                           | Very low   | Low    | The pointer is a single object with two strings (~200 B). `sessionStorage` quota is 5 MB+ on every modern browser. If it ever fails, the `persist` middleware catches the quota error and degrades to in-memory tracking; the banner still works for the current tab. |
| Two simultaneous tabs both call `bootstrap()` and both attempt `POST /api/v1/auth/refresh` | Medium     | Low    | The refresh endpoint already rotates tokens server-side; the second request gets a fresh token, the first one's token is invalidated. The auth store's `refreshPromise` (Promise field) dedupes concurrent refresh attempts within a single tab — multiple 401s await the same in-flight refresh. Cross-tab dedup is not needed because each tab maintains its own access token in memory and resolves to the rotated token on its next 401. |
| Build-time OpenAPI fetch fails because the backend isn't running                       | Medium     | Low    | The committed `generated/` directory means `next build` works offline. Only `make regen-frontend-api` needs a running backend. CI runs the regen against ephemeral test backends.                                                       |
| TanStack Query's `refetchOnWindowFocus` thrashes the backend during a long debug session | Low        | Low    | Default behavior is fine for production. Local development can pass `?devOptions` to disable it; not load-bearing.                                                                                                                      |
| Rewrites in `next.config.ts` add ~1ms latency per API call                              | Low        | Low    | Acceptable at family scale. If it ever becomes a problem (which would require thousands of requests/second from a single user — impossible), Traefik can be configured to bypass the Next.js process for `/api/*` paths at the ingress level. |

### Architectural Invariant Compliance

Per CLAUDE.md "Code Organization Principles":

- **Source-specific vs generic separation:** the dashboard is source-agnostic — every component reads from the source-agnostic `transactions` / `accounts` REST responses. No `if source === 'monobank'` branching exists anywhere; source-specific rendering (e.g., showing `cashback_type` only for Monobank) is keyed off the data fields, not the source name.
- **Layer separation:** no SQL in the frontend (trivially), no business logic in components beyond rendering + event handlers. Business logic (the §2.7 controller, the 409 detail parser, the cash-balance walk) lives in `hooks/` and `lib/` — testable in isolation.
- **No raw dicts as DTOs:** every API response goes through generated types; the only `dict`-shaped data in the frontend is the `metadata` field on transactions, which is explicitly opaque per spec 003.
- **Top-level imports only:** ESLint's default rules enforce this.

---

## 4. Testing Strategy (summary)

Already detailed in §2.9. Restated as a checklist:

- [ ] Unit tests (Vitest) for `parse-reprocess-detail.ts`, `format.ts`, `mcc.ts`, all Zustand store actions.
- [ ] Component tests (Vitest + RTL + MSW) for `ReprocessBanner`, `FiltersBar`, `TransactionRow`, `AccountTile`, `DivergingChart`.
- [ ] One Playwright E2E smoke covering login → feed → drill-down.
- [ ] CI gate: `generated/` directory diff against a freshly regenerated client fails the build.
- [ ] Visual regression, full E2E coverage, and accessibility audits are out-of-scope for Phase 1.
