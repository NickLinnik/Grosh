# Functional Specification: Frontend (Phase 1)

- **Roadmap Item:** Phase 1 Frontend — Dashboard, Accounts, Transactions, Settings, Admin
- **Status:** Draft
- **Author:** Nick
- **Dependency:** Spec 003 — Transaction Ingestion Pipeline. Spec 004 consumes the `/v1/*` endpoints spec 003 ships **plus** a small set of backend additions defined in §10 (manual transaction PATCH/DELETE, account rename PATCH, user display-name PATCH, integration-list schema and query change, anomaly read endpoint). These additions are part of the Phase 1 delivery — the frontend is not considered functionally complete without them. Their contracts are defined in §10 so both frontend and backend implementation slices can proceed from the same source of truth.

---

## 1. Overview and Rationale

The ingestion pipeline (spec 003) loads transactions, classifies them as internal transfers, computes UAH/USD/EUR equivalents on write, and exposes paginated reads + flexible aggregates. None of that is visible without a UI. Spec 004 delivers the entire Phase 1 frontend surface: four user-facing routes for any authenticated user, plus two admin routes. Manual transaction CRUD lives inside `/accounts` (per-account); reprocess and rates-backfill operations live inside `/admin/operations`.

The dashboard's job is to answer *"how am I doing financially"* in under a second on the morning glance — and to support deeper exploration by changing a filter without ever leaving the page. We replace the clustered-bars-plus-trend-line shape with a **diverging bar chart** (income above zero, expense below zero) — bar asymmetry encodes savings directly without a separate trend series. A rolling savings total is shown as a big-number tile next to the chart.

Page-level global filters are the load-bearing UX choice. Every page that shows data (dashboard, `/accounts`) carries its own filter bar in URL query parameters; every component on the page (tiles, chart, KPI, feed widget) recomputes when the filter changes. This makes the dashboard not just a summary but a slice-and-dice surface that the same user uses both for "this morning's quick look" (default filter) and "what did I spend in groceries last quarter" (filter applied). The constructor (Phase 2+, see §10) eventually lets users compose their own widget layouts; the global-filter model already accommodates it.

The pipeline's data-quality signals (`converted_pct < 100%`, reprocess running, unresolved transfer anomalies, Monobank integration off) all need surfaces — silently dropping rows or pausing event flow without telling the user is a trust killer. Phase 1 covers each with a small banner or badge, not a full audit UI. All four surfaces are in-scope; the two that need new backend paths (anomaly badges via §10.5, integration-health banner via §10.4) are covered by the §10 additions.

**Success criteria:**

- The top nav consistently shows `Dashboard`, `Accounts`, `Settings` for every authenticated user; admins additionally see `Admin` opening to `Users` and `Operations`. Non-admin direct access to `/admin/*` returns a 403 page.
- On first load to `/`, the user sees current balances per active account (tiles), the rolling-12-month diverging chart, the savings KPI, and a small feed-widget at the bottom showing the rows that compose the displayed metrics — all over the page's default filter (last 12 months, all accounts, all directions, all currencies, all categories).
- Changing any page-level filter on `/` (date range, accounts, directions, currencies, categories) recomputes the chart, KPI, feed widget, and the relevant tiles within the same render cycle.
- `/accounts` opens with the integration→account tree on the left and the universal cross-account feed on the right. The user can multi-select tree nodes; selection and the right-pane's `account` filter chips are two views of the same state — changing one updates the other.
- Manual transactions are created, edited, and soft-deleted from within the right pane of `/accounts` when a manual account is the active selection.
- When a reprocess job is running for the current user, a top-of-page banner says "Reprocessing — data may be incomplete" and the data widgets show subtle loading states. The banner disappears within 5 s of terminal status.
- When any of the user's Monobank integrations has `status != 'active'` or `webhook_registered=false`, a red integration-health banner mounts in the App Shell on every route and stays there until the user re-links (§5.5).
- Feed rows referenced by unresolved `transfer_match_anomalies` rows carry an amber `unpaired` badge whose tooltip explains the anomaly type in plain language (§4.4.3).
- Users can rename any of their accounts (manual or bank) from the active-account header on `/accounts` (§4.6) and edit their own display name from `/settings` (§5.1).
- All admin mutations (create user, soft-delete user, force bulk reprocess, trigger rates backfill) require an explicit confirmation step. Self-demotion (admin demoting themselves out of admin role) and self-delete are both forbidden at the UI level.
- The OpenAPI-generated TypeScript SDK is the only source of response-model types the frontend uses; CI fails on any hand-typed copies.

---

## 2. Application Shell

The frontend is a Next.js App Router client-side SPA (see `context/spec/004-phase1-frontend/technical-considerations.md` §2.5 for the rendering-model decision). All authenticated routes render inside a shared **App Shell** that owns the top navigation, the integration-health placeholder, the reprocess-in-progress banner, and the global toast container.

### 2.1 Top Navigation

A persistent horizontal nav bar at the top of every authenticated page:

```
+------------------------------------------------------------------+
| Grosh   [Dashboard]  [Accounts]  [Settings]   [Admin▾]   [User▾]|
+------------------------------------------------------------------+
```

- **Acceptance Criteria:**
  - [ ] Left side: Grosh wordmark (plain text, per `context/product/design-system.md` §8). Clicking it navigates to `/`.
  - [ ] Center / left-aligned: nav items in fixed order — `Dashboard` (`/`), `Accounts` (`/accounts`), `Settings` (`/settings`). The current route's item is visually marked as active (filled chip background using `--color-brand-50`, text color `--color-brand-800`, per design system §2.5).
  - [ ] **Admin menu** appears only for users whose `currentUser.role === 'admin'`. When shown, it sits between `Settings` and the user menu, rendered as a button with a chevron that opens a dropdown menu containing `Users` (`/admin/users`) and `Operations` (`/admin/operations`).
  - [ ] Right side: user menu — a button labeled with the current user's `display_name`, opens a dropdown with `Profile` (links to `/settings`), `Logout`. The avatar/initials affordance is deferred to a later phase (no `avatar_url` field today).
  - [ ] On mobile (<768 px), the top nav collapses behind a hamburger button. The Grosh wordmark stays visible; the menu items are revealed in a vertical drawer when tapped.
  - [ ] Non-admin users navigating directly to any `/admin/*` URL receive a 403 page (see §6.3). They never see the Admin menu in the nav.

### 2.2 App Shell Banners

The App Shell owns two persistent banner slots that render above the route content on every authenticated route. Individual routes never mount their own top-level banners; anything that should be visible across routes lives here.

- **Reprocess-in-progress banner** — mounted when there is an active reprocess job tracked in `useReprocessStore`. Full state machine in §7.2.
- **Integration-health banner** — mounted when any of the user's Monobank integrations has `status != 'active' OR webhook_registered = false`. Full contract in §5.5.

When both banners apply simultaneously, they stack: integration-health first (top, red), then reprocess (below, neutral). Rationale: an integration-health failure is a "you should act on this" signal that should have higher visual priority than "we're processing in the background."

### 2.3 Global Toast Container

Sonner's `<Toaster />` is mounted once at the root level. All toasts (success, error, info, warning) appear in the top-right corner. Toast lifetimes are 4 s default, 10 s for warnings, manually-dismissable for errors. Multiple toasts stack vertically, newest on top.

### 2.4 Routes

| Route                   | Owner              | Authentication | Description                                                                                          |
|-------------------------|--------------------|----------------|------------------------------------------------------------------------------------------------------|
| `/login`                | spec 002           | public         | Already implemented; restyled against the design system in this slice.                              |
| `/`                     | this spec §3       | any user       | Dashboard — page-level filters + tiles + chart + KPI + feed widget.                                  |
| `/accounts`             | this spec §4       | any user       | Two-pane: integration→account tree (left) + universal feed with filter bar (right). Account CRUD.    |
| `/settings`             | this spec §5       | any user       | Preferences and integration management (Monobank token, webhook health, account-level deletion).    |
| `/admin/users`          | this spec §6.1     | admin only     | User list, create, soft-delete.                                                                      |
| `/admin/operations`     | this spec §6.2     | admin only     | Bulk reprocess + rates-backfill triggers and recent-job status.                                      |

Any URL outside this set redirects authenticated users to `/`. The auth-guard layer is owned by spec 002 and is not re-specced here.

---

## 3. Dashboard (`/`)

The dashboard is a summary-on-top, evidence-at-the-bottom layout that responds to page-level filters. Top-down on desktop, single-column on mobile:

```
+----------------------------------------------------------+
| [Banner row: reprocess-in-progress only, conditional]    |
+----------------------------------------------------------+
| [Page-level filter bar — §3.1]                           |
+----------------------------------------------------------+
| [Account tiles strip — §3.2]                             |
+----------------------------------------------------------+
| [Rolling 12-month chart + Savings KPI tile — §3.3, §3.4] |
+----------------------------------------------------------+
| [Feed widget — §3.5]                                     |
+----------------------------------------------------------+
```

- **Acceptance Criteria:**
  - [ ] Banner row is hidden when no banners apply (no extra vertical space). Two banners may appear here (from the App Shell — §2.2): the integration-health banner (§5.5) and the reprocess-in-progress banner (§7.2). When both apply, integration-health stacks on top.
  - [ ] All page sections render in the order shown above on desktop. On mobile (<768 px), the chart and KPI tile stack vertically; everything else flows top-to-bottom in the same order.
  - [ ] **Rendering model: client-side SPA.** The dashboard route is a Next.js App Router page whose tree starts with a `'use client'` boundary; no server-side data fetching, no SSR, no hydration handshake. The server ships a minimal HTML shell + the JS bundle; the browser then runs the auth check, fires the page's TanStack Query fetches in parallel (accounts, aggregates, first page of transactions for the feed widget), and replaces the per-surface skeletons as each query resolves. Rationale: the audience is ≤10 authenticated household users on warm browser caches — SSR's first-paint benefit is invisible at this scale, while its tax (hydration mismatches around timezone/locale/auth state, `'use client'` boundary hunting, two execution environments) is real every day. The App Router is used for routing, layouts, and code-splitting only.

### 3.1 Page-Level Filter Bar

The dashboard's filter bar is the user's primary tool for slicing the displayed data. Every component below the filter bar (tiles, chart, KPI, feed widget) recomputes whenever any filter changes.

Filter state lives in URL query parameters so the entire filtered view is bookmarkable and shareable. Examples:

- `/` — default filter (last 12 months, no other filters set).
- `/?from=2026-05-01&to=2026-06-01&direction=expense` — expenses in May 2026 only.
- `/?from=2026-01-01&account=<uuid1>&account=<uuid2>` — year-to-date across two specific accounts.

#### 3.1.1 Available Filters

The filter bar exposes five filter dimensions. Each dimension's UI is described in §3.1.2.

| Dimension     | Query param(s)                    | Default                          | Backend mapping                                                    |
|---------------|-----------------------------------|----------------------------------|--------------------------------------------------------------------|
| Date range    | `from` (inclusive), `to` (exclusive) | Last 12 months, ending in current month (user's timezone) | `GET /v1/transactions?from=...&to=...` and `GET /v1/transactions/aggregates?from=...&to=...` |
| Accounts      | `account` (repeated UUID)         | unset = all accounts             | `GET /v1/transactions?account_id=...` (single-account, repeated when multi-select; see §3.1.3 on multi-account semantics) |
| Directions    | `direction` (repeated enum: `income`, `expense`, `zero`) | unset = all directions           | `GET /v1/transactions?direction=...` (per spec 003 §2.6, only single-value supported today; see §3.1.3) |
| Currencies    | `currency` (repeated enum: `UAH`, `USD`, `EUR`)         | unset = `UAH` only on chart; feed widget always shows native original currency per row | `GET /v1/transactions/aggregates?currency=...` |
| Special categories shown | `category` / `exclude_category` (repeated enum) | unset = `exclude_category=transfer` (hide internal transfers from feed and aggregates) | `GET /v1/transactions?exclude_category=...` |

#### 3.1.2 Filter Bar UI

The filter bar sits sticky at the top of the dashboard content area (below the App Shell's nav and banner row). Compact horizontal layout on desktop; collapses to a "Filters (3 active)" pill on mobile that opens a sheet.

- **Acceptance Criteria:**
  - [ ] **Date range** is a single button labeled with the active range (e.g., "Last 12 months" or "May 1 – Jun 1, 2026"). Clicking it opens a date-range picker popover with preset shortcuts (`Today`, `This month`, `Last 30 days`, `Last 3 months`, `Last 12 months` (default), `Year to date`, `Custom...`) and a calendar for custom selection. The custom selection enforces `from < to`. The picker also offers a "Clear" button that resets to the default (last 12 months).
  - [ ] **Accounts** is a multi-select dropdown labeled with the selected count (e.g., "Accounts: All" when unset, "Accounts: Monobank Black" when one selected, "Accounts: 3" when multiple). Opens a popover with a checkable list of the user's `is_active=true` accounts, grouped by integration (see `/accounts` tree structure in §4.1). A "Clear" / "Select all" pair sits at the top of the popover. Deactivated accounts are not shown.
  - [ ] **Directions** is a 3-chip toggle row (`Income`, `Expenses`, `Transfers`). The user clicks chips to include/exclude each direction. Default = all three on (visually "filled"). Including `Transfers` toggles the `exclude_category=transfer` query param: when the chip is on, transfers are visible (no `exclude_category`); when the chip is off, the request includes `exclude_category=transfer`. The other two chips drive `direction`: see §3.1.3 for the mapping rules.
  - [ ] **Currencies** is a 3-chip toggle row (`UAH`, `USD`, `EUR`) that lives near the chart, NOT in the global filter bar — see §3.3 acceptance criteria. The currency chips control the chart only (small-multiples behavior). Other widgets (tiles, feed widget) always show their own per-row original currency or the display-currency selected in §3.6.
  - [ ] **Categories** filter is reserved for Phase 2 — only `transfer` exists today (per spec 003 §2.4). Phase 1 ships only the `Transfers` toggle described above; no other category UI.
  - [ ] **Active-filter summary chip** sits to the right of the filter controls when 1+ filters differ from default: e.g., "3 filters active". Clicking it opens a popover listing each active filter as a removable chip (`Date: May 1–Jun 1 ×`, `Account: Monobank Black ×`). A "Reset all" button at the bottom clears every filter back to defaults.
  - [ ] On mobile (<768 px), the entire filter bar collapses into a single "Filters" button that opens a bottom sheet containing every control vertically stacked. The active-filter summary chip is replaced by a count badge on the button (`Filters · 3`).
  - [ ] All filter changes update the URL via `router.replace()` (no history pollution) and invalidate the relevant TanStack Query keys. Filter state is hydrated from the URL on initial mount so a deep link arrives in the correct state.

#### 3.1.3 Filter Semantics — Edge Cases

Several filter combinations require explicit semantics because the backend's request parameters don't map 1:1 onto the UI.

- **Directions / Transfers chips ↔ `direction` + `category` / `exclude_category` params.** The three chips drive two orthogonal request axes: `direction` (single-value, `income`/`expense`) controls Income and Expenses; `category` (whitelist) vs `exclude_category` (blacklist) controls Transfers (`special_category='transfer'`). Translation table for the 7 valid chip states (all-three-off is special-cased):

  | Income | Expenses | Transfers | Request params                                       | Meaning                                |
  |--------|----------|-----------|-----------------------------------------------------|----------------------------------------|
  | on     | on       | on        | (no direction, no category filter)                  | Show everything                        |
  | on     | on       | off       | `exclude_category=transfer`                         | Hide transfers; show income + expense  |
  | on     | off      | on        | `direction=income` (no category filter)             | Income rows; both ordinary and transfer-tagged income legs |
  | on     | off      | off       | `direction=income&exclude_category=transfer`        | Ordinary income only (no transfer legs)|
  | off    | on       | on        | `direction=expense` (no category filter)            | Expense rows; both ordinary and transfer-tagged expense legs |
  | off    | on       | off       | `direction=expense&exclude_category=transfer`       | Ordinary expense only (no transfer legs)|
  | off    | off      | on        | `category=transfer`                                 | Transfers only (whitelist)             |
  | off    | off      | off       | (no request fired; empty state in widgets)          | "No directions selected — pick at least one to see data." |

  Note: spec 003 §2.4 explains that transfer legs are still directionally `income` or `expense` — `special_category='transfer'` is orthogonal to `direction`. The mappings above respect that orthogonality. Balance-only bank events (`direction=zero` per spec 003) are NOT exposed via the chips in Phase 1; they're an internal pipeline signal and a future power-user filter could surface them via a separate control.
- **Accounts multi-select:** the backend's `GET /v1/transactions` accepts `account_id` as a single UUID query parameter only (per spec 003 §2.6). When the user selects multiple accounts, the frontend issues **N parallel requests** (one per account, all with the same date / direction / category filters) — but the pagination story differs from single-account selection. Single-account selection uses TanStack Query's `useInfiniteQuery` with the backend's per-account cursor. Multi-account selection cannot merge N independent cursors cleanly, so Phase 1 takes a different approach:
  - For multi-account selection, the frontend issues **one bounded request per account** with `limit=200` (the server cap) and **no cursor**, expecting up to 200 newest rows per account. It then merges, sorts by `(time DESC, id DESC)`, and renders the resulting in-memory list with **client-side virtual scrolling** (no further server requests). The merged list shows the union of the most-recent ~200 rows per account.
  - At the bottom of the merged list (after the last in-memory row), the feed shows a footer: "Showing the most recent 200 rows per account. To see older history, select a single account from the tree." This is honest about the limitation without breaking the multi-account exploration flow.
  - If any per-account response would return more than 200 rows (the response includes `next_cursor != null`), the footer also includes a chip per such account: "Older history available in Account X — open just that account to scroll further." Clicking the chip narrows the tree selection to that account, which switches the feed to single-account infinite-scroll mode.
  - For the aggregates endpoint, the backend also accepts only one `account_id` filter — the frontend fetches per-account aggregates in parallel (one request per account) and sums them client-side per bucket per currency. Aggregates aren't paginated, so this is clean.
  - This is an acceptable Phase 1 trade-off because the household has ≤10 accounts and the multi-account flow is exploratory, not exhaustive. If it ever feels limiting, a backend update to accept repeated `account_id` query params (returning a single merged cursor) is a small change.
  - **Partial-failure handling:** if any of the N parallel per-account requests fails (5xx, network error, 401, or otherwise), the feed renders the union of the *successful* responses, plus a per-failed-account warning chip above the feed: `"Couldn't load {account_name} — Retry"`. The chip's `Retry` button refires only that one account's request and merges its result in on success. The aggregates query uses the same pattern: failed accounts' contributions are excluded from the chart/KPI/tile sums, and the page shows a warning banner naming the affected accounts.
- **Date range "all time":** if the user picks a custom range that extends back further than transaction history, the backend still returns the available rows. The chart's bucket count adapts to the requested range — `(to - from)` divided into 12 buckets (so a 12-month range = monthly buckets; a 3-month range = ~weekly buckets; a 365-day "custom 1 year" range = monthly buckets). The bucket size for the chart is computed client-side from the requested range and passed to the aggregates endpoint as the `bucket` parameter (see §3.3).
- **Filter combinations that yield zero rows:** every widget renders its own empty state ("No data matches these filters"). The active-filter summary chip stays visible so the user can clear filters.

### 3.2 Account Overview Tiles

A horizontal strip of tiles, one per active account the user owns, that respects the page-level filter — but with one specific exception (current balance is filter-independent).

- **Acceptance Criteria:**
  - [ ] **Tiles are filter-RESPECTING for "Last activity" but filter-INDEPENDENT for "Current balance".** Rationale: the current balance is a snapshot fact ("how much is in this account *right now*") — filtering by date range to "May 2024" should not show "what the balance was in May 2024". The "Last activity" timestamp DOES respect the date filter (it shows the most recent transaction within the filtered window).
  - [ ] **Tile selection narrows the page filter.** Clicking a tile adds that account to the page's `account` filter (or removes it if already selected — toggle). Multiple tile selections accumulate (the page filter takes multiple `account` values). This is the same multi-select model as the accounts dropdown in §3.1.2.
  - [ ] Tile layout (left-to-right inside the tile): account icon (debit_card / credit_card / fop / cash variant), account name (or masked_pan if name is null), currency code, large balance number, "Last activity: {relative time}" muted text below.
  - [ ] **Bank-account balance** is `balance_cents` from the latest transaction on the account, where "latest" is the row with the greatest `(time, id)` tuple — `time DESC, id DESC`. UUIDv7 `id`s are time-ordered so they break `time` ties deterministically (matches the cursor sort used everywhere else in spec 003). Fetched via `GET /v1/transactions?account_id={uuid}&limit=1` — independent of the page's `from`/`to` filter.
  - [ ] **Cash-account balance** is the sum of `amount_cents * direction_sign` across **all** transactions for that account, regardless of feed pagination AND regardless of the page's date filter. The frontend issues a dedicated paginated walk of `GET /v1/transactions?account_id={uuid}&exclude_category=transfer&limit=200` for cash accounts, iterating until `next_cursor=null`. Result is cached in TanStack Query keyed by `["balance", account_id]` with the same 60s stale time as accounts. Cash accounts in a single household are not expected to exceed a few hundred lifetime entries; if a cash account grows beyond ~10 pages (2000 rows) the spec author will reconsider and add a server-side balance endpoint.
  - [ ] If the balance walk hits an error mid-iteration, the tile shows the partial sum with an amber "incomplete" indicator and a retry control. The chart and feed are unaffected.
  - [ ] Accounts with `is_active=false` are not shown in the tile strip. Filter is client-side (the backend returns them per spec 003 §2.5).
  - [ ] When a user has zero active accounts, the strip is replaced with a single empty-state card linking to `/accounts` ("No accounts yet — connect Monobank or add a cash account").
  - [ ] If `GET /v1/accounts` returns an error, the strip renders a single retry tile spanning the row, not a blank space. The chart and feed still load.
  - [ ] Tile order: bank accounts (Monobank) first, sorted by `currency_code` then by `created_at` ascending; manual cash accounts last. Deterministic to avoid layout jitter across refreshes.
  - [ ] Selected tiles (matching the page's `account` filter) render with a visual selected state — e.g., a `--color-brand-500` outer ring and `--color-brand-50` background. Unselected tiles remain in default styling.

### 3.3 Rolling Monthly Diverging Chart

A diverging bar chart spanning the filtered date range, with N buckets adaptively chosen.

- **Acceptance Criteria:**
  - [ ] Chart fetches data over the page's `from`/`to` filter range, with the bucket interpolated to yield ~10-14 visible bars: ranges ≤ 90 days bucket to weeks; ranges 90 days – 18 months bucket to months; longer ranges bucket to quarters. The `bucket` parameter is passed to `GET /v1/transactions/aggregates`.
  - [ ] Default currency is `UAH`. A multi-select chip row above the chart lets the user toggle USD and EUR. When more than one chip is selected, the chart renders as **small multiples** (one stacked diverging chart per currency, equal height, same x-axis), not as overlaid bars on shared axes (the y-scales are not comparable across currencies).
  - [ ] The chip row never lets the user deselect all three; the last active chip is disabled.
  - [ ] **No bucket switcher** in Phase 1 beyond the adaptive bucket derived from the page date filter. Direct bucket override (e.g., "force daily on a 30-day range") is deferred.
  - [ ] **No intra-series intensity scaling, no per-bar color ramp.** Bar height alone encodes magnitude. Each income bar uses the same green; each expense bar uses the same red. (Double-encoding height with color is an anti-pattern at low bar density.)
  - [ ] Internal transfers are excluded from chart buckets by default (`exclude_category=transfer` per the page's category filter). If the user explicitly toggles `Transfers` on in the directions chips, transfers DO appear in the chart buckets, rendered as **grey bars on a separate stack offset from the zero line** — visually distinct from the green income / red expense bars. The transfer bars sit below the expense bars on the negative side (or above income on the positive side, depending on the transfer leg's direction), so they're clearly "other money movement" without competing with the savings narrative. A small legend appears below the chart when transfer bars are visible: "Transfers shown separately (grey)." We deliberately do NOT omit transfers from the chart when the chip is on — that would break the chart-as-evidence-for-the-feed promise (the feed shows transfer rows; the chart must reflect them in some form).
  - [ ] Hovering a bar shows a tooltip with: bucket label, total income (formatted with currency symbol and grouping), total expense, net savings (income − expense), and the bucket's `converted_pct` if `< 100%`.
  - [ ] When `converted_pct < 100%` for a rendered currency, the corresponding bar shows a small amber "!" badge in its upper-right corner. Tooltip explains: "{N} of {M} transactions in {bucket} have no {currency} rate available; their values are not included. [Run reprocess]". The link triggers the §7.1 reprocess flow.
  - [ ] **Multi-currency badge scoping.** When the chart renders as small multiples (multiple currency chips selected), each small-multiple chart computes its own `converted_pct` badges from its own currency's response — a bad UAH bucket in the UAH chart does not put a badge on the USD chart. The KPI tile's badge follows the KPI's scoping rule (§3.4): it only reflects the first-selected currency.
  - [ ] If the aggregates query returns zero items (no transactions in the filtered range), the chart renders an empty state: "No transactions match these filters. Try clearing the filters or check that you've imported transactions."
  - [ ] Bars are rendered with Recharts `BarChart` using two stacked `Bar` series (income positive, expense negated to draw downward). The shared y-axis is symmetric around zero. The x-axis labels show bucket labels: short month names (e.g., "Jan", "Feb") for month buckets; ISO-week labels ("W23") for week buckets; quarter labels ("Q2'25") for quarter buckets. Months in years other than the current year show the year suffix ("Jan '25").

### 3.4 Savings KPI Tile

A single tile placed beside (desktop) or below (mobile) the chart. Shows the sum of `delta_cents` across all buckets in the filtered range in the chart's primary currency.

- **Acceptance Criteria:**
  - [ ] Tile shows: a label that names the filtered date range (e.g., "Last 12 months" when default, "May 2026" when filtered to that month, "Year to date" when YTD preset is active, or "May 1 – Jun 1, 2026" for arbitrary custom ranges), large net savings number with currency symbol, a small sparkline beneath showing the running cumulative savings across the buckets in the range.
  - [ ] **KPI delta respects the Transfers chip** the same way the chart does. When `Transfers` is off (default), the KPI's `delta_cents` sums only ordinary income/expense buckets — transfers are excluded. When `Transfers` is on, the KPI includes transfer flows in the delta so the number matches the chart's visible bars. The sparkline follows the same rule. The label does not change between the two modes, but the number does — there is no visible "with transfers" badge because the chip state above is the visible affordance that explains the change.
  - [ ] Positive net savings render in green text; negative in red. Zero renders in neutral.
  - [ ] If the chart is rendering small multiples (multiple currencies selected), the KPI tile shows only the **first** selected currency. The remaining currencies are visible inside their own small-multiple charts; we do not stack multiple KPI tiles.
  - [ ] If any bucket in the filtered range has `converted_pct < 100%` for the displayed currency, the KPI shows a small amber "!" icon next to the number with the same tooltip mechanism as the chart bars.

### 3.5 Dashboard Feed Widget (Evidence Panel)

A compact transaction-list widget below the chart that shows the rows composing the page's currently filtered data. The widget is the "evidence panel" for the metrics above — clicking a chart bar (Phase 2 enhancement) could narrow the feed widget further, and the widget's row contents change as filters change.

- **Acceptance Criteria:**
  - [ ] Widget shows the **first 30 rows** matching the page filter, sorted by `(time DESC, id DESC)`. Below those rows, a "See {N} matching transactions in /accounts →" link navigates to `/accounts` with the same page filter pre-applied (the `/accounts` filter is independent state, so this is a one-shot copy on click, not bidirectional sync).
  - [ ] The widget's row design is identical to the `/accounts` feed row design (§4.4.2) for visual consistency. Drill-down behavior is identical (§4.4.4).
  - [ ] The widget has **no filter controls of its own.** It inherits the page-level filter entirely. Trying to filter "differently than the page" is the explicit non-goal — that's what `/accounts` is for. The widget exists to show *evidence*, not to be a parallel exploration tool.
  - [ ] When the filtered result is empty, the widget collapses to a single line: "No transactions match these filters." The "See in /accounts →" link is omitted.
  - [ ] The widget's pagination is fixed at 30 rows — no infinite scroll on the dashboard. This is deliberate: dashboards should not require scrolling thousands of rows. Deep exploration belongs in `/accounts`.

### 3.6 Display-Currency Selector

A dashboard-level dropdown control that lives near the user menu (top-right corner of the page-level filter bar). Affects how converted amounts are shown in the feed widget AND in the `/accounts` feed (see §4.4). It is NOT the same as the chart currency chips — display currency is what each row's *secondary* line shows; chart currency is what dimension(s) the chart renders.

- **Acceptance Criteria:**
  - [ ] Dropdown labeled "Display: [UAH | USD | EUR]" with `UAH` as default.
  - [ ] Setting persists across pages within the session via Zustand (`useUiStore`). It is NOT a per-page URL filter; it's a frontend-only display preference.
  - [ ] On change, all feed rows currently visible (on the dashboard widget AND in `/accounts` if mounted) re-render with the new display currency on their secondary line. No backend request — the data already includes UAH/USD/EUR amounts per row from spec 003.

---

## 4. Accounts (`/accounts`)

Two-pane surface: integration→account tree on the left, universal transactions feed on the right. The tree's selection state and the right pane's `account` filter are **two views of the same model** — changing one updates the other.

`/accounts` is where account-level operations live: rename a cash account, soft-delete an account, trigger a Monobank backfill, paste a new Monobank token. These are contextual actions on tree nodes, not pages of their own.

### 4.1 Tree Sidebar

The left pane is a vertical tree of integrations and their accounts.

```
+-----------------------------------+
| Search accounts...                |
|-----------------------------------|
| ▾ Monobank                        |
|     Monobank Black (UAH)          |
|     Monobank FOP USD              |
| ▾ Manual                          |
|     Cash on hand                  |
|                                   |
|   + Add cash account              |
|   + Connect Monobank              |
+-----------------------------------+
```

- **Acceptance Criteria:**
  - [ ] The tree shows two top-level groups: **Monobank** (children = Monobank integration accounts) and **Manual** (children = cash accounts). The Monobank group appears only if the user has at least one Monobank integration with `status='active'`; otherwise the group is hidden. The Manual group always appears, even when empty.
  - [ ] **Selection is multi-select.** Single click on an account toggles its membership in the selection. Single click on the integration group (e.g., "Monobank") toggles ALL accounts under that group. Tri-state checkboxes appear on hover or on focus: empty (none selected), partial (some children selected), full (all children selected).
  - [ ] **Two-way sync** with the right pane's filter:
    - When the user changes the selection in the tree, the right pane's `account` filter chips update (§4.3.1) and the URL's `account` query params update.
    - When the user changes the `account` filter chips in the right pane (e.g., clicks the X on a filter chip), the tree's selection updates.
    - When no accounts are selected, the right pane shows the universal cross-account feed (filter `account` is empty). When all accounts are selected, behavior is identical (and the tree visually shows the integration groups as "fully selected").
  - [ ] **Search input** at the top of the tree filters the visible tree nodes by account name and integration name (substring, case-insensitive). Selection state survives search filtering — accounts hidden by the search remain in the active selection but aren't visible until the search clears.
  - [ ] **Contextual actions per tree node.** Right-clicking a tree node (or clicking a kebab `⋮` button that appears on hover/focus) opens a context menu with node-appropriate actions:
    - For a Monobank integration root: `View integration health` (shows a small popover with `status`, `webhook_registered`, last webhook timestamp), `Delete integration` (with confirmation). (Token-less webhook re-registration is still deferred to Phase 2 per §11 — it requires a backend endpoint outside the §10 scope; users re-register by re-linking in Settings §5.3.)
    - For a Monobank account: `Backfill transactions...` (opens a date-range picker → submits to `POST /v1/monobank/accounts/{id}/backfill`), `View backfill history` (lists recent job statuses from session storage).
    - For a manual cash account: `Rename...`, `Soft-delete account` (with confirmation).
  - [ ] **"+ Add cash account"** below the Manual group opens a small inline form: name (required), currency (dropdown: UAH/USD/EUR/etc., default UAH). On submit → `POST /v1/manual/accounts` with `type: "cash"`. On success, the new account appears in the tree and is auto-selected.
  - [ ] **"+ Connect Monobank"** at the bottom of the tree (or under the Monobank group if no integrations exist yet) opens a sheet/modal containing the Monobank token-paste flow — see §5.3.
  - [ ] When the page first mounts with no `account` query params in the URL, the tree shows no selection and the right pane shows the universal feed.
  - [ ] When the page mounts with `account=<uuid>` in the URL, the tree pre-selects those accounts and the right pane filters accordingly.
  - [ ] The tree is scrollable independently of the right pane on desktop. On mobile, the tree collapses to a "Filter by account ▾" button at the top of the right pane content; tapping it opens a sheet containing the tree.

### 4.2 Right Pane — Layout

The right pane fills the rest of the viewport and contains, top to bottom:

1. **Active-account header** — when one account is selected, shows account name, balance, integration health (for bank accounts), and an action row. When multiple or zero accounts are selected, shows a summary header instead (e.g., "All accounts" or "3 accounts selected").
2. **Filter bar** — page-level filter bar identical in shape to §3.1.2, with `account` filter chips reflecting the tree selection (and these chips ARE editable from the right pane — see §4.3.1).
3. **Feed** — infinite-scroll transaction list per §4.4.

### 4.3 Right Pane — Filter Bar

Same filter dimensions as §3.1.1 — date, accounts, directions, currencies (not surfaced on `/accounts`; only used by the chart, which isn't here), category.

#### 4.3.1 Account Filter Chips and the Tree

The accounts dimension is represented on `/accounts` in two ways simultaneously:
- The tree on the left (selection state).
- A row of filter chips at the top of the right pane (e.g., `Account: Monobank Black ×`, `Account: Cash on hand ×`, `+ More`).

- **Acceptance Criteria:**
  - [ ] Both views are bound to the same Zustand selector (the `account` filter array). Edits to either propagate.
  - [ ] The `+ More` chip opens a popover identical to the dashboard's accounts dropdown (§3.1.2). Selecting accounts there adds them to the filter; the tree visualization updates.
  - [ ] When zero accounts are selected, the filter shows a single chip: `Accounts: All`. Clicking it opens the multi-select popover to add accounts.

### 4.4 Right Pane — Transaction Feed

A combined, chronological list of transactions across the currently selected accounts (or all accounts when none are selected). Infinite scroll.

Data: `GET /v1/transactions` with query parameters driven by the filter bar (subject to the multi-account semantics in §3.1.3).

#### 4.4.1 Pagination

- **Acceptance Criteria:**
  - [ ] Infinite scroll, not numbered pages. Uses TanStack Query's `useInfiniteQuery` with the spec 003 `CursorPage` envelope. The next page is fetched when the user scrolls within ~600px of the current bottom.
  - [ ] First page request size is 50; subsequent pages also 50. Maximum is the server's `limit=200` cap.
  - [ ] When the server returns `next_cursor=null`, the feed shows a "No more transactions" sentinel and stops fetching.
  - [ ] On any feed query error, the next-page sentinel becomes a "Retry" button. Already-fetched pages stay visible; the user can still scroll through what loaded.

#### 4.4.2 Row Layout

Each transaction renders as a single row with a 3 px left border colored by `direction`. Text colors stay at normal foreground/muted to satisfy WCAG contrast (no red text on amount — color sits on the border, not the type).

```
| date         | description           |       primary amount  |
|              | account · indicators  |  display-currency · @rate
```

- **Acceptance Criteria:**
  - [ ] Left border: green for `direction=income`, red for `direction=expense`, grey for `special_category=transfer`. `direction=zero` (balance-only events from the bank) renders with a neutral grey border, identical to transfers (this matches their "neither income nor expense" semantics).
  - [ ] Date column shows day + short month (e.g., "24 Jun") on its own line; year is shown only when the row is from a year other than the current.
  - [ ] Description column line 1: `description` truncated to one line with ellipsis. Line 2: account display name in muted color (mirrors what the tile shows), followed by inline indicator badges (see §4.4.3).
  - [ ] Amount column line 1 (right-aligned, larger): original amount = `operation_amount_cents` formatted with `operation_currency_code` symbol if non-null, otherwise `amount_cents` formatted with `currency_code`.
  - [ ] Amount column line 2 (right-aligned, smaller, muted): the converted amount in the currently selected **display currency** (§3.6) (`amount_uah_cents`, `amount_usd_cents`, or `amount_eur_cents`). Hidden when the display currency equals the row's original currency (no point repeating ₴100 as "₴100").
  - [ ] Amount column line 3 (right-aligned, smallest, muted): the effective rate `original / converted` shown as `@ {rate}`. Shown only when (a) display-currency line is shown AND (b) the conversion required a real rate (i.e., `metadata.layer.conversion.effective_rate` exists or the ratio is non-unit).
  - [ ] If the converted amount for the selected display currency is **null** (no rate available), line 2 shows "(no rate)" in muted text and line 3 is omitted. The row also gets the `converted_pct` badge described in §4.4.3.

#### 4.4.3 Inline Row Indicators

Compact badges that sit at the end of the description line 2 (after account name). Each is a 4-6 character pill.

- **Acceptance Criteria:**
  - [ ] **`PENDING`** pill (amber) when `hold=true`. Always shown when true regardless of other badges. (Holds are confusing and need to be obvious — they may disappear in later reprocesses.)
  - [ ] **`↔ paired`** badge when `special_category='transfer'` AND `related_transaction_id IS NOT NULL`. Tooltip names the paired account. Clicking the badge scrolls to the paired row in the feed if it is currently rendered; otherwise opens the row's drill-down and offers a "Jump to paired" action.
  - [ ] **`no rate`** badge (amber) when the row's `{display_currency}_cents` amount is null. Clicking opens a tooltip: "This transaction has no {currency} rate stored. Run reprocess after rates backfill to refresh." Same trigger as the chart-bar badge.
  - [ ] **`MANUAL`** badge (neutral) when `origin='manual'`. Disambiguates manually-entered cash transactions from bank events.
  - [ ] Indicators are read-only in the feed row. Acting on them happens in the drill-down or via the banner-level reprocess control.

#### 4.4.4 Row Drill-Down

Clicking a row (anywhere except a badge with its own action) expands it inline, pushing the rows below it down. One drill-down at a time; opening another collapses the first.

- **Acceptance Criteria:**
  - [ ] Drill-down shows: full description, MCC code + a human label resolved client-side from a static ISO 18245 table, cashback amount with currency, counterparty IBAN if present, transaction ID (short UUID prefix, click-to-copy), the full `metadata.layer.conversion.path` (formatted as a chain like "UAH → USD via monobank @ 38.42 (FRESH)"), and a "View paired" link if `related_transaction_id` is present.
  - [ ] **Manual rows (`origin='manual'`) additionally show `Edit` and `Delete` buttons in the drill-down.** `Edit` opens the same form as manual transaction creation (§4.5), pre-populated with the row's current values, and submits `PATCH /v1/manual/transactions/{id}` (defined in §10.1). On success, the row updates in place with the returned values; the drill-down stays open. `Delete` opens a confirmation dialog ("Delete this transaction? This can't be undone.") and on confirm submits `DELETE /v1/manual/transactions/{id}` (defined in §10.1). On success the drill-down collapses and the row is removed from the feed in place. Both actions invalidate the cash-balance walk cache for the row's account (`["balance", account_id]`) so the tile refreshes.
  - [ ] **`unpaired` badge** (amber dot) renders on a row when the row's `id` appears in the "unresolved anomalies" set fetched from `GET /v1/transactions/anomalies?status=unresolved` (defined in §10.5). Tooltip on hover explains the anomaly `reason_code` in plain language: `unpaired_from_description` → "This looks like a transfer from another account, but the matching leg wasn't found"; `unpaired_to_description` → "This looks like a transfer to another account, but the matching leg wasn't found"; `ambiguous_pair_match` → "Multiple possible transfer partners found — can't tell which is the pair"; `description_account_mismatch` → "Description names an account that isn't the actual partner"; `description_consistency_mismatch` → "The transfer legs' descriptions disagree." Clicking the badge opens the drill-down with the anomaly details expanded at the top.
  - [ ] **Drill-down persistence rules:** drill-down state survives infinite-scroll page appends (the next page mutates the cached list in place via `useInfiniteQuery` rather than invalidating the cache, so the open row stays open as new rows append below). Drill-down collapses on: (a) filter or account-dropdown changes, (b) display-currency changes, (c) reprocess-completed cache invalidation, (d) the user opening a different row's drill-down (one open at a time). It does not collapse when the user simply scrolls.

### 4.5 Manual Transaction Entry

When a manual cash account is the only selected account in the tree, the right pane's header includes a **"+ Add transaction"** button. Clicking it opens an inline sheet (slides up from the bottom on mobile; modal on desktop) with the manual transaction creation form.

- **Acceptance Criteria:**
  - [ ] The "+ Add transaction" button appears ONLY when exactly one account is selected AND that account is a manual cash account (`source='manual', type='cash'`).
  - [ ] Form fields: amount (positive number with two-decimal precision), direction (radio: `Income` / `Expense` — `Zero` is not exposed in the UI per spec 003 §2.5), date (defaults to today; date+time picker), description (free-text required), MCC code (optional, autocomplete from the static ISO 18245 list).
  - [ ] On submit → `POST /v1/manual/transactions` with the account_id of the selected account. On success → close the sheet, the feed prepends the new row, the account tile's balance refetches.
  - [ ] On 422 from the backend → form fields highlight their respective errors per `validation_errors`.
  - [ ] Edit and delete of an existing manual transaction happen from the drill-down (§4.4.4). Edit reuses the same form component with all creation fields editable (amount, direction, time, description, mcc). Delete is a soft removal per the backend's semantics (see §10.1); the row disappears from the feed and any cached aggregates recompute on next fetch.

### 4.6 Active-Account Header

When the tree has zero, one, or many accounts selected, the right pane's header above the filter bar adapts:

- **Zero accounts selected:** header shows "All accounts", muted total balance row across currencies (e.g., "Total: ₴62,318 · $1,247"), and no per-account actions.
- **One account selected:** header shows account name, the account's current balance prominently, integration health badge (for bank accounts, driven by the fields defined in §10.4), and an action row: `Rename` (both manual AND bank accounts — via `PATCH /v1/accounts/{id}` defined in §10.2), `Backfill...` (Monobank only), `Soft-delete account` (manual or Monobank).
- **Multiple accounts selected:** header shows "N accounts selected", a summary balance (sum across same-currency accounts; mixed-currency selections show a per-currency breakdown), and no per-account actions.

These header behaviors are visual conveniences; the canonical state lives in the tree selection / filter chips.

---

## 5. Settings (`/settings`)

Single-page route for user preferences and integration management. No tabs needed — the screen is small.

```
+----------------------------------------------------------+
| [Banner row]                                             |
+----------------------------------------------------------+
| Profile                                                  |
|   Display name: Nick              [Edit]                 |
|   Email: nick@...                 (read-only)            |
|   Logout from this device         [Logout]               |
|   Logout from all devices         [Logout all]           |
|----------------------------------------------------------|
| Preferences                                              |
|   Timezone: Europe/Kyiv (auto-detected)  [Change]        |
|   Default display currency: UAH          [UAH ▾]         |
|   Default rate source: monobank          [monobank ▾]    |
|----------------------------------------------------------|
| Monobank integration                                     |
|   Status: connected · webhook active                     |
|   [Re-link / rotate token]   [Disconnect integration]    |
|   (or, if not connected: [Connect Monobank account])     |
+----------------------------------------------------------+
```

### 5.1 Profile Section

- **Acceptance Criteria:**
  - [ ] Shows `display_name` from `GET /v1/auth/me` with an "Edit" button. Clicking `Edit` reveals an inline text input pre-filled with the current name and a `Save`/`Cancel` pair. `Save` submits `PATCH /v1/users/me` (defined in §10.3) with the new name; on 200 the header updates in place, on 422 the input shows the field-level validation error. `Cancel` reverts the input without submitting.
  - [ ] Shows `email` from `GET /v1/auth/me`, read-only (email change is admin-only via DB or future endpoint).
  - [ ] "Logout" button calls `POST /v1/auth/logout` (current device only) and then runs the full logout teardown contract (technical-considerations §2.5).
  - [ ] "Logout from all devices" button calls `POST /v1/auth/logout-all` and then runs the same teardown. Confirmation dialog before action: "Logging out everywhere will end every active session. You'll need to log in again here."

### 5.2 Preferences Section

- **Acceptance Criteria:**
  - [ ] Timezone shows the persisted `user_settings.timezone` value. A "Change" button opens a dropdown of IANA timezones (filterable input). On selection → `PUT /v1/settings` with the new timezone. On success → the chart refetches (timezone affects bucket boundaries).
  - [ ] Default display currency dropdown (`UAH | USD | EUR`). Persists to `useUiStore` (no backend field today — spec 003 doesn't store this on `user_settings`; it's a frontend-only preference for Phase 1).
  - [ ] Default rate source dropdown — populated from the values supported by the backend's `rate_source_config` table (currently `monobank`, `nbu`). On change → `PUT /v1/settings` with `default_rate_source`. On 422 → toast error.

### 5.3 Monobank Integration Section

This section consolidates the Monobank linking flow. The same flow is reachable from `/accounts` (via the "+ Connect Monobank" affordance in the tree); both points open the same sheet.

- **Acceptance Criteria:**
  - [ ] Reads from `GET /v1/monobank/integrations`. If empty → shows the "Connect Monobank account" button. If non-empty → shows status row(s) per integration.
  - [ ] **Status row per integration** shows: `monobank_client_id` (truncated), creation date, and an action row: `Re-link / rotate token` (opens the linking sheet with the integration prefilled for token rotation), `Disconnect integration` (confirmation → `DELETE /v1/monobank/integrations/{id}`).
  - [ ] **Linking sheet flow:**
    1. User pastes their Monobank personal API token.
    2. Frontend calls `POST /v1/monobank/link` with the token.
    3. On 422 `MONOBANK_TOKEN_INVALID` → inline error: "This token was rejected by Monobank. Check that you copied it correctly."
    4. On 502 `MONOBANK_API_UNAVAILABLE` → error banner: "Monobank is unreachable. Try again in a moment."
    5. On 409 `INTEGRATION_ALREADY_LINKED` → "You already have a Monobank integration for a different account. Disconnect it first."
    6. On 201 or 200 → sheet shows the list of accounts returned (`accounts: [{account_id, external_account_id, currency_code, was_rebound}]`) for confirmation. The user confirms; the sheet closes; tiles in dashboard and tree in `/accounts` refresh.
    7. If `webhook_registered: false` in the response, a non-blocking warning toast appears: "Live transaction sync is paused — retry the connection from the Monobank integration row." (Phase 1 doesn't have a banner for this; see §5.5 / §10.)
  - [ ] After successful linking, the sheet offers a prompt: "Import recent transactions?" with a button that opens the backfill date-range picker (§4.1 contextual action). Skipping it is allowed; the user can trigger backfill later from the tree.

### 5.4 Account Management

Account-level actions (rename cash, soft-delete) live in `/accounts` (§4.6), not here. This section explicitly does not duplicate them. A cross-link sentence at the bottom of the page reads: "Manage individual accounts (rename, delete) from the Accounts page."

### 5.5 Integration-Health Banner

A red banner mounted in the App Shell that surfaces Monobank integration failure (token rejected, webhook de-registered). The banner is the primary Phase 1 surface for the silent-data-loss failure mode where transactions stop flowing but the user doesn't realize it. Data source is `GET /v1/monobank/integrations`, which returns `status` and `webhook_registered` per integration (contract defined in §10.4).

- **Acceptance Criteria:**
  - [ ] The banner renders in the App Shell above the route content whenever the user has any Monobank integration where `status != 'active' OR webhook_registered = false`. It appears on every route (dashboard, /accounts, /settings, /admin/*) until the condition clears.
  - [ ] The banner's message follows this exhaustive matrix on the `(status, webhook_registered)` tuple per integration; when the user has multiple integrations, only the first offending one is surfaced in the banner and a "(+ N other integrations affected)" suffix is appended:

    | status | webhook_registered | Message                                                              | CTA                                    |
    |--------|--------------------|----------------------------------------------------------------------|----------------------------------------|
    | active | true               | (banner hidden)                                                      | —                                      |
    | active | false              | "Monobank webhook isn't registered — new transactions won't arrive." | `Reconnect` → Settings §5.3 link flow. |
    | error  | true               | "Monobank rejected your access token — new transactions won't arrive." | `Reconnect` → Settings §5.3 link flow. |
    | error  | false              | "Monobank integration is broken — new transactions won't arrive."    | `Reconnect` → Settings §5.3 link flow. |

  - [ ] The banner is not user-dismissable — the only way it disappears is by successfully re-linking the integration in Settings §5.3. This is deliberate: an integration-health banner that the user can hide silently defeats its purpose.
  - [ ] Clicking `Reconnect` navigates to `/settings` and scrolls to §5.3 with the affected integration row highlighted.
  - [ ] The banner uses `--color-expense-soft` for the background and `--color-expense` for the left border, matching the visual language of failure banners elsewhere.
  - [ ] The list query `GET /v1/monobank/integrations` returns ALL of the caller's integrations regardless of `status` value (contract update in §10.4) — otherwise the banner would never see error-state rows.

---

## 6. Admin (`/admin/*`)

Admin routes are gated to users with `currentUser.role === 'admin'`. Non-admin access returns a 403 page.

### 6.1 User Management (`/admin/users`)

A single page for managing the user list.

```
+---------------------------------------------------------------+
| Users                                          [+ Create user]|
+---------------------------------------------------------------+
| Email           | Name    | Role   | Created    | Last active | Actions |
|-----------------|---------|--------|------------|-------------|---------|
| nick@...        | Nick    | admin  | 2026-01-04 | 2 min ago   |   —     |
| alice@...       | Alice   | member | 2026-02-12 | yesterday   |   ⋮     |
| bob@...         | Bob     | member | 2026-04-20 | never       |   ⋮     |
+---------------------------------------------------------------+
| ◀ Prev   Page 1 of 1   Next ▶                                 |
+---------------------------------------------------------------+
```

- **Acceptance Criteria:**
  - [ ] Loads `GET /v1/admin/users` with cursor pagination (default `limit=50`).
  - [ ] Columns: `email`, `display_name`, `role`, `created_at` (formatted as `YYYY-MM-DD`, hover for full ISO timestamp), `last_active_at` (relative: "2 min ago", "yesterday", "3 weeks ago"; renders as "never" when null), `actions` (kebab menu).
  - [ ] **Actions menu per row** contains: `Soft-delete user`. **The current admin's own row does not show the actions menu** (matches the backend's 400 `Cannot delete your own account.` per spec 003 §2.6.2).
  - [ ] **Soft-delete dialog** explains: "Deactivating {email} sets `is_active=false`, revokes their refresh tokens, and instantly logs them out of any active sessions. Their transactions and accounts remain in the database but become inaccessible to them. This is reversible only by direct database change. Proceed?" On confirm → `DELETE /v1/admin/users/{id}` → on 204 → row disappears from the list, toast "User deactivated."
  - [ ] **"+ Create user"** button opens a sheet:
    - Fields: `email` (required, validated client-side as RFC-compliant), `password` (required, with show/hide toggle), `display_name` (required), `role` (radio: `member` (default) / `admin`).
    - Submit → `POST /v1/admin/users` → on 201 → close sheet, refresh list, toast "User {email} created."
    - On 409 `VALIDATION_ERROR` (duplicate email) → inline error on the email field: "A user with this email already exists."
    - On 422 → field-level validation errors per `validation_errors` (e.g., malformed email).
  - [ ] Pagination uses the same `CursorPage` Prev/Next pattern as the rest of the app.
  - [ ] **Self-demotion prevention:** the create-user form's `role` toggle is a setter for new users only. There is NO "edit user role" affordance in Phase 1 — changing an existing user's role requires a direct database change. Specifically, this prevents an admin from accidentally demoting themselves out of admin role in the UI.

### 6.2 Operations (`/admin/operations`)

A single page with two operation cards: bulk reprocess and rates backfill.

```
+---------------------------------------------------------------+
| Operations                                                    |
+---------------------------------------------------------------+
| Bulk reprocess                                                |
|   Users:    [Select users... ▾] (default: all snapshot)       |
|   Force:    [ ] Bypass per-user 1/hour rate limit             |
|             [Trigger reprocess]                               |
|                                                               |
|   Recent jobs (this session):                                 |
|     job-abc123 · 2 min ago · running    [View status ▾]       |
|     job-xyz456 · 1 hr ago · succeeded   [View status ▾]       |
|---------------------------------------------------------------|
| Rates backfill                                                |
|   Source:   [nbu ▾] (monobank disabled — see why)             |
|   From:     [2026-05-01]  To: [2026-06-01]                    |
|             [Trigger rates backfill]                          |
|                                                               |
|   Recent jobs (this session):                                 |
|     job-def789 · 5 min ago · succeeded  [View status ▾]       |
+---------------------------------------------------------------+
```

#### 6.2.1 Bulk Reprocess Card

- **Acceptance Criteria:**
  - [ ] **User selector**: multi-select dropdown listing the user list from `GET /v1/admin/users` (paginated under the hood; the dropdown shows the first page and offers a search input that hits the API). A "Select all" button puts every user in the selection. Empty selection means "send `user_ids: null` to the backend, which means 'all users at snapshot time' per spec 003 §2.9." This is a deliberate choice — the explicit-empty case maps to the backend's "all users" snapshot.
  - [ ] **Force toggle**: a checkbox labeled "Bypass per-user 1/hour rate limit." Defaults off. When toggled on, a small warning text appears: "This bypasses the 1/hour rate limit for the selected users. Use sparingly — repeated triggers consume backend resources."
  - [ ] **Trigger button** → confirmation dialog → `POST /v1/admin/reprocess` with body `{user_ids: [...] | null, force: bool}` → on 202:
    - If `job_id` is non-null: a new row appears in the "Recent jobs" list with status `pending`. The frontend stores `{job_id, status_url}` in localStorage keyed by `grosh:admin:reprocess-jobs` and begins polling the job's `status_url`.
    - Toast summary: "Reprocess triggered for {N} users. Skipped: {alice@... (rate-limited), bob@... (locked)}." Skipped reasons come from the `skipped: [{user_id, reason}]` array in the response. **Skipped-user emails are resolved client-side** by joining each `user_id` against the cached user list from `GET /v1/admin/users`. If a user_id is not in the cache (e.g., the cache holds only the first paginated page and a skipped user is on a later page), the toast shows the truncated user_id (`12345678…`) instead of the email, with the same reason annotation.
    - If `job_id` is null (every user was skipped): no row in the list. Toast: "All users were skipped — no job was started. Reasons: ..."
  - [ ] **Recent jobs list**: shows up to 10 most recent admin reprocess jobs from localStorage. Each row: `job-id (truncated) · started_at (relative) · status (badge: pending / running / succeeded / failed)`. Clicking "View status ▾" expands the row inline to show the full `JobStatusResponse` (pods counts, failure_reason if any, completed_at). Polling for non-terminal jobs continues at 5 s intervals; same exponential-backoff-on-503 rules as the per-user reprocess banner (§7.2).
  - [ ] **Cross-session visibility is Phase 2** — the recent-jobs list only contains jobs the current browser triggered. Jobs triggered from another device or in another browser session are invisible. This matches the per-user reprocess banner's design (technical-considerations §2.7).

#### 6.2.2 Rates Backfill Card

- **Acceptance Criteria:**
  - [ ] **Source dropdown**: shows the list of rate sources supported by the backend (`monobank`, `nbu`). Monobank is disabled in Phase 1 because the Monobank API doesn't expose historical rates (per spec 003 §2.8); the disabled option carries a tooltip explaining why.
  - [ ] **From/To date pickers**: required. Default: 30 days ago to today. The picker enforces `from <= to`.
  - [ ] **Trigger button** → confirmation dialog → `POST /v1/admin/rates-backfill` with `{source, from_date, to_date}` → on 202:
    - Row in the "Recent jobs" list with the job_id, source, date range, and status `pending`.
    - Toast: "Rates backfill triggered for {source}, {from}–{to}."
    - Frontend polls the `status_url`; same rules as bulk reprocess.
  - [ ] **Recent jobs list**: same pattern as bulk reprocess, keyed separately in localStorage under `grosh:admin:rates-backfill-jobs`.

### 6.3 Non-Admin Access

- **Acceptance Criteria:**
  - [ ] Any direct navigation to `/admin/*` by a user whose `currentUser.role !== 'admin'` renders a 403 page: a centered card saying "You don't have permission to view this page" with a button "Back to dashboard" linking to `/`.
  - [ ] The 403 page is rendered client-side after the auth-guard layer confirms the user's role. The check uses `useAuthStore` — no backend round-trip needed since the user object is already cached.
  - [ ] If the user's role changes mid-session from admin to member (e.g., another admin demoted them in the DB), the next admin page they visit returns 403 from the backend; the frontend translates that to the same 403 page. (This is an edge case — role demotion mid-session is not a Phase 1 supported flow.)

---

## 7. Cross-Cutting Behaviors

### 7.1 Reprocess Trigger Controller

The reprocess trigger is invoked from multiple affordances (§3.3 chart-bar `converted_pct` tooltip, §4.4.3 row-level `no rate` tooltip, §7.2 banner Retry button, §6.2.1 admin bulk-trigger). All routes share a single client-side controller with the same behavior:

- **Acceptance Criteria:**
  - [ ] On click, the control immediately enters a `pending` state — visibly disabled (60 % opacity, `cursor: not-allowed`, `aria-disabled='true'`), with the label switched to "Starting…". A second click while in `pending` is a no-op. The control stays `pending` until the trigger response resolves (any HTTP status, success or error, or a network error) — it never times out the disabled state on its own.
  - [ ] On 202 Accepted, the controller stores `{job_id, status_url}` from the response body in session storage (per-user trigger) or localStorage (admin bulk trigger), transitions to `polling` state, and the relevant banner appears within one render tick.
  - [ ] On 409 `REPROCESS_LOCKED` (per-user trigger only), the controller parses the existing `job_id` and `status_url` from the response's `detail` string. The expected format is exactly `"Reprocessing already in progress for user {user_id}. Existing job: {job_id}. Poll {status_url} for progress."` (per spec 003 §2.9). The parser extracts `status_url` as the substring between `"Poll "` and `" for progress."` and `job_id` as the substring between `"Existing job: "` and `". Poll "`. If parsing succeeds, the controller behaves identically to a 202 (store + poll + banner).
  - [ ] **409 parser graceful degradation.** If parsing fails (e.g., the backend detail format drifts), the controller logs a `console.warn` with payload `{event: "reprocess_409_parse_failed", detail: "<truncated to 200 chars>", expected_format_hash: "<hash of the format string above>"}` and falls back to showing the banner in a "Reprocess already running — refresh the page in a few minutes" state with no `status_url` to poll and no error toast. A unit test asserts both branches by stubbing the 409 response with (a) the canonical format and (b) a deliberately-broken payload, then verifying (a) starts polling and (b) emits the warn log and renders the fallback banner.
  - [ ] On 429 `RATE_LIMITED`, the controller shows a one-shot toast that displays the response's `detail` string verbatim (e.g., "Reprocess already used this hour — try again at 2026-06-24T15:30:00Z"). No client-side parsing of the timestamp is required — the backend's human-readable message is shown as-is, sidestepping any reliance on spec 003's exact detail format. No banner appears.
  - [ ] On 502 `JOB_SUBMISSION_FAILED`, toast: "Couldn't start reprocess — Kubernetes is unreachable. Retry in a moment." The control exits `pending` and the user can click again.
  - [ ] On 5xx other than 502, toast: "Couldn't start reprocess — try again." The control exits `pending`.
  - [ ] On network error (no response), toast: "Network error — couldn't reach the server." The control exits `pending`.

### 7.2 Reprocess-In-Progress Banner

A neutral (blue/grey) banner that renders above the route content when the current user has an active reprocess job. Mandated by spec 003 §2.9 acceptance criterion "During reprocessing, the user sees a 'refreshing' indicator (data is temporarily incomplete)."

Data source: a frontend-only poll loop that activates only after a reprocess trigger from this same browser. Phase 1 does not introduce a "list active jobs for current user" endpoint; instead, the frontend retains the `status_url` returned from any trigger it performed and polls it.

- **Acceptance Criteria:**
  - [ ] When a reprocess job is triggered from the UI (via the §7.1 controller), the frontend stores the `job_id` + `status_url` in browser session storage and begins polling `GET {status_url}` every 5 seconds.
  - [ ] While the job is in `pending` or `running`, a banner shows: "Reprocessing your transactions — chart and feed may show incomplete data."
  - [ ] On `succeeded`, the banner is removed, the chart and feed cache are invalidated (TanStack Query `invalidateQueries` on `["accounts"]`, `["balance"]`, `["transactions"]`, `["aggregates"]`), and a one-shot success toast appears: "Reprocess complete."
  - [ ] On `failed`, the banner becomes red and shows the `failure_reason` field plus a "Retry" button that calls `POST /v1/users/{user_id}/reprocess` again. The retry honors the 1/hour rate-limit; if 429 comes back, the banner shows the "next eligible at" detail from the response.
  - [ ] On `JOB_STATUS_UNAVAILABLE` (503), the frontend retries with exponential backoff (1s, 2s, 4s, 8s, capped at 60s). After 5 consecutive 503s, polling stops and the banner becomes "Reprocess status temporarily unavailable" with a manual "Check again" button. (Mirrors the spec 003 §2.3 frontend guidance.)
  - [ ] **Halt-counter persistence:** the consecutive-503 counter is in-memory only — it is NOT written to session storage. On page reload while in the halted state, polling resumes from a clean counter (next attempt is 1 second). Clicking "Check again" also resets the counter to zero. This is intentional: a transient K8s outage that triggered the halt is most likely resolved by the time the user reloads, and forcing a fresh retry budget is the right default at family scale.
  - [ ] On `JOB_NOT_FOUND` (404, K8s GC'd the job after 3600s), the banner is removed and the cache is invalidated as if the job had succeeded. (Job state we did not observe in time — the safe assumption is "it finished and we missed the terminal status.")
  - [ ] If the user reloads the page while a job is mid-flight, the session-storage entry is rehydrated and polling resumes. If the entry is missing (browser session ended), no banner is shown — the user can't see in-flight jobs they didn't trigger from this session. Cross-session visibility is Phase 2.

### 7.3 Data Fetching and Caching

Across all surfaces, frontend data access follows the same rules.

- **Acceptance Criteria:**
  - [ ] All API calls use a single typed client generated from `services/api` and `services/ingestion` OpenAPI documents. The frontend never declares response types by hand. (Spec 003 §2.10.4 already guarantees the OpenAPI schema is complete.)
  - [ ] All queries use TanStack Query. Query keys are stable tuples — see technical-considerations §2.6 for the canonical list.
  - [ ] Default stale time: 60 seconds for accounts/integrations, 30 seconds for aggregates, 0 for the transaction feed (always background-revalidates on focus). These can be tuned later; documented here so the choices aren't accidental.
  - [ ] On 401 from any endpoint, the frontend invokes the refresh flow once (`POST /v1/auth/refresh`); on second 401 within the same request lifecycle, runs the full logout teardown (technical-considerations §2.5) and redirects to the login page.
  - [ ] On 5xx other than `JOB_STATUS_UNAVAILABLE` (which has its own backoff in §7.2), TanStack Query's default retry policy applies: 3 retries with exponential delay, then surface the error UI for that component.
  - [ ] When the user is on a tab and it regains focus, the per-account feed and accounts queries refetch automatically. The dashboard's aggregates query does not (its stale time is 30s; explicit refresh comes from chart-currency-chip changes or page-filter changes).

### 7.4 Formatting and Localization

- **Acceptance Criteria:**
  - [ ] All monetary amounts are rendered with `Intl.NumberFormat` using the user's browser locale and the relevant currency code. Cents are converted to major units by dividing by 100 at render time; no float math is stored.
  - [ ] All timestamps are rendered in the user's timezone from `GET /v1/settings.timezone`.
  - [ ] **Timezone bootstrap (client-only — no SSR involved).** The bootstrap fires **once per browser-tab session**, inside the auth store's post-login completion path (not on every dashboard mount). On the first dashboard render after login, the bootstrap (a) calls `GET /v1/settings`, (b) reads `Intl.DateTimeFormat().resolvedOptions().timeZone` for the browser's IANA zone, and (c) if the persisted `timezone` equals the default `"UTC"` AND the browser zone differs, issues a one-shot `PUT /v1/settings` with the browser zone before the aggregates query is allowed to run. The aggregates query is gated behind a TanStack Query `enabled: settingsResolved` flag so the chart never fetches with the wrong timezone — the user sees a chart skeleton for the extra ~150ms instead of a UTC-bucketed flash that then re-buckets. If `GET /v1/settings` returns a non-default timezone or matches the browser zone, no PUT is issued and the aggregates query proceeds immediately. Subsequent dashboard mounts in the same browser-tab session do NOT re-issue the GET or PUT — the session-scoped guard in the auth store ensures one execution per tab.
  - [ ] **Bootstrap timeout / failure handling.** The 5-second timeout is a **single total budget** that covers the GET and (if it fires) the PUT combined. Timer starts the instant the bootstrap effect begins issuing `GET /v1/settings`; whichever request is still in flight when the timer elapses gets its `AbortController` signalled and the bootstrap resolves immediately with `settingsResolved = true` using the browser-detected IANA zone. Any error response from the GET or PUT short-circuits to the same fallback before the timer expires. The PUT-to-persist is retried in the background on the next session bootstrap.
  - [ ] Relative times ("3 minutes ago", "yesterday") use `Intl.RelativeTimeFormat`. Anything older than 7 days renders as an absolute date.
  - [ ] No multi-language UI in Phase 1 (per roadmap Phase 4); all chrome is in English.

### 7.5 Empty, Loading, and Error States

- **Acceptance Criteria:**
  - [ ] Each independent surface (tiles, chart, KPI, feed, tree) renders its own loading skeleton — failures in one do not block the others. Skeletons match the final layout's vertical rhythm so no layout shift occurs on resolution.
  - [ ] Empty states have explicit copy and a primary action: account tiles → "No accounts yet — connect Monobank or add a cash account" (links to `/accounts`); chart → "No transactions match these filters"; feed → "No transactions match these filters — clear filters".
  - [ ] Error states show a one-line message plus a "Retry" button bound to the relevant query's `refetch`. Generic 5xx says "Couldn't load — try again."; 401 redirects (per §7.3); other typed errors (per spec 003 §2.10.2 `code` field) get specific copy: e.g. `MONOBANK_API_UNAVAILABLE` → "Monobank is unreachable; live transactions will resume automatically when it recovers."

---

## 8. Authentication and Authorization

The auth layer is owned by spec 002; this section enumerates how spec 004 consumes it.

- **Acceptance Criteria:**
  - [ ] Every authenticated route mounts inside an `<AuthGuard>` that redirects to `/login` when there is no valid access token (or refresh fails).
  - [ ] Admin routes (`/admin/*`) additionally check `currentUser.role === 'admin'`; non-admin access returns the 403 page from §6.3.
  - [ ] On logout from anywhere, the full logout teardown contract (technical-considerations §2.5) runs: clearTracking → setToken null + setRefreshPromise null → queryClient.clear → POST /v1/auth/logout → redirect.

---

## 9. Visual Design Reference

Spec 004 implementation refers to:
- `context/product/design-system.md` for tokens (colors, radii, type, spacing, motion).
- `context/product/mockups/mood-financial-serious.html` for the locked visual mood.
- `context/product/mockups/BRIEF.md` for the Claude Design canvas brief (used at design-frame time, not implementation time).

The frontend's `services/frontend/src/styles/globals.css` is the canonical Tailwind v4 `@theme {}` mirror of the design system. Any visual decision not covered here defers to the design system; any conflict between this spec and the design system is a spec bug to be reconciled in favor of the design system.

---

## 10. Backend Additions Required

Spec 003 defined the ingestion, storage, and read-side APIs Grosh ships today. Spec 004's frontend needs five small additions to reach functional completeness for Phase 1. Each is scoped, contract-defined here, and consumed by the sections listed. These additions are Phase 1 in-scope work — not "nice to have later" — because the frontend is not considered done without them.

All additions are constructed to align with existing spec 003 conventions: `/v1/` prefix, RFC 7807 error envelope (§2.10 of spec 003), RLS-enforced on user-scoped tables (§2.11 of spec 003), OpenAPI-typed request/response models (§2.10.4 of spec 003).

### 10.1 Manual Transaction PATCH / DELETE

**Consumed by:** §4.4.4 (drill-down Edit/Delete), §4.5 (edit form reuse).

Two endpoints on the ingestion service, complementing the existing `POST /v1/manual/transactions` from spec 003 §2.5.

- **`PATCH /v1/manual/transactions/{transaction_id}`** — edit a manual transaction the caller owns.
  - Path param: `transaction_id: UUID` — the target row's `transactions.id`.
  - Request body: `ManualTransactionUpdate` with all creation fields, all optional (only supplied fields are updated):
    - `amount_cents: int | None` (must be positive; direction sign is separate)
    - `direction: Literal["income", "expense"] | None` (same narrowing as `POST`; `zero` is not exposed here)
    - `time: datetime | None`
    - `description: str | None`
    - `mcc: str | None` (nullable — passing `null` explicitly clears the mcc, omitting the field leaves it untouched)
  - Response: 200 with `ManualTransactionResponse` (same shape as `POST` — the updated row).
  - Authorization: the transaction must be owned by the caller (verified via `transactions.user_id = app.current_user_id()`) AND must have `origin = 'manual'`. Bank-sourced rows (`origin = 'bank'`) return 403 with `code: INSUFFICIENT_PERMISSIONS` — the UI never shows Edit for bank rows so this is defensive against tampered requests. Rows owned by another user return 404 with `code: TRANSACTION_NOT_FOUND` (no existence leak per the IDOR-defense pattern spec 003 §2.10.3 uses).
  - Idempotency: PATCH with the same body is idempotent. No conditional-request headers (`If-Match`) are required at family scale; last-write-wins is acceptable.
  - Side effects: the row's `updated_at` bumps. If `time` or `amount_cents` changed, the row's converted amounts (`amount_uah_cents` etc.) are re-computed at write time using the same rate resolution as manual create (spec 003 §2.5) — so an edit to `time` that crosses a rate boundary can change the converted amounts. The `metadata.layer.conversion.path` is regenerated to reflect the new resolution.
  - **Transfer-pair severance on invalidating edit.** If the row is one leg of a transfer pair (non-null `related_transaction_id`) AND the edit changes `amount_cents` or `time` such that the pair-match conditions from spec 003 §2.4.1 no longer hold (same-currency amount cross-match within ±2s), the pair is severed inside the same DB transaction: both legs' `related_transaction_id` and `special_category` are set to NULL. If either leg's `description` matches the transfer-phrase family (`З %` / `На %`), a `transfer_match_anomalies` row of type `unpaired_from_description` or `unpaired_to_description` is inserted for that leg (same rule as DELETE below). Edits that preserve pair validity (e.g., a description-only change, or an amount edit that still satisfies the ±2s cross-match) leave the pair intact.
  - Validation errors: 422 with `code: VALIDATION_ERROR` and per-field `validation_errors` (Pydantic v2 shape).
  - Republish to Redpanda: **no.** The row is already persisted and normalized; PATCH is a DB-level update. This is the same rule spec 003 uses for the enrichment layer's writes on `transactions` — only fresh events go through the pipeline.

- **`DELETE /v1/manual/transactions/{transaction_id}`** — remove a manual transaction the caller owns.
  - Path param: same as PATCH.
  - Request body: none.
  - Response: 204 No Content.
  - Authorization: same as PATCH.
  - Deletion is **hard, not soft.** Spec 003 uses soft-delete for accounts (long-lived, referenced by historical transactions), but individual transactions are leaf rows — nothing FKs to them once the transfer-pair relation is severed. Hard delete keeps the row set clean and mirrors what "delete" means to the user.
  - **Atomic cascade.** The DELETE endpoint executes the following inside a single DB transaction, in order:
    1. Hard-delete the target row.
    2. If the target was paired (non-null `related_transaction_id`), set the partner row's `related_transaction_id = NULL` and `special_category = NULL`, converting it back into an ordinary transaction.
    3. If (and only if) step 2 ran AND the partner's `description` matches the transfer-phrase family (`З %` / `На %`), insert a `transfer_match_anomalies` row for the partner with `reason_code = unpaired_from_description` or `unpaired_to_description` (whichever matches the partner's phrase side). If an anomaly row for the partner + reason_code already exists, the insert is idempotent (`ON CONFLICT DO NOTHING`).
    4. FK `ON DELETE CASCADE` on `transaction_embeddings` (Phase 2 vector store, pgvector) removes any embedding row referencing the deleted transaction.
    5. Return 204 No Content once the transaction commits. Any error during steps 1–4 rolls back and returns 5xx per the RFC 7807 envelope — the user's `Delete` action is atomic from their perspective.

### 10.2 Account Rename PATCH

**Consumed by:** §4.6 (active-account header `Rename` action for both manual AND bank accounts).

- **`PATCH /v1/accounts/{account_id}`** — rename any account the caller owns.
  - Path param: `account_id: UUID`.
  - Request body: `AccountUpdate` — `{name: str}` (required; max 100 chars; whitespace-trimmed server-side; empty string after trim → 422).
  - Response: 200 with `AccountResponse` — same shape as `GET /v1/accounts/{id}` returns today.
  - Authorization: `accounts.user_id = app.current_user_id()`. Not-owned → 404 `ACCOUNT_NOT_FOUND`.
  - Works for both `source='manual'` AND `source='monobank'` accounts. This is the reason this endpoint exists as a general `PATCH /v1/accounts/{id}` rather than being bolted onto the existing `PUT /v1/manual/accounts/{id}` from spec 003 §2.5 — bank accounts also deserve a user-friendly display name. The Monobank-provided fields (`masked_pan`, `iban`, `type`) remain untouched by this endpoint; only `name` is editable.
  - Idempotency: PATCH with the same name is idempotent.
  - **Name collision.** If another of the caller's accounts (regardless of source) already carries the same trimmed name, return 409 with `code: VALIDATION_ERROR` and `validation_errors: [{field: "name", message: "An account with this name already exists."}]`. This matches the existing `PUT /v1/manual/accounts/{id}` behavior (spec 003 §2.5). Uniqueness is per-user, per-name, case-sensitive; users can distinguish "Cash" from "cash" if they choose. The backend enforces the constraint via a unique index on `(user_id, name)` — a migration is added if the index doesn't already exist for manual-only accounts.
  - Note on the existing `PUT /v1/manual/accounts/{id}` (spec 003 §2.5): that endpoint stays. It's used at manual-account creation-flow to rename before the account has been used, and by the tree's contextual "Rename" for manual accounts. The frontend uses `PATCH /v1/accounts/{id}` as the canonical rename entrypoint from this spec forward; the existing `PUT` is a compatibility artifact and can be deprecated in a future cleanup.

### 10.3 User Display-Name PATCH

**Consumed by:** §5.1 (Profile section `Edit` on display name).

- **`PATCH /v1/users/me`** — edit the current user's own mutable profile fields.
  - Path: `/me` resolves to the caller's `users.id` from the JWT claim. There is deliberately no `PATCH /v1/users/{id}` variant — admins editing other users' display names is not a Phase 1 flow.
  - Request body: `UserSelfUpdate` — `{display_name: str}` (required; whitespace-trimmed server-side; must be 1–100 characters after trimming).
  - Response: 200 with `UserResponse` — the same shape `GET /v1/auth/me` returns today.
  - Validation error: 422 with `code: VALIDATION_ERROR` and `validation_errors: [{field: "display_name", message: "Display name must be 1–100 characters after trimming."}]`. Same shape applies for §10.2's `name` field (`validation_errors: [{field: "name", ...}]`).
  - Authorization: any authenticated user can edit their own display name. Non-caller `user_id` targeting is impossible by construction (path is `/me`, not `/{id}`).
  - Not editable via this endpoint: `email`, `password`, `role`, `is_active`. Password change lives elsewhere (spec 002); email and role changes are admin operations only.
  - Idempotency: PATCH with the same name is idempotent.

### 10.4 Monobank Integration Health Fields

**Consumed by:** §5.5 (Integration-Health Banner), §4.1 (tree contextual "View integration health" action), §4.6 (active-account header integration health badge).

Two changes to the existing `GET /v1/monobank/integrations` endpoint from spec 003 §2.1.

- **Schema addition.** `MonobankIntegrationResponse` gains two fields:
  - `status: Literal["active", "error"]` — mirrors the `bank_integrations.status` column. Currently `active` when the integration is healthy and `error` after a Monobank 401/403 has been observed during backfill (spec 003 §2.3 already sets this state; only the schema exposure is new).
  - `webhook_registered: bool` — whether the last `POST /v1/monobank/link` or `POST /monobank/webhook/{secret}` registration attempt succeeded. Cached on the integration row as a new column `bank_integrations.webhook_registered: bool NOT NULL DEFAULT true` (default `true` for the migration-backfill of existing rows — reasonable because they were healthy enough to survive without user complaint). The link flow (§5.3, and spec 003 §2.1) updates this column with the `webhook_registered` value from the Monobank API call.

- **Query filter change.** The list query currently filters `WHERE status = 'active'` (per code audit in spec-004 round 2 review). This filter is REMOVED for the two contexts that need to see error-state rows — with a deliberate carve-out for the third:
  - **`GET /v1/monobank/integrations` list query** — `status='active'` filter dropped. Returns all caller's integrations regardless of status so the health banner can react.
  - **`POST /v1/monobank/link` re-link on the SAME `monobank_client_id`** — `status='active'` filter dropped. A re-link on an error-state integration for the same Monobank user correctly rebinds it and transitions `status` back to `active`. This is the recovery path when a token was revoked.
  - **`POST /v1/monobank/link` idempotency check for a DIFFERENT `monobank_client_id`** (which returns 409 `INTEGRATION_ALREADY_LINKED` per spec 003 §2.1) — `status='active'` filter is **retained**. Rationale: if the caller has an error-state integration for stale `client_id A` (their old Monobank account died) and now tries to link fresh `client_id B` (a new Monobank account), we do NOT want to block the fresh link with a 409 referencing the dead integration. The 409 fires only when the caller has an *active* integration for a different Monobank user; a stale error-state row for a different Monobank user is not a blocker.

- **Migration.** Adding the `webhook_registered` column is a straightforward migration (default `true`). No data backfill needed. The `bank_integrations.status` column already exists.

- **Ripple:** the webhook receiver (`POST /monobank/webhook/{secret}`) is unaffected — it still validates by `webhook_secret` and doesn't care about `status`. The reprocess flow (spec 003 §2.9) doesn't consult these fields.

### 10.5 Transfer Anomaly Read Endpoint

**Consumed by:** §4.4.3 (`unpaired` badge on feed rows).

- **`GET /v1/transactions/anomalies`** — list `transfer_match_anomalies` rows for the caller.
  - Query params (all optional):
    - `status: Literal["unresolved", "resolved"] | None` — default: no filter (both statuses returned). The badge query passes `status=unresolved`.
    - `reason_code: list[str] | None` — repeated-key list of anomaly reason codes (per spec 003 §2.4.1's enum and the `transfer_match_anomalies.reason_code` DB column): `unpaired_from_description`, `unpaired_to_description`, `ambiguous_pair_match`, `description_account_mismatch`, `description_consistency_mismatch`.
    - `from: datetime | None` (inclusive) and `to: datetime | None` (exclusive) — filter by the anomaly row's `detected_at` timestamp; half-open range per CLAUDE.md.
    - `cursor: str | None` and `limit: int = 100` (max 500) — cursor pagination using the existing `CursorPage` envelope (spec 003 §2.6.1).
  - Response: `CursorPage[TransferAnomalyResponse]` where each item is:
    - `id: UUID` — the anomaly row's id
    - `transaction_id: UUID` — the transaction the anomaly is attached to
    - `reason_code: str` — one of the five enum values above (matches the `transfer_match_anomalies.reason_code` DB column)
    - `detected_at: datetime`
    - `status: Literal["unresolved", "resolved"]`
  - Authorization: joins `transfer_match_anomalies` to `transactions` and filters by `transactions.user_id = app.current_user_id()`. Non-owned anomalies are invisible; no existence leak.
  - **RLS migration.** The `transfer_match_anomalies` table currently has no RLS (per code audit; it was written by the enrichment consumer under `BYPASSRLS`). This spec enables RLS on the table with a policy `USING (transaction_id IN (SELECT id FROM transactions WHERE user_id = current_setting('app.current_user_id')::uuid))`. The `grosh_consumer` role retains its `BYPASSRLS` privilege and continues writing anomalies unchanged; the new API-service query runs under `grosh_api` (RLS-enforced) and reads only anomalies attached to the caller's transactions. The CI invariant test from spec 003 §2.11 gets `transfer_match_anomalies` added to its enumerated per-user-scoped-table list.
  - Sort: `detected_at DESC, id DESC`. Cursor payload and encoding follow spec 003 §2.6.1 verbatim — the `(detected_at, id)` tuple is base64-encoded and opaque to clients. Successor predicate in SQL: `(detected_at, id) < (cursor_detected_at, cursor_id)`. An invalid or undecodable cursor returns 400 with `code: INVALID_CURSOR` (already in the shared error enum).
  - Ripple: this endpoint is read-only. Anomaly rows are still written by the transfer-detection layer (spec 003 §2.4.1) and auto-resolved when a partner arrives; no changes to the write path.

### 10.6 Error Code Additions

The `ErrorCode` StrEnum in `grosh_shared/http/errors.py` gains **exactly one** new value:

| Code                     | HTTP status | Domain                                                    | Introduced in |
|--------------------------|-------------|-----------------------------------------------------------|---------------|
| `TRANSACTION_NOT_FOUND`  | 404         | Manual transaction PATCH/DELETE target not found/owned    | §10.1         |

All other errors returned by §10.1–§10.5 reuse existing codes defined in spec 003 §2.10.2: `ACCOUNT_NOT_FOUND`, `USER_NOT_FOUND`, `INTEGRATION_ALREADY_LINKED`, `INSUFFICIENT_PERMISSIONS`, `VALIDATION_ERROR`, `INVALID_CURSOR`, `INTERNAL_ERROR`. The full RFC 7807 error envelope shape (spec 003 §2.10.2) is unchanged; only the enum grows by one.

---

## 11. Scope and Boundaries

### In-Scope

- Application shell with top navigation (Dashboard / Accounts / Settings / Admin), conditional admin menu, global toast container, reprocess-in-progress and integration-health banners.
- Dashboard route (`/`) with page-level global filters (date, accounts, directions, currencies, categories), account tiles strip (filter-respecting except for current balance), 12-month diverging chart, savings KPI tile, evidence-panel feed widget.
- Accounts route (`/accounts`) with integration→account tree (multi-select with two-way filter sync), universal cross-account feed with infinite scroll, drill-down, contextual account actions (rename for both bank AND manual accounts, soft-delete, backfill, connect Monobank), manual transaction create/edit/delete flows.
- Settings route (`/settings`) with profile section (display name editable), preferences section (timezone, default display currency, default rate source), Monobank integration management (paste/rotate/disconnect), cross-link to `/accounts` for per-account management.
- Admin routes (`/admin/users` and `/admin/operations`) with user list + create + soft-delete, bulk reprocess + rates backfill operation cards, session-scoped recent-jobs lists.
- Cross-cutting reprocess trigger controller (§7.1), reprocess banner (§7.2), data fetching contract (§7.3), formatting and timezone bootstrap (§7.4), empty/loading/error states (§7.5).
- Display-currency selector that persists in Zustand and affects all feed rows.
- OpenAPI-generated TypeScript SDK as the sole source of response-model types.
- **Backend additions from §10:** manual transaction PATCH/DELETE, general account rename PATCH, user display-name PATCH, Monobank integration health schema + query change, transfer anomaly read endpoint. See §10 for the full contracts.

### Out-of-Scope (deferred, with reason)

- **Cross-session visibility of in-flight reprocess and backfill jobs.** Phase 2 — requires a new "list active jobs for current user" / "list active admin jobs" endpoint.
- **Token-less webhook re-registration.** A future backend endpoint (`POST /v1/monobank/integrations/{id}/retry-webhook`) would re-register the webhook using the encrypted token already stored, avoiding a full "Reconnect" trip for the `webhook_registered=false` case. Phase 2.
- **Backend support for multi-account and multi-direction filters on `GET /v1/transactions`.** Spec 003 §2.6 supports single-value `account_id` and `direction`. Multi-select on the frontend (per §3.1.3) is implemented via N parallel requests. If this becomes a real perf problem, a small backend update to accept repeated params is the right path; until then, the parallel-request approach is acceptable at family scale.
- **Categories filter beyond `transfer`.** Spec 003's `special_category` field has only the `transfer` value today; full category UI awaits Phase 2 classification work.
- **Bucket override on the chart** (e.g., "force daily on a 30-day range"). The adaptive bucket interpolation in §3.3 is the only Phase 1 option.
- **Dashboard widget constructor.** Phase 2+. Phase 1's fixed dashboard layout is the v0 of the constructor's default starter layout.
- **Search / free-text filter on transactions.** No backend support; Phase 2 follow-up.
- **Per-account transaction view as a deep-linked route** (e.g., `/accounts/<id>`). Phase 1 uses `/accounts?account=<id>` (query-param-selection in the tree) as the equivalent; the path-segment route can be added later if deep-linking ergonomics require it.
- **Editing or deleting bank transactions from the feed.** Out indefinitely. Bank-sourced rows are immutable from the UI.
- **Net worth dashboard, forecast view, scheduled events.** Phase 3.
- **Family aggregate view, sharing permissions UI.** Phase 4.
- **Mobile-first PWA polish, offline mode.** Out of scope; responsive layout only.
- **Real-time push updates (SSE).** Phase 2 per roadmap.
- **Visual regression testing, accessibility audits.** Phase 2.
