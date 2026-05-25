# Functional Specification: Transaction Feed & Dashboard

- **Roadmap Item:** Basic Transaction Feed UI + Rolling Monthly Aggregate Chart (Phase 1)
- **Status:** Draft
- **Author:** Nick
- **Dependency:** Spec 003 — Transaction Ingestion Pipeline (provides REST API endpoints)

---

## 1. Overview and Rationale (The "Why")

Transactions flowing into the database are invisible without a frontend. The transaction feed and monthly chart together replace the spreadsheet's two core views: the detailed line-item log and the year-at-a-glance monthly summary. The feed gives the user confidence that data is arriving correctly; the chart answers "how am I doing financially" at a glance.

The chart needs to support dual-currency viewing (UAH and USD) because the user's income arrives in USD and expenses are in UAH — these carry different cognitive weight and the user wants to see both perspectives.

**Success criteria:**

- User can see all transactions in a combined, filterable feed.
- User can see a rolling 12-month income/expense chart with savings trend.
- Chart is switchable between UAH and USD views.
- Anomalous months (high spend, high income) are visually prominent without requiring the user to read axis labels.

---

## 2. Functional Requirements (The "What")

### 2.1 Transaction Feed

Combined transaction list showing all transactions across all accounts.

- **Acceptance Criteria:**
  - [ ] Feed shows: date, description, amount, account name, and transaction type (income/expense/transfer).
  - [ ] Transactions are color-coded by type: income = green, expense = red, transfer = neutral/grey.
  - [ ] Filter toggles at the top allow showing/hiding each type (Income / Transfers / Expenses). All enabled by default.
  - [ ] Feed shows both original currency and UAH equivalent where applicable (e.g., "$100 / ₴4,231").
  - [ ] When a transaction involves a currency conversion (operation currency differs from account currency), the feed shows both amounts with an arrow (e.g., "$100 → ₴4,231") and the effective conversion rate as secondary text (e.g., "@ 42.31").
  - [ ] Feed is sorted by date descending (newest first).
  - [ ] Feed supports pagination or infinite scroll.
  - [ ] Data fetched via `GET /transactions` with TanStack Query for caching and background revalidation.

### 2.2 Rolling Monthly Aggregate Chart

Clustered bar chart showing income and expenses per month over a rolling 12-month window.

- **Acceptance Criteria:**
  - [ ] Chart displays a rolling 12-month window from the current date.
  - [ ] Each month shows clustered bars: one for total income, one for total expenses.
  - [ ] A delta (savings) trend line is overlaid on the bars.
  - [ ] Internal transfers are excluded from both income and expense totals (enforced by the API's continuous aggregate).
  - [ ] Intra-series intensity scaling: each bar's color intensity is proportional to its value relative to other bars in the same series (income vs. income, expense vs. expense). Uses percentile-based scaling so single outliers don't flatten the rest.
  - [ ] Chart is switchable between UAH and USD views via a toggle. Each view fetches its own pre-computed aggregate.
  - [ ] Chart renders using Recharts `ComposedChart` (bars + line on the same canvas).
  - [ ] Data fetched via `GET /transactions/monthly-aggregate?currency=UAH|USD` with TanStack Query.

---

## 3. Scope and Boundaries

### In-Scope

- Combined transaction feed with type-based color coding and filter toggles
- Dual-currency display in the feed (original + UAH) with conversion rate for FX transactions
- Rolling 12-month clustered bar chart with delta trend line
- Intra-series intensity scaling on bars (percentile-based)
- UAH/USD chart toggle
- Pagination or infinite scroll on the feed
- TanStack Query for data fetching and caching

### Out-of-Scope

- Per-account transaction view — future enhancement
- Category labels on transactions — Phase 2 (classification)
- Search/text filter on transactions — future enhancement
- Date range picker on chart (rolling 12 months only, no manual range)
- Transaction classification (rule engine, MCC, ML) — Phase 2
- Forecasting and scheduled events — Phase 3
- Net worth dashboard — Phase 3
- Family aggregate view — Phase 4
