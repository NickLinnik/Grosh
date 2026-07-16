# Claude Design Brief — Grosh Frontend (Phase 1)

This brief is the input to a Claude Design canvas session. It is intentionally short. The two attached documents — `context/product/design-system.md` and `context/spec/004-phase1-frontend/functional-spec.md` — carry the depth; this file directs the canvas's attention.

---

## What I want

**One canvas session covering the whole Phase 1 Grosh frontend, plus a parallel logo experiment.**

The Phase 1 frontend spans four authenticated routes for regular users plus two admin routes. Design the canonical / default state of each screen at desktop width (~1440 px), plus the important state variants and one mobile viewport. **Do not attempt to design every corner case** — the spec is exhaustive about behaviour; your job is to compose the surfaces, not enumerate them.

In parallel, produce 4–6 wordmark + glyph pairings (see §5 below).

---

## Screens to design

Numbered in the order I want to see them approved. If we run short of canvas time, cut from the bottom.

| # | Screen | Priority | Spec reference |
|---|--------|----------|----------------|
| 1 | `/` dashboard — default state | Must have | functional-spec §3, §2.1 top nav, §2.2 App Shell banners (hidden here), §3.6 display currency |
| 2 | `/accounts` — two-pane, no account selected in tree, universal feed on right | Must have | functional-spec §4.1 tree, §4.2 right pane, §4.3 filter bar, §4.4 feed |
| 3 | `/accounts` — with a single manual cash account selected in the tree (the account-header row, filter chips, `+ Add transaction` button visible, feed scoped to that account) | Must have | functional-spec §4.4.4 drill-down open, §4.5 manual entry sheet, §4.6 active-account header |
| 4 | `/settings` — profile + preferences + Monobank integration section | Must have | functional-spec §5.1, §5.2, §5.3, §5.5 (banner NOT shown here — this is the settings page proper) |
| 5 | App Shell with **both banners** active — reprocess-in-progress banner (neutral) stacked below integration-health banner (red). Shown against a dashboard background. | Must have | functional-spec §2.2, §5.5, §7.2 |
| 6 | `/admin/users` — user list with 3 rows | Should have | functional-spec §6.1 |
| 7 | `/admin/operations` — bulk reprocess + rates backfill cards, one recent job each | Should have | functional-spec §6.2 |
| 8 | Dashboard mobile (<768 px) — single-column, hamburger nav | Should have | functional-spec §2.1 (mobile), §3 (mobile paragraph) |
| 9 | Empty-state pass: brand-new user, no accounts on `/accounts`, empty tile strip on `/`, empty feed | Nice to have | functional-spec §7.5 |

**A note on scope:** the spec has ~10 additional sub-states (drill-down expanded on a bank row, reprocess banner in `failed` variant, admin bulk-reprocess with skipped users toast, integration-health banner across each `(status, webhook_registered)` combination, etc.). Don't design them individually — the design system + the base compositions above give the implementer enough to render them by rule. If a specific sub-state seems visually load-bearing while you're designing #1–#5, add it as a supporting frame; don't invent frames for spec branches I haven't asked for.

---

## Sample data

Use this consistent set across every frame so the canvas feels like one app, not seven fragments.

**Household:** Nick (admin), Alice (member), Bob (member). All three appear in the admin user list; only Nick's data appears on the dashboard and accounts screens (since screens are per-user).

**Accounts (Nick's, shown in the tree under two integration groups):**
- **Monobank Black** — UAH debit card, balance ₴42,318.50, last activity 2 min ago
- **Monobank FOP USD** — USD FOP account, balance $1,247.00, last activity yesterday
- **Cash on hand** — manual cash account, UAH, balance ₴3,200.00, last activity 5 days ago

**Chart (last 12 months, UAH):** typical monthly pattern with two salary spikes (income bar to ~₴90k in ~2 months), regular monthly expense of ~₴35–45k. One month has slight negative net. One bar carries the amber `!` badge (converted_pct < 100%).

**Feed rows (8+ across screens, mix as needed):**
- ATB Market · Monobank Black · −₴1,247.50 (expense)
- Salary — Provectus IT · Monobank FOP USD · +$2,500.00 · ≈ ₴103,250 · @ 41.30 (income, cross-currency)
- Monobank → FOP transfer · Monobank Black · −₴25,000 · with `↔ paired` badge (transfer)
- Сільпо · Monobank Black · −₴832.10 (expense)
- OLX покупка · Monobank Black · −₴2,400 · with `PENDING` pill (hold)
- Coffee at Aroma Kava · Cash on hand · −₴120 · with `MANUAL` badge
- Freelance invoice · Monobank FOP USD · +$450 · ≈ (no rate) · with `no rate` amber badge
- Anomaly row: pattern-matches transfer but wasn't paired · Monobank Black · −₴5,000 · with amber `unpaired` badge (spec §4.4.3)

**Admin recent jobs:**
- reprocess job-abc123 · 2 min ago · running
- reprocess job-xyz456 · 1 hr ago · succeeded
- rates-backfill job-def789 · 5 min ago · succeeded

---

## How to design it

### Use the design system, do not redesign it

`context/product/design-system.md` is locked. It defines:

- The full color palette (oklch tokens, both light and dark mode)
- Radii, type scale, spacing scale
- Token application rules for every UI primitive — which token applies to which element
- Density, motion, shadow conventions

**You are bound by these tokens.** Do not propose alternate color values. Do not pick a different font. Do not vary the radius scale. The design system is the answer to "how does Grosh look"; your job is "how does Grosh *composes* across all Phase 1 surfaces."

### Both modes, both viewports

Every "Must have" screen (1–5) should exist in **light AND dark mode**. Should-haves (6–7) can pick one mode if canvas time is tight. Only screen #8 is mobile; all others are desktop 1440 px.

### What "good" looks like

- **Information hierarchy is clear at a glance.** Especially on the dashboard: KPI big number and chart's net-savings story readable in under a second. Feed is dense; chart is heroic; tiles are quiet anchors.
- **The data is the protagonist.** Chrome (borders, shadows, icons) is restrained. No decoration that doesn't earn its place.
- **Numbers line up vertically.** Tabular numerals are mandatory on amounts. The feed's right column should look like a column, not a ragged edge.
- **Color encodes role, not magnitude.** Bar height encodes amount; the green/red encodes income vs expense. Never both.
- **Consistent shell across routes.** Top nav, banners, page container, filter-bar visual language should be identical between `/` and `/accounts`. A user navigating between them should feel like they're in the same app, not five.
- **The tree on `/accounts` is a first-class UI element, not a sidebar afterthought.** It carries filter state via multi-select checkboxes; the right pane's filter chips visually mirror the tree's state.

### Aesthetic anchors (for vibes, not for copy)

If you need a directional reference for the *feel*: think Mercury, Wise, or Brex. Confident, quiet, financially serious. Not Linear (too tech-tool). Not Monobank (too consumer-warm). Not Bloomberg (too editorial).

I have **not** attached any visual mood reference deliberately — interpret the design system on your own, fresh. If you produce something I haven't seen before, that's the point.

### Explore variants

Use Claude Design's "explore variants" feature on **three specific composition questions**. For everything else, one variant is enough — the spec dictates the composition.

1. **Dashboard chart + KPI row layout.** The relationship between the chart and the KPI tile has the most freedom: side-by-side vs stacked vs KPI as floating card vs KPI integrated into chart. 2–3 directional alternatives on this row.
2. **`/accounts` tree vs right-pane balance.** Where the tree's edge meets the right pane, what's the visual affordance? Persistent divider, subtle border, floating pane? Multi-select checkboxes — always visible or hover-only? 2 alternatives.
3. **Filter bar density.** The filter bar shows date-range button + accounts pill + directions chips + display-currency (right-corner). At three active filters, does it wrap? Collapse to a summary chip? 2 alternatives.

---

## Interactions to visualize (not full state machines)

Just enough to make the composition legible. Don't design every hover state; do show the key moments below.

- **`/accounts` tree selection sync** — pick one screen (screen #3) to show a mid-hover state: user is about to check "Monobank FOP USD" while "Monobank Black" is already checked. The right pane's filter chips row shows "Account: Monobank Black" (the already-committed selection), and the tree row for FOP USD shows the pre-check hover state. Don't animate — just capture the visual moment that communicates "both views are the same state."
- **Drill-down open on a manual row** — screen #3 with one manual row expanded, showing Edit + Delete buttons (which are the §10.1 backend-additions capabilities). MCC label, cashback, IBAN (if present), conversion path, transaction ID with copy button.
- **Reprocess banner failed variant** — on screen #5, show the failure state (red banner, `Retry` button, failure_reason text) rather than the running variant, since failure carries more visual weight and testing the design's ability to handle red banner + red integration-health banner is worthwhile.

---

## 4. Out of scope for this canvas

- **Login screen** — already implemented; a restyle pass against the design system happens in the implementation slice, not the design pass.
- **Full state machines for the reprocess-trigger controller.** Show the banner variants (running / failed / 503-halted), not the button pending state.
- **Every `(status, webhook_registered)` combination for the integration-health banner.** Show one canonical failure state (screen #5); the spec's message matrix (§5.5) covers the rest textually.
- **Print / export layouts, PWA / offline UI, animations.** Phase 2+.
- **Non-English copy.** All chrome is English; feed row descriptions can mix Ukrainian and English (that's realistic sample data).
- **Any surface not covered by spec 004.** No net worth dashboard, no forecast view, no classification labels, no family aggregate view, no bank-connect flow deeper than the paste-token → confirm-accounts sequence in §5.3.

---

## 5. Parallel side experiment: logo

Grosh ships Phase 1 with a plain text wordmark in Inter Semibold. The design system reserves a `--logo-mark` token that is currently unset.

In parallel with the frontend frames, produce **4–6 wordmark + glyph pairings** on the same canvas (as a separate frame group). Constraints:

- Use only the design-system palette. Brand-500 deep teal is the obvious anchor.
- Inter Semibold (or one weight up to Inter Bold) for the wordmark.
- Glyph should be simple — geometric or abstract, single-color, scaleable from 16 px (favicon) to 64 px (login screen) without losing character.
- Show each variant at three sizes: 16 px (favicon), 24 px (top app bar), 48 px (login screen).
- One variant should be "wordmark only, no glyph" as the control. If none of the glyph variants beat the control, the control wins.

**What I'm looking for:** does any glyph make Grosh feel like *a thing* in a way the plain wordmark doesn't? If not, that's fine — we ship without one. Don't force it.

### Naming hint (not a constraint)

"Grosh" is Ukrainian for a historical small coin (a fraction of a hryvnia). If the glyph wants to nod at coins, currency, growth, household, or a layered/stacked aggregation metaphor — those are all on-brand. But abstract geometric marks (a single letterform, a clean shape) are equally welcome. No literal hryvnia symbol — that's the currency, not the brand.

---

## Format of the deliverable

Whatever the canvas naturally produces. I'll review on the canvas and export the frames I want to keep into `context/product/mockups/` as HTML or PNG. The exported frames become the visual references the implementation slice consumes; the canvas session itself is throwaway.

---

## What success looks like

After this session I should have:

1. Locked composition for each of screens #1–#5 (must-haves). Ideally #6–#8 too.
2. A decision on whether Grosh has a glyph or stays text-only.
3. Possibly a small list of design-system refinements I want to make based on seeing the tokens compose at real scale.

That's it. We are not finishing the entire UI here — we are locking visual composition for the surfaces the implementation slice needs to build and answering the logo question. Behavior details (state machines, error handling, race conditions) all live in the spec; do not attempt to redesign them.
