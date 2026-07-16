# Grosh Design System

- **Status:** v1 (Phase 1 frontend baseline)
- **Author:** Nick
- **Scope:** Source of truth for design tokens consumed by `services/frontend`. All concrete oklch values, radii, type scale, spacing scale, and semantic role definitions live here. Spec 004's technical-considerations document references this file at §2.4 (Tailwind v4 setup) and §2.8 (component implementation notes); the same file is the input to Claude Design canvas sessions for visual exploration.

> **What this doc is.** A declarative pin of every design decision that survives across features. Treat it like a tiny contract — Phase 1 implementation reads from it, Phase 2+ specs inherit from it, and the Claude Design canvas reads from it when generating frames. Changes here are deliberate and reviewed; they don't happen mid-implementation slice.
>
> **What this doc is not.** A theming framework, a component library, or a token-pipeline spec. We don't need Style Dictionary or a JSON-token build step at ≤10 users — the tokens live in `services/frontend/src/styles/globals.css` as Tailwind v4 `@theme {}` CSS variables, this document is the human-readable mirror.

---

## 1. Foundational Decisions

These are the high-level choices everything else hangs off. Each was made deliberately during the design-system pass and is not up for negotiation per-feature — change here first if you want to revisit.

| Decision | Choice | Why |
|---|---|---|
| Visual mood | **Financial-serious** (Mercury / Wise / Brex direction) | Daily-driver dashboard for a household financial lead. Signals competence without being playful or editorial. Survives years of use without looking dated. |
| Color mode | **Both light and dark, auto-switched by `prefers-color-scheme`** | shadcn default. Tokens are designed in both modes from day one so dark mode isn't a retrofit. |
| Income/expense saturation | **Chroma 0.135, mid lightness** (L=0.55 light, L=0.62 dark) with **cooler hues** (income teal-leaning hue 172, expense rose-leaning hue 10) | Landed via four iterations against the mockup. Iteration 1 (chroma 0.13–0.16, hue 145/25) was too tiring; iteration 3 (chroma 0.11, L=0.62/0.66) felt under-saturated; iteration 5 (chroma 0.16, L=0.48/0.58) read as matte. Iteration 4 — locked in this document — sits between them: enough chroma to feel grounded, enough lightness to feel like light rather than pigment, with the cool hue keeping eye-fatigue low. Reference visual: `context/product/mockups/mood-financial-serious.html`. |
| Brand accent | **Deep teal** (hue 220, chroma 0.10, lightness 0.50) | Harmonizes with the teal-leaning income green and the cool overall palette. Reads as "considered and quiet" rather than "banking blue" or "Monobank purple." |
| Type system | **Inter, self-hosted** | Industry default for finance UIs (Mercury, Linear, Stripe). Free OFL license. Native tabular-numeral feature flag (`'tnum'`) — critical for monetary columns. |
| Logo / brand mark | **Plain text wordmark, no glyph** (Phase 1) | Defer logo design until there's an actual brand to mark. Plain wordmark in the heading font keeps shipping unblocked. Linear, Vercel, Stripe shipped with text-only wordmarks for years. |
| Token mechanics | **Tailwind v4 CSS-first `@theme {}`** | No `tailwind.config.ts`, no PostCSS config. Tokens live in `globals.css` as CSS custom properties; Tailwind v4 generates utility classes from them. |
| Density | **Confident, not cramped** | Padding 16–20 px in cards, 10–12 px in rows. Sits between Stripe's minimalism (24+) and consumer-app density (8). |

---

## 2. Color Tokens

### 2.1 Reading guide

- All values are **oklch** in the format `oklch(L C H)` — lightness 0–1, chroma 0–~0.4, hue 0–360.
- Each token name has a fixed **role**; values shift between light and dark modes, names do not.
- Tokens that end in `-soft` are tinted background washes for badges, banner backgrounds, and hover states. They are NOT semantic equivalents — `--color-income-soft` is a pale teal-green tint, not a "lighter version of income green for buttons."

### 2.2 Brand (accent) palette

A 10-step ramp on a single hue (220 — deep teal). The 500 step is the canonical accent. Lower steps are tints (for backgrounds, hover states); higher steps are shades (for text on tint, pressed states).

| Token | Light mode | Dark mode | Use |
|---|---|---|---|
| `--color-brand-50` | `oklch(0.97 0.015 220)` | `oklch(0.28 0.040 220)` | Subtle background tint (active chip background, banner ground) |
| `--color-brand-100` | `oklch(0.93 0.030 220)` | `oklch(0.32 0.050 220)` | Hover tint |
| `--color-brand-200` | `oklch(0.86 0.055 220)` | — | Focus ring, secondary affordance |
| `--color-brand-300` | `oklch(0.76 0.080 220)` | — | Decorative borders, sparkline secondary |
| `--color-brand-400` | `oklch(0.62 0.095 220)` | `oklch(0.62 0.110 220)` | Sparkline primary, icon active |
| **`--color-brand-500`** | **`oklch(0.50 0.100 220)`** | **`oklch(0.55 0.105 220)`** | **Canonical brand color.** Primary button background, active chip border, focus ring, KPI sparkline. |
| `--color-brand-600` | `oklch(0.43 0.095 220)` | `oklch(0.48 0.090 220)` | Primary button hover |
| `--color-brand-700` | `oklch(0.36 0.080 220)` | — | Primary button pressed |
| `--color-brand-800` | `oklch(0.28 0.060 220)` | — | Brand-tinted dark text on light tint |
| `--color-brand-900` | `oklch(0.22 0.045 220)` | — | Reserved (logo dark) |

### 2.3 Semantic palette (income / expense / warn)

The colors that encode meaning. Three rules apply to all three:

1. **Color encodes role, not magnitude.** Bar height in the chart and font weight in the feed encode magnitude. The income/expense colors are categorical — same green for ₴100 and ₴100,000.
2. **Use the base token on borders, bars, and badge backgrounds.** Use `-soft` on banner grounds and badge fills. Never use the base token as a text color on a light background — it sits at L=0.62, below the WCAG AA threshold on small body text. Use `--color-text` for the row's amount text; let the 3-px left border carry the role.
3. **Same chroma (0.11) and hue across both modes.** Only lightness shifts between light and dark to balance contrast against the background.

| Token | Light mode | Dark mode | Use |
|---|---|---|---|
| `--color-income` | `oklch(0.55 0.135 172)` | `oklch(0.62 0.135 172)` | Feed-row left border (income), chart positive bars, KPI savings text when positive |
| `--color-income-soft` | `oklch(0.92 0.04 172)` | `oklch(0.28 0.055 172)` | Income pill backgrounds, "savings positive" KPI tile ground |
| `--color-income-dark` | `oklch(0.50 0.145 172)` | — | Pressed/hover for income chip; rarely used directly |
| `--color-expense` | `oklch(0.55 0.135 10)` | `oklch(0.62 0.135 10)` | Feed-row left border (expense), chart negative bars, KPI savings text when negative |
| `--color-expense-soft` | `oklch(0.93 0.035 10)` | `oklch(0.30 0.06 10)` | Expense pill backgrounds |
| `--color-expense-dark` | `oklch(0.50 0.145 10)` | — | Pressed/hover for expense chip |
| `--color-warn` | `oklch(0.74 0.13 78)` | `oklch(0.80 0.13 78)` | Amber. The `PENDING` pill, the `no rate` badge, the `converted_pct < 100%` chart-bar badge, the partial-balance indicator |
| `--color-warn-soft` | `oklch(0.95 0.045 78)` | `oklch(0.32 0.06 78)` | Amber pill backgrounds |

Note the hue shift: pure-grass green is hue ~145; ours is **172** (teal-leaning). Pure tomato-red is hue ~25; ours is **10** (rose-leaning). The shift toward cooler hues was deliberate — pulls the semantic colors closer to the cool-neutral background and reduces eye fatigue during long sessions.

### 2.4 Neutral palette (background, surface, border, text)

The bedrock of every surface. The neutrals carry a tiny hint of warmth in light mode (hue 90, very low chroma) and a tiny hint of blue in dark mode (hue 230, very low chroma) — enough to avoid the "dead gray" look without reading as colored.

| Token | Light mode | Dark mode | Use |
|---|---|---|---|
| `--color-bg` | `oklch(0.985 0.005 90)` | `oklch(0.16 0.015 230)` | Page background. Cream-warm in light, navy-tinted near-black in dark. |
| `--color-surface` | `oklch(1 0 0)` | `oklch(0.205 0.018 230)` | Card and tile background — one step "up" from the page ground. |
| `--color-surface-2` | `oklch(0.975 0.005 90)` | `oklch(0.235 0.020 230)` | Nested surface (drill-down content, dialog footer). |
| `--color-border` | `oklch(0.91 0.008 90)` | `oklch(0.30 0.022 230)` | Default 1 px hairline between cards and rows. |
| `--color-border-strong` | `oklch(0.84 0.010 90)` | `oklch(0.38 0.030 230)` | Emphasis border (focus ring, danger boundary). |
| `--color-text` | `oklch(0.22 0.012 240)` | `oklch(0.96 0.008 230)` | Primary text (descriptions, amounts, headings). |
| `--color-text-muted` | `oklch(0.50 0.010 240)` | `oklch(0.72 0.010 230)` | Secondary text (account names under descriptions, "Last activity" timestamps, rate annotations). |
| `--color-text-faint` | `oklch(0.62 0.008 240)` | `oklch(0.55 0.012 230)` | Tertiary text (footer microcopy, helper hints). Rare. |

### 2.5 Token application rules

These are the rules the implementer follows when picking which token to use for which element. They cover the cases where two tokens could plausibly apply.

| Element | Token | Notes |
|---|---|---|
| Body text (descriptions, amounts) | `--color-text` | Even on income/expense rows. The 3-px left border carries the role; the text stays neutral for WCAG. |
| Muted secondary line (account name, timestamp, rate `@ 41.30`) | `--color-text-muted` | |
| Primary button background | `--color-brand-500` | Hover `--color-brand-600`, pressed `--color-brand-700`. |
| Primary button text | `oklch(1 0 0)` (pure white) | Tested against brand-500 background for AA. |
| Secondary / ghost button | Background `transparent`, border `--color-border`, text `--color-text`. Hover background `--color-surface-2`. | |
| Destructive button | Background `--color-expense`, text white. Same shape as primary. | |
| Focus ring | 2 px outer ring in `--color-brand-200` (light) / `--color-brand-400` (dark), offset 2 px from the focused element. | shadcn convention. |
| Card / tile background | `--color-surface` | |
| Card border | `--color-border`, 1 px | |
| Card subtle shadow (light mode only) | `0 1px 2px 0 oklch(0.22 0.012 240 / 0.04)` | Dark mode uses inset top-edge `inset 0 1px 0 0 oklch(1 0 0 / 0.04)` for depth instead. |
| Feed row hover background | `--color-surface-2` | |
| Filter chip — active | Background `--color-brand-50`, border `--color-brand-500`, text `--color-brand-800` (light) / `--color-brand-100` (dark) | |
| Filter chip — inactive | Background `transparent`, border `--color-border`, text `--color-text-muted` | |
| Amber `PENDING` / `no rate` pill | Background `--color-warn-soft`, text `--color-warn` (light mode) / `--color-text` (dark mode — warn-soft is dark enough that warn yellow on it has poor contrast) | Adjust per real rendering. |
| Neutral `MANUAL` / `↔ paired` pill | Background `--color-surface-2`, text `--color-text-muted`, 1 px border `--color-border` | |
| Banner — reprocess-in-progress | Background `--color-brand-50`, border-left 3 px `--color-brand-500`, text `--color-text` | |
| Banner — error (deferred §2.6) | Background `--color-expense-soft`, border-left 3 px `--color-expense`, text `--color-text` | |
| Chart positive bar | `--color-income`, top corners rounded 2 px | |
| Chart negative bar | `--color-expense`, bottom corners rounded 2 px | |
| Chart zero line | `--color-border-strong`, 1 px | |
| Chart converted_pct badge | Amber dot (filled circle) in `--color-warn`, 8 px diameter, positioned upper-right of bar | |
| Sparkline (KPI tile) | Stroke `--color-brand-400`, 2 px, no fill | |

---

## 3. Radii

Medium-confident radius scale. Cards and inputs are clearly rounded but not playful; chips are fully rounded for affordance contrast.

| Token | Value | Use |
|---|---|---|
| `--radius-sm` | `0.375rem` (6 px) | Inputs, small badges, drop-down menu items |
| `--radius` | `0.5rem` (8 px) | Default — buttons, cards, dialogs, banners |
| `--radius-lg` | `0.75rem` (12 px) | Account tiles, KPI tile, modal containers |
| `--radius-full` | `9999px` | Filter chips, pills, badges, circular avatars |

Chart bars use a manual 2 px corner — too small to derive from the scale. Hardcoded in the chart component.

---

## 4. Type System

### 4.1 Family

**Inter, self-hosted.** Loaded as a font subset (Latin + Cyrillic — the user reads Ukrainian content in transaction descriptions) via woff2.

```
--font-sans: "Inter", ui-sans-serif, system-ui, -apple-system, "Segoe UI",
             Roboto, "Helvetica Neue", Arial, sans-serif;
--font-mono: ui-monospace, "SF Mono", "JetBrains Mono", Menlo, Consolas, monospace;
```

The system fallback chain matters — if the Inter request times out, the browser renders SF Pro on macOS / Segoe UI on Windows, which both have good Latin and Cyrillic coverage and tabular numerals.

### 4.2 Critical font features

Three OpenType features are non-negotiable for a finance UI. They're applied globally on `body`, then escalated per-element where needed:

```
font-feature-settings: 'tnum' on,  /* tabular numerals — digits all same width */
                       'cv11' on,  /* single-storey 'a' — slightly more modern */
                       'ss01' on;  /* alternate stylistic set — see Inter docs */
```

`tnum` is the load-bearing one. Without it, amounts in the feed don't vertically align (`,234.56` becomes wider than `,111.11`), which destroys the entire reason the feed is sortable by amount.

### 4.3 Size scale

Compact scale — finance UIs reward density over generosity. Sizes use rem so they scale with the user's browser default.

| Token | Value | Use |
|---|---|---|
| `--text-xs` | `0.75rem` / 12 px | Microcopy, badge labels, footer hints |
| `--text-sm` | `0.875rem` / 14 px | Secondary body, muted timestamps, table cell secondary line |
| `--text-base` | `1rem` / 16 px | Body text, feed-row description, filter chips |
| `--text-lg` | `1.125rem` / 18 px | Section headings ("Accounts"), tile names |
| `--text-xl` | `1.25rem` / 20 px | Sub-page headings |
| `--text-2xl` | `1.5rem` / 24 px | Page title (rare — dashboard has no big H1 today) |
| `--text-tile-balance` | `1.625rem` / 26 px | Account tile balance number. Tabular nums. Semibold. |
| `--text-kpi` | `2.25rem` / 36 px | KPI tile big number. Tabular nums. Semibold. |

### 4.4 Weights

- **400 Regular** — body text, secondary lines.
- **500 Medium** — feed-row descriptions, tile names, filter-chip labels.
- **600 Semibold** — section headings, tile balance, KPI big number, primary button text.
- **700 Bold** — reserved (currently unused; if a future banner needs extra weight).

Inter at 14 px regular is the workhorse weight — most pixels on the dashboard sit here.

### 4.5 Line-height & letter-spacing

- Body and feed text: `line-height: 1.5` (default Tailwind `leading-normal`).
- Headings (semibold, sizes ≥ 18 px): `line-height: 1.25`, `letter-spacing: -0.01em`. Pulls characters together at large sizes for tighter rhythm.
- Numeric amounts: `letter-spacing: 0` — never negative, because tabular nums already lock spacing.

---

## 5. Spacing & Layout

Tailwind v4's default 4-px grid is the canonical scale. Reproduced here for reference; **do not override unless required by a real layout need**.

| Token | Value | Common use |
|---|---|---|
| `--space-1` | 4 px | Hairline gaps inside pills |
| `--space-2` | 8 px | Inside-row gap between icon and text |
| `--space-3` | 12 px | Default inline gap in flex rows |
| `--space-4` | 16 px | Card inner padding (rows, banners) |
| `--space-5` | 20 px | Card inner padding (tiles) |
| `--space-6` | 24 px | Section gap between tile strip / chart / feed |
| `--space-8` | 32 px | Page outer padding (mobile breakpoint goes to 16 px) |

**Dashboard outer container**: max-width 1440 px, horizontal padding 32 px on desktop / 16 px on mobile.

**Mobile breakpoint**: `768px`. Below that, account tile strip becomes a vertical stack, chart + KPI stack vertically. The feed stays as-is.

---

## 6. Shadows & Elevation

Restrained. Finance UIs are flat; cards rely on borders, not float.

| Token | Light mode | Dark mode |
|---|---|---|
| `--shadow-sm` (cards, tiles) | `0 1px 2px 0 oklch(0.22 0.012 240 / 0.04)` | `inset 0 1px 0 0 oklch(1 0 0 / 0.04)` |
| `--shadow-md` (popovers, drop-downs) | `0 4px 8px -2px oklch(0.22 0.012 240 / 0.08), 0 2px 4px -2px oklch(0.22 0.012 240 / 0.04)` | `0 4px 12px -2px oklch(0 0 0 / 0.40)` |
| `--shadow-lg` (modals, dialogs) | `0 12px 24px -6px oklch(0.22 0.012 240 / 0.12)` | `0 12px 32px -6px oklch(0 0 0 / 0.55)` |

Dark mode uses an inset top-edge highlight on small cards instead of a drop shadow — gives the same "lifted" feel without a black-on-black blur.

---

## 7. Iconography

- **Library**: `lucide-react`. Already pinned in spec 004's tech spec §2.1.
- **Stroke width**: 2 (Lucide default). Don't override unless inside a tight badge where 1.5 reads better.
- **Default size**: 16 px for inline icons (inside buttons, in row metadata), 20 px for tile icons, 24 px for prominent affordances.
- **Color**: inherits `currentColor` — set the icon color via the parent element's `color`. Icon-only buttons use `--color-text-muted` at rest, `--color-text` on hover.

---

## 8. Logo & Wordmark

Phase 1 ships with a **plain text wordmark** in Inter Semibold:

```
Grosh
```

- Top app bar instance: 18 px Inter Semibold, color `--color-text`, no glyph.
- Login screen instance: 36 px Inter Semibold, color `--color-text`, centered above the form.
- Favicon: a 32 × 32 png of the letter "G" in Inter Semibold, color `--color-brand-500` on `--color-surface` background. Generated and committed once; the design system doesn't need a token for it.

**Future iteration:** when the brand has earned a glyph, run a Claude Design canvas session to explore wordmark + glyph pairings. The design system gets one new token then:

```
--logo-mark: url("/branding/grosh-mark.svg");  /* unset in Phase 1 */
```

Any component using `--logo-mark` must gracefully fall back to "no mark, just wordmark" when the token is unset. This way the logo can ship later without an implementation slice touching every consumer.

---

## 9. Motion

Restrained, function-focused. No decorative animations.

| Token | Value | Use |
|---|---|---|
| `--duration-fast` | 120 ms | Hover state transitions, button press feedback |
| `--duration` | 200 ms | Drop-down open, drill-down expand/collapse, banner mount |
| `--duration-slow` | 400 ms | Modal open / close |
| `--easing` | `cubic-bezier(0.2, 0.0, 0, 1.0)` | Default "out" curve — fast start, settle softly |
| `--easing-in-out` | `cubic-bezier(0.4, 0, 0.2, 1)` | For modal transitions; symmetric entry and exit |

Reduce-motion users (`prefers-reduced-motion: reduce`) get all durations clamped to 0 ms via a global CSS rule. The state still changes; the transition just doesn't animate.

---

## 10. Implementation: where tokens live in the codebase

| File | Role |
|---|---|
| `services/frontend/src/styles/globals.css` | Single source of truth. Contains the `@theme {}` block declaring every token from this document. Imported once in `app/layout.tsx`. |
| `services/frontend/src/styles/typography.css` | Inter `@font-face` declarations and Latin+Cyrillic subset URLs. Imported from `globals.css`. |
| `services/frontend/public/fonts/` | Inter woff2 files (regular, medium, semibold). Committed to repo; no CDN. |
| `services/frontend/public/branding/` | Reserved for the future logo SVG. Empty in Phase 1. |
| `services/frontend/src/components/ui/` | shadcn primitives. Each component reads its tokens via Tailwind utility classes — never via direct `var()` calls — so the design system stays the only token source. |

Implementation note for the implementer (spec 004 tasks): when `npx shadcn@latest init` is run, it will offer to write its own default `oklch` tokens into `globals.css`. **Decline the offer or accept and immediately overwrite** — this document's tokens take precedence. shadcn's defaults are tuned for its showcase; ours are tuned for Grosh.

---

## 11. How to evolve this document

- **Adding a new token** (because a new feature needs it): edit this file first, get a design review, then update `globals.css`. The order matters — the document is the spec, the code is the implementation.
- **Changing an existing token value** (e.g. the user wants a more saturated income green): update both files in the same commit; visual regression is the reviewer's responsibility.
- **Renaming a token**: don't, unless you're also renaming everywhere it's referenced. Token names are part of the spec contract — renaming `--color-income` to `--color-positive` would force a sweep across `services/frontend` and any future docs.
- **Deprecating a token**: leave it in the doc with a `Deprecated` note for one minor version, then remove. There's no live consumer outside this repo, so this is cheap.

The mockup files under `context/product/mockups/` are reference visuals only — they may drift slightly from the canonical tokens above. If they ever conflict, **this document wins**. The mockups are regenerated when the design system moves significantly; minor token drift in mockups is acceptable.
