# Product Definition: Grosh

- **Version:** 1.0
- **Status:** Proposed

---

## 1. The Big Picture (The "Why")

### 1.1. Project Vision & Purpose

To replace a tedious, error-prone spreadsheet workflow with an automated, self-hosted personal finance platform that ingests real banking data, classifies it intelligently, and forecasts future cash flow — giving a small family network a single source of financial truth with no manual backfilling.

Secondarily, the project is a deliberate learning vehicle for modern backend, streaming, ML, and DevOps practices in a real production context.

### 1.2. Target Audience

A small, trusted family network of ~3 people who share finances informally but want individual privacy plus household-level visibility. Users are technically comfortable but not finance professionals. The platform is self-hosted, invite-only — no public registration.

### 1.3. User Personas

- **Persona 1: "Nick the Admin"**
  - **Role:** Platform owner, primary developer, household financial lead.
  - **Goal:** Full automation of transaction ingestion; a reliable cash flow forecast (configurable horizon, up to 1 year as history grows); household-level net worth at a glance.
  - **Frustration:** Spreadsheets require manual entry, are easy to forget, and offer no forecasting beyond simple formulas.

- **Persona 2: "Family Member"**
  - **Role:** Secondary user (partner or parent), non-technical.
  - **Goal:** See personal spending by category, confirm or correct auto-classifications, trust that the data is up to date without any effort.
  - **Frustration:** Doesn't want to open a bank app and re-enter data somewhere else.

### 1.4. Success Metrics

- **Zero manual entry:** All Monobank transactions arrive automatically via webhook — no spreadsheet backfilling required. Historical transactions are imported via backfill on first setup.
- **High classification accuracy:** 90%+ of transactions are correctly categorized without user correction, improving over time via the feedback loop.
- **Reliable forecasting:** Cash flow forecasts are within a reasonable margin of actual spend; confidence intervals are visible. Horizon is configurable — defaults to 90 days early on, extends up to 1 year as transaction history accumulates.
- **Family visibility:** Each member sees their own data; the admin sees household aggregates — all in one UI, with no separate tools.
- **Learning value:** The project demonstrates hands-on proficiency with Kafka-compatible streaming, ML pipelines with active learning, Kubernetes, and modern CI/CD.

---

## 2. The Product Experience (The "What")

### 2.1. Core Features

- **Automatic transaction ingestion** — Monobank webhook receiver pushes transactions into a streaming pipeline in real time. Historical backfill pulls up to the full statement history via Monobank's statement API and routes it through the same pipeline — no separate code path. Manual accounts and cash can be entered by the user.
- **Intelligent transaction classification** — Three-tier system: merchant rules → MCC code fallback → embedding-based ML classifier. Human-in-the-loop feedback loop promotes corrections to rules and improves the model over time.
- **Cash flow forecasting** — Combines deterministic scheduled events (salary, rent, subscriptions) with probabilistic Prophet-based forecasting on historical category spend. Forecast horizon is configurable: defaults to 90 days, extends up to 1 year as history accumulates. Displayed with uncertainty bands.
- **Net worth dashboard** — Aggregates all accounts (bank cards, cash, debts) per user and across the household. Tracks balance over time.
- **Family network view** — Admin sees category-level household aggregates; raw transactions are isolated per user unless explicitly shared. Privacy enforced at the database layer via Row-Level Security.
- **Extensible bank integrations** — Monobank is the first integration; the architecture is designed to accommodate additional bank APIs without reworking the core pipeline.

### 2.2. User Journey

**Real-time:** A new transaction hits the Monobank webhook → FastAPI receives it and publishes to Redpanda → the enrichment consumer deduplicates, applies merchant rules, falls back to MCC, then runs the ML classifier → the enriched transaction is written to TimescaleDB → the user opens the Next.js UI, sees the transaction in their feed with a predicted category and confidence score → if uncertain, they confirm or correct it → high-confidence recurring merchants are automatically promoted to rules → the forecast view updates to reflect the new spending pattern.

**First setup / backfill:** Admin triggers `POST /accounts/{id}/backfill` for each account → FastAPI paginates through Monobank's statement API and publishes all historical transactions to the same Redpanda topic → the consumer processes them identically, deduplicating by transaction ID → history is populated in the feed and available for forecasting.

---

## 3. Project Boundaries

### 3.1. What's In-Scope for this Version

- Monobank webhook receiver and historical backfill, both routing through the same Redpanda-based transaction pipeline.
- Transaction consumer with enrichment: deduplication, rule-based classification, MCC fallback, and k-NN embedding classifier.
- Human-in-the-loop feedback UI: surface unclassified or low-confidence transactions, accept user corrections, auto-promote recurring merchants to rules.
- Manual entry for cash transactions and non-Monobank accounts.
- Transaction feed UI with category labels and filter/search.
- Scheduled events engine and Prophet-based cash flow forecast with configurable horizon (90-day default, up to 1 year) and uncertainty bands.
- Net worth dashboard (bank accounts + cash + debts).
- Family network model: per-user data isolation (RLS) + admin-only household aggregate view.
- JWT auth with refresh token rotation; admin-created accounts only.
- Self-hosted infrastructure: k3s on Hetzner, TimescaleDB + Redpanda via Docker Compose, Terraform provisioning, GitHub Actions CI/CD, Infisical secrets.

### 3.2. What's Out-of-Scope (Non-Goals)

- Investment portfolio tracking (designed with extension in mind, but not built in v1).
- Public registration or multi-tenant SaaS.
- Mobile application (responsive web only).
- Automatic bank connections beyond Monobank in v1 (architecture supports it; integrations deferred).
- Tax calculation or advice.
- Replacing Monobank's own UI for day-to-day banking interactions.
- Notification service (Telegram bot) — planned but post-v1.
