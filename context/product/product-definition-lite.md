# Grosh — Product Summary

## Vision

Replace a tedious spreadsheet workflow with an automated, self-hosted personal finance platform that ingests real banking data, classifies it intelligently, and forecasts future cash flow — giving a small family network a single source of financial truth with no manual backfilling.

## Target Audience

A small family network (~3 people), self-hosted, invite-only. Users want personal spending visibility and household-level aggregates without manual data entry.

## Core Features

- **Automatic transaction ingestion** — Monobank webhook → Redpanda pipeline; manual entry for cash/other accounts
- **Intelligent classification** — Merchant rules → MCC fallback → ML embedding classifier, with human-in-the-loop feedback loop
- **Cash flow forecasting** — Scheduled events + Prophet-based probabilistic forecast, 90-day forward view with uncertainty bands
- **Net worth dashboard** — All accounts (bank, cash, debts) aggregated per user and across household
- **Family network view** — Per-user data isolation (PostgreSQL RLS) + admin household aggregates
- **Extensible integrations** — Monobank first; architecture supports additional banks without rework
