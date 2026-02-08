# Project summary (concise)

## What we’re building
A local-first, multi-account portfolio tracker + tax engine + GPT assistant.

## Core inputs
- Trade history CSVs (stocks + options)
- Cash activity CSVs (deposits/withdrawals; external vs internal transfers)
- Optional: broker tax exports (CSV preferred) for reconciliation
- Optional: annual tax PDF parsing (fallback only)

## MUST-haves (MVP)
- Multi-account support (Taxable + IRA), consolidated + per-account views
- Deterministic realized/unrealized P&L (stocks + options)
- Selected-year **tax gain/loss report** for taxable accounts:
  - ST/LT splits
  - wash sale disallowed loss (code W) + basis adjustments applied to replacement lots
  - “8949-like” detail export + “Schedule D-like” summary
- Two wash sale modes:
  - Broker-style (helps match typical 1099-B limitations)
  - IRS-style (cross-account taxable; flag IRA-trigger cases)
- Year-end reconciliation workflow:
  - import broker “realized gain/loss” export OR enter broker totals manually
  - show diffs + drilldowns + mismatch checklist
- Store lot-selection metadata:
  - account default disposal method
  - effective-date changes (FIFO early; later MinTax), and/or per-sale overrides if broker CSV includes it

## GPT features
- In-app “Ask GPT” chat:
  - Local-only mode (DB tools only) + Web-enabled mode (web search tool)
  - GPT reads raw lots/trades via tool calls (not by dumping DB)
- Proactive “Daily Briefing / Risk Officer”:
  - deterministic risk checks (code)
  - GPT writes summary + protective action list
  - optionally drafts option legs (rule-based, labeled “draft—confirm in chain”)
- RSS framework:
  - feed manager UI + ingestion + DB storage (feeds can be added later)
  - no paywall bypass/scraping

## Guardrails
- No brokerage credentials storage, no scraping logged-in broker pages
- No auto-trading (ideas only)
- Keep personal data out of the public repo
- Prefer conda for Python environment
- Back up personal data via local Dropbox folder (or other synced folder)
