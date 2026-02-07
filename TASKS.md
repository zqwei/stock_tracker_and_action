# TASKS.md — First tasks for Codex (high signal)

These are starter tasks to keep development focused.

## Task 1 — Create repo scaffold
- Create the folder structure described in `AGENTS.md`
- Add a minimal Streamlit app with a sidebar:
  - Accounts
  - Import Trades
  - Import Cash
  - Overview (placeholder)
  - Data Quality (placeholder)

## Task 2 — DB schema (multi-account)
Implement DB models/tables:
- accounts
- trades_raw (imported raw rows + mapping metadata)
- trades_normalized
- cash_activity
- price_cache (optional stub)
- pnl_realized (derived)
- positions_open (derived snapshot)

## Task 3 — CSV import + mapping
- Accept a CSV upload
- Show a mapping step if columns are unknown
- Persist mapping by broker name + file signature
- Output normalized trades rows into DB

## Task 4 — P&L engine (MVP)
- FIFO lots for stocks
- Options open/close matching by contract spec
- Compute realized P&L by close date
- Compute open positions + unrealized P&L from latest quote (stub provider ok)

## Task 5 — Contributions
- Import cash activity CSV
- Provide tagging UI to mark external vs internal
- Compute net contributions by account + consolidated

## Task 6 — Calendar (daily realized)
- Build daily realized P&L table
- Show calendar heatmap + daily details table

## Task 7 — Wash sale risk (basic)
- For taxable loss sale rows, scan buys within +/-30 days across all accounts
- Show warnings + trade links in UI

Definition of Done for MVP:
- Import trades + cash for 2 accounts (taxable + IRA)
- Overview shows realized/unrealized + contributions
- Calendar works
- Wash sale warnings appear when conditions are met
