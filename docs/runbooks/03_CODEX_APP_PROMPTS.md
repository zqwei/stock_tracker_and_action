# Codex app prompts (copy/paste)

## Prompt 0 — Coordinator (architecture + boundaries)
```text
Read and follow AGENTS.md, TAX_REPORTING.md, RECONCILIATION.md, TASKS.md.

Confirm you loaded them (list instruction sources) and summarize non-negotiables.

Create an implementation plan and file-ownership boundaries for 3 worktrees:
- DB/Ingest agent
- Tax/WashSale agent
- UI agent

Define interface contracts:
- canonical normalized trades schema
- wash sale & lots ledger tables
- functions the UI will call
Then stop.
```

## Prompt 1 — DB/Ingest agent (worktree)
```text
ROLE: DB/Ingest agent. Follow AGENTS.md + TASKS.md.

Implement:
- DB schema (multi-account)
- trades_raw, trades_normalized, cash_activity
- mapping persistence for CSV imports + minimal mapping UI
- data quality validators (unparsed options, missing fields, duplicates)
- store disposal method metadata (account default + effective-date history + per-sale override field)

Do NOT implement tax/wash sale math. Do NOT build full UI beyond import/mapping.
Add unit tests for parsing/normalization.
Produce a PR.
```

## Prompt 2 — Tax/WashSale agent (worktree)
```text
ROLE: Tax/WashSale agent. Follow AGENTS.md, TAX_REPORTING.md, RECONCILIATION.md.

Implement:
- lot engine (FIFO default; honor per-sale specified lots when available)
- wash sale engine with basis adjustments (selected-year capable, year-boundary aware)
- Tax Year report page output:
  - 8949-like rows (code W + adjustment amount)
  - ST/LT totals
  - year-end open lots with adjusted basis
- Broker-style vs IRS-style modes
Add tests: partial replacement, Dec/Jan boundary, multiple replacement lots.
Produce a PR.
```

## Prompt 3 — UI agent (worktree)
```text
ROLE: UI agent. Follow AGENTS.md + REPORTS.md.

Build Streamlit UI pages and wiring ONLY:
- Overview, Holdings, P&L, Contributions, Calendar
- Tax Year + Reconciliation pages (call core functions)
- Wash Sale Risk page
- Settings/Data Quality

Do NOT change DB schema. Do NOT implement finance math.
Produce a PR.
```

## Prompt 4 — GPT features (worktree, after core is stable)
```text
Implement in-app GPT features.

- Add Streamlit page: Ask GPT
- Use Responses API with:
  - function calling tools (read-only DB queries, row-limited)
  - optional web_search tool (toggle), show sources/citations
- Add Daily Briefing feature:
  - deterministic risk checks (code)
  - GPT summary + protective actions
  - optional draft option legs (rule-based, “draft—confirm in chain”)
- Add RSS framework:
  - feed manager UI + ingestion + DB storage
  - no paywall bypass/scraping
Stop after it works end-to-end.
```
