# ROADMAP.md — Phased plan

## Phase 0 — Scaffold
- Repo structure + Streamlit skeleton
- Local DB schema for multi-account support
- Basic import UI

## Phase 1 (MVP) — Imports + P&L + contributions + calendar
- Trades CSV import (mapping + validation)
- Cash activity import (external vs internal tagging)
- Lots engine (FIFO default) + realized/unrealized P&L
- Daily realized P&L calendar
- Wash sale risk detector (basic cross-account ticker checks)
- Export reports to CSV

## Phase 2 — Benchmarks + time windows
- Price history provider interface + caching
- Benchmark comparison vs proxies (DIA/SPY/QQQ)
- Money-weighted return (XIRR)
- Window filtering (inception, 1Y, 6M, 3M, 1M, 5D)

## Phase 3 — Enrichment
- Earnings calendar for current holdings
- Macro event calendar
- News for current holdings only

## Phase 4 — Strategy assistant
- Covered call and cash-secured put idea cards
- Event proximity warnings
- Account-aware policy settings (taxable vs IRA)

## Phase 5 — Optional broker sync
- Add official API integration (OAuth) as alternative to file imports
