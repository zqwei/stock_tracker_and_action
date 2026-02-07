# AGENTS.md — Portfolio Tracker & Strategy Assistant (Multi-account, taxable + IRA)

## 0) Mission
Build a **local-first** portfolio tracking + reporting app that supports **multiple brokerage accounts**, including **taxable** and **IRA** accounts, primarily driven by **user-imported files** (CSV trade history, account activity, ACH transfers), with optional **annual tax report PDF** parsing as a fallback.

The app must generate:
- Accurate **realized/unrealized P&L** by **stock and options**
- **Net contributions** (“money invested”) from **bank → brokerage** transfers
- **Performance vs benchmarks** (DJI / SPX / IXIC proxies) across time windows
- **P&L calendar** (daily realized P&L; expand later to daily total P&L)
- **Weekly calendar**: earnings + macro events (CPI, JOLTS, FOMC, Jobs report, etc.)
- **News for current holdings only** (exclude fully-exited symbols)
- **Options strategy ideas** (covered calls, selling puts, etc.) with risk disclosures

This is an **analytics & decision-support** tool. It must **not auto-trade**.

---

## 1) Non-negotiable constraints

### Security & privacy
- **Do NOT ask for or store** brokerage username/password/2FA.
- **No web scraping of logged-in brokerage pages**.
- Primary inputs are **user-provided files** (CSV/PDF).
- Any future broker integration must be via **official OAuth/API** only.
- Store data locally (SQLite/DuckDB). Any API keys must be in `.env` and never committed.

### Financial/tax advice scope
- Strategy outputs must be labeled **“educational, not financial/tax advice.”**
- Never claim guaranteed profits.
- Require user confirmation before generating any “trade ticket” template.

### Taxable account realism
- In **taxable**, track holding period (short/long), lots, realized gains/losses.
- In **IRA**, prioritize performance reporting; do not over-focus on capital-gains tax.
- Add warnings where options activity may create frequent **short-term** taxable outcomes in taxable accounts.

### Wash sale cross-account risk (REQUIRED guardrail)
The system must include a **Wash Sale Risk** detector across **all** user accounts.

MVP scope (informational flags only):
- Detect same-ticker “replacement buys” within +/-30 days around a **taxable loss** sale, across accounts.
- Provide warnings; do NOT attempt full “substantially identical” logic in MVP.
- Warn that broker wash-sale reporting may be incomplete across accounts/brokers.

UI requirement:
- A dedicated **Wash Sale Risk** page
- Inline warnings on symbol P&L pages and trade review pages

---

## 2) Tech choice (default)
Use **Python** (analytics-heavy).

Suggested stack:
- Python 3.11+
- Data: `pandas`, `numpy`
- Local DB: **SQLite** (SQLModel/SQLAlchemy) or **DuckDB**
- UI: **Streamlit** (fastest to ship)
- Charts: `plotly` or `matplotlib`
- PDF parsing (optional): `pdfplumber` first; `camelot`/`tabula` fallback
- Market data + events/news: pluggable provider interfaces with caching + rate limiting

---

## 3) Multi-account support (REQUIRED)

### Account types
- `TAXABLE`
- `TRAD_IRA`
- `ROTH_IRA`
(Allow future types: `401K`, `HSA`, etc.)

### Core rule
Every record MUST include:
- `account_id` (internal UUID)
- `account_type` (enum)
- `broker` (string; e.g., Webull/Fidelity/Schwab)
- `account_label` (user-friendly name)

### Reporting views
Every report page must support:
- filter by single account
- consolidated view (all accounts)
- optional grouping by account_type

---

## 4) Deliverables (MVP-first)

### MVP = file import + core reports
1) Import **trade history CSV**
2) Import **cash activity CSV** (ACH deposits/withdrawals)
3) Compute:
   - Realized P&L by symbol (stocks + options)
   - Unrealized P&L for current open positions
   - Fees/commissions
   - Net contributions (“money invested”)
4) Streamlit pages:
   - Overview
   - Holdings
   - P&L (by symbol + instrument type)
   - Contributions / Cash flow
   - Calendar (daily realized P&L)
   - Wash Sale Risk (basic)
   - Settings + Data Quality checks

### Phase 2 = benchmarks + time windows
- Performance comparison vs benchmarks for:
  - Since inception
  - 1Y / 6M / 3M / 1M / 5D
- Provide:
  - Money-weighted return (XIRR) using contributions
  - Time-weighted return later (needs periodic valuation reconstruction)

### Phase 3 = enrichment (earnings, macro, news)
- Earnings calendar for **current holdings**
- Macro event calendar
- News feed for **current holdings only**
- Deduplicate news; default 7–14 days

### Phase 4 = strategy assistant (options)
- Generate candidate ideas:
  - Covered calls (if shares held)
  - Cash-secured puts (if cash/buying power available)
  - (Optional later) spreads
- Each idea must include:
  - Max gain/loss, breakeven, assignment scenarios
  - Earnings/event proximity warnings
  - Collateral/buying power requirements
  - “Educational only” disclaimer
- No auto-trading.

---

## 5) Inputs & ingestion requirements

### 5.1 Trade History CSV (required)
Implement a robust `csv_normalizer`:
- Reads header row
- Maps known columns to canonical fields using broker templates
- If unknown columns: provide a “mapping UI” to map once and save mapping

Canonical trade fields (internal schema):
- `broker` (string)
- `account_id` (UUID)
- `account_type` (enum)
- `trade_id` (string, optional)
- `executed_at` (datetime)
- `instrument_type`: `STOCK` | `OPTION`
- Equities:
  - `symbol` (string)
  - `side`: `BUY` | `SELL`
- Options:
  - `option_symbol_raw` (string)
  - `underlying` (string)
  - `expiration` (date)
  - `strike` (float)
  - `call_put`: `C` | `P`
  - `side`: `BTO` | `STO` | `BTC` | `STC`
  - `multiplier` (default 100 if missing)
- Shared:
  - `quantity` (float/int)
  - `price` (float)
  - `fees` (float)
  - `net_amount` (float signed cash impact; compute if missing)
  - `currency` (default USD)

Normalization rules:
- Buys reduce cash; sells increase cash (signed `net_amount`)
- Options qty = contracts; `multiplier` applied to notional
- Parse options when possible; flag unparsed options in Data Quality page

### 5.2 Cash activity / transfers CSV (required)
Goal: compute net contributions (“money invested”) and detect internal transfers.

Canonical cash fields:
- `broker` (string)
- `account_id` (UUID)
- `account_type` (enum)
- `posted_at` (datetime/date)
- `type`: `DEPOSIT` | `WITHDRAWAL`
- `amount` (positive float)
- `description` (string)
- `source`: `ACH` | `wire` | `transfer` | etc.
- `is_external` (bool; user-confirmed)
- `transfer_group_id` (optional, to match internal transfers)

Definitions:
- External contribution = bank <-> brokerage/IRA (counts)
- Internal transfer = between user accounts/brokers (does NOT count as new money)

Implementation:
- Provide tagging UI to classify `is_external`
- Default guess via description keywords; require user confirmation

### 5.3 Annual tax report PDF (optional fallback)
Purpose: recover missing realized gains/losses when CSV history is incomplete.

Pipeline:
1) Extract tables with `pdfplumber`
2) Detect 1099-B sections (Description, Date Acquired, Date Sold, Proceeds, Cost, Wash Sale, Gain/Loss)
3) Output extracted rows to a review UI for confirmation/editing

Rules:
- Treat PDF-imported data as “needs review”
- Prefer CSV for detailed per-trade and per-day analysis

OCR:
- Optional; warn about accuracy when PDF is scanned

---

## 6) Core calculations

### 6.1 Positions & lots engine
Maintain open lots for:
- Stocks: FIFO default (add LIFO/Specific Lot later)
- Options: match opens/closes by contract spec (underlying/exp/strike/type)

Handle:
- Partial closes
- Fees and contract multipliers
- Flag assignments/exercises if broker data provides; otherwise defer

Outputs:
- Realized P&L by close date
- Unrealized P&L for open positions using latest prices
- Current holdings = open qty != 0

### 6.2 “Save the loss” report
For each symbol (stocks + options):
- Realized P&L YTD and by selected window
- Unrealized P&L open lots
- Largest loss lots + holding period (taxable only)
- “Harvest candidate” flags (informational; no advice)

### 6.3 Net invested (contributions)
Compute:
- Net contributions since inception
- Contributions by month/year
- Separate by account and consolidated

### 6.4 Benchmark comparison
Use ETF proxies by default (simplifies data):
- DJI proxy: DIA
- SPX proxy: SPY
- IXIC proxy: QQQ (note: Nasdaq-100 proxy)

Compute vs:
- Since inception (first external deposit date)
- 1Y/6M/3M/1M/5D

Return methods:
- Money-weighted return (XIRR): contributions as cash flows + terminal value
- Time-weighted later when daily valuations are supported

### 6.5 P&L calendar
MVP:
- Daily realized P&L calendar (close date)
- Daily fees
Phase 2+:
- Daily total P&L (requires daily portfolio valuation reconstruction)

### 6.6 Wash sale risk detector (MVP)
For taxable loss sale of ticker T on date D:
- Find any buy of ticker T in any account within [D-30, D+30]
- Flag as “wash sale risk (cross-account)”
- Provide contextual explanation and link to the relevant trades

---

## 7) Providers & enrichment (pluggable)
Interfaces:
- `PriceProvider.get_history(symbol, start, end, interval="1d")`
- `PriceProvider.get_quote(symbol)`
- `EventsProvider.get_earnings_calendar(symbols, start, end)`
- `EventsProvider.get_macro_calendar(start, end)`
- `NewsProvider.get_news(symbols, start, end)`

Requirements:
- Local caching
- Rate limiting
- Provider can be swapped without touching analytics core

News requirements:
- Current holdings only (open positions)
- Deduplicate; default lookback 7–14 days
- Filter out symbols fully exited

---

## 8) Options strategy assistant (Phase 4)

### Account-aware policy settings
Per account:
- `tax_sensitivity`: HIGH (taxable) / LOW (IRA)
- `turnover_preference`: LOW/MED/HIGH
- `allowed_strategies`: checkboxes (covered calls, CSP, spreads, naked)
- `risk_limit`: max % collateral / max contracts / max notional
Defaults:
- TAXABLE: tax_sensitivity HIGH, turnover LOW, naked disabled
- IRA: tax_sensitivity LOW, turnover MED/HIGH allowed, naked disabled by default

Outputs:
- “Idea cards” only; no trade execution
- Include max gain/loss, breakeven, assignment risk, event risk
- Warn about unlimited risk for naked calls
- Warn about wash sale risk when ideas imply replacement buys near taxable loss sales

---

## 9) UI requirements (Streamlit)

Global:
- Add a global account selector:
  - “All accounts (consolidated)”
  - each individual account
- Every page respects this filter.

Pages (MVP):
1) Overview
2) Holdings
3) P&L
4) Contributions
5) Calendar
6) Wash Sale Risk
7) Settings / Data Quality

Phase 2+:
8) Benchmarks
9) Events & News
10) Strategies

---

## 10) Repo structure (suggested)
```
portfolio-assistant/
  AGENTS.md
  README.md
  DATA_IMPORTS.md
  REPORTS.md
  SECURITY.md
  ROADMAP.md
  TASKS.md
  pyproject.toml
  .env.example
  src/
    app_streamlit.py
    core/
      models.py
      db.py
      ingest/
        csv_import.py
        csv_mapping.py
        pdf_import.py
        validators.py
      analytics/
        pnl_engine.py
        lots.py
        contributions.py
        benchmarks.py
        calendar.py
        wash_sale.py
      providers/
        prices_base.py
        events_base.py
        news_base.py
      strategy/
        covered_calls.py
        short_puts.py
        risk.py
      utils/
        dates.py
        money.py
        logging.py
  tests/
    test_ingest.py
    test_pnl_engine.py
    test_contributions.py
    test_wash_sale.py
```

---

## 11) Definition of done (MVP)
MVP is done when the user can:
1) Create multiple accounts (taxable + IRA), label them, and import files per account
2) Import trade CSV + cash CSV
3) See:
   - Realized P&L by symbol (stocks + options)
   - Unrealized P&L for open positions
   - Net contributions (external deposits minus withdrawals)
   - Daily realized P&L calendar
   - Wash sale risk warnings (basic cross-account ticker checks)
4) Export reports to CSV (and optionally PDF)
