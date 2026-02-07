# DATA_IMPORTS.md — File formats & mapping

This project is designed to be broker-agnostic. Imports should work via:
1) broker templates (known column sets), and
2) a mapping UI when columns differ.

## 1) Trades CSV (required)
Minimum columns to compute P&L:
- execution datetime
- symbol / option details
- side (buy/sell or BTO/STO/BTC/STC)
- quantity
- price
- fees (optional but recommended)

### Canonical fields (internal)
See `AGENTS.md` for the full canonical schema. Key fields:
- `executed_at`
- `instrument_type` (`STOCK`/`OPTION`)
- `symbol` (stocks) or `underlying/expiration/strike/call_put` (options)
- `side`
- `quantity`, `price`, `fees`
- `net_amount` (signed cash impact; compute if missing)
- `account_id`, `account_type`, `broker`

### Option symbol parsing
If the broker exports an option as a single string (e.g., OCC-style symbol), the app should attempt to parse:
- underlying
- expiration
- call/put
- strike
If parsing fails, retain the raw string and surface it on the Data Quality page.

## 2) Cash activity CSV (required for “money invested”)
Purpose: compute net contributions and separate external vs internal transfers.

Minimum columns:
- posted date
- type (deposit/withdrawal)
- amount
- description

The app should provide a review UI that allows the user to mark which lines are:
- External (bank ↔ brokerage): counts toward contributions
- Internal transfers (between user accounts): does NOT count

## 3) Annual tax report PDF (optional fallback)
Use only when CSV history is incomplete.

Target sections (if present):
- 1099-B realized gain/loss tables (description, dates, proceeds, cost basis, gain/loss, wash sale)

Outputs must be presented as “needs review,” since PDF parsing can be unreliable.

OCR should be optional and clearly labeled as best-effort.
