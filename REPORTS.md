# REPORTS.md — What reports the app should produce

## MVP reports
### 1) Overview
- Total portfolio value (by account + consolidated)
- Realized P&L (YTD + selected window)
- Unrealized P&L
- Fees
- Net contributions

### 2) Holdings
- Current holdings per account and consolidated
- Quantity, average cost, market value, unrealized P&L
- For options: contract details (underlying/exp/strike/type)

### 3) P&L by symbol
- Separate tables for stocks and options
- Realized vs unrealized P&L
- Largest winners/losers
- “Save the loss” (harvest candidate flags) — informational only

### 4) Contributions
- External deposits - withdrawals = net invested
- Charts by month/year
- Breakdowns by account

### 5) Calendar (daily realized P&L)
- Daily realized P&L heatmap / calendar view
- Daily fees
- Daily trade count

### 6) Wash Sale Risk (MVP)
- For taxable loss sales: show replacement-buy warnings within +/- 30 days across all accounts
- Link to the specific trades that triggered the flag

## Phase 2+
### Benchmarks
- Compare portfolio performance vs proxies:
  - DIA (DJI proxy), SPY (SPX proxy), QQQ (IXIC proxy)
- Windows: inception, 1Y, 6M, 3M, 1M, 5D
- Money-weighted return (XIRR) and (later) time-weighted

## Phase 3+
### Events & News
- Weekly earnings calendar for current holdings
- Weekly macro events list
- News feed for current holdings only; exclude fully exited symbols

## Phase 4+
### Strategy ideas (options)
- Covered calls / cash-secured puts (default)
- Risk metrics, assignment scenarios, event proximity warnings
- Educational only; no auto trading
