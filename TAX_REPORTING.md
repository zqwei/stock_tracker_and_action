# TAX_REPORTING.md — Tax-year realized gains/losses + wash sale computation

## Goal
Generate a **selected-year** realized gain/loss report for taxable accounts that is suitable for year-end
reconciliation against broker tax packages (e.g., 1099-B / realized gain/loss reports).

This module is informational and for reconciliation/support. It does not replace tax preparation.

---

## Wash sales (high level)
A wash sale occurs when you sell stock/securities at a loss and within 30 days before or after the sale you
acquire substantially identical stock/securities (including via a contract/option to acquire).

When a wash sale happens, the loss is **disallowed** and generally **added to the basis** of the replacement
shares (deferred loss). That deferral may later be recognized when the replacement shares are sold.

Special cross-account case:
- If the replacement purchase is in an IRA/Roth IRA, the loss may be disallowed and may not increase IRA basis (effectively forfeited).
This should be flagged clearly in the UI as “permanently disallowed due to IRA replacement” (informational).

---

## Implementation requirements
### Data requirement
Wash sales use a 61-day window around the loss sale date (30 days before/after).
So to compute year Y:
- You must have trades spanning at least late year Y-1 through early year Y+1 for affected tickers,
or ideally full trade history.

### Computation modes (both required)
1) **Broker-style (reconciliation)**:
   - Apply wash sales only within the same account and same security identifier (CUSIP if present; else ticker).
   - This helps the user match the brokerage’s reported wash sale amounts.

2) **IRS-style (compliance)**:
   - Apply across all taxable accounts (and flag IRA-triggered disallowance scenarios).
   - Produce a separate “difference” report explaining why it diverges from broker-style.

---

## Suggested algorithm (lot-level)
Maintain lots with adjustable basis and “wash sale adjustments” ledger entries.

When processing a taxable loss sale:
1) Determine loss per share/contract.
2) Identify replacement acquisitions inside the window:
   - Prior purchases in the 30 days before
   - Future purchases in the 30 days after
   - Options as replacement: minimum support includes buy-to-open calls as replacement intent
3) Determine the number of shares “replaced” (min sold-at-loss shares vs replacement shares).
4) Disallow that portion of the loss and allocate it to replacement lots:
   - Increase basis of replacement lots by the disallowed loss allocated to them
   - Record adjustment entries with a reference to the loss sale that generated it
5) The loss sale row should be tagged with wash-sale code W and the disallowed amount.

Edge cases that must be handled:
- More/less shares bought than sold (partial replacement)
- Multiple replacement lots (allocate in chronological order of acquisition)
- Year boundaries (replacement in Jan for a Dec loss sale)
- Options contracts (contract multiplier and identification)

---

## Outputs
### 8949-like detail rows (exportable)
For each realized disposition in the selected year:
- description
- date acquired
- date sold
- proceeds
- basis
- adjustment codes (W, etc.)
- adjustment amount
- gain/loss

### Summary
- ST gain/loss totals
- LT gain/loss totals
- total wash-sale disallowed loss

### Year-end lot snapshot
List open lots at 12/31 with adjusted basis and holding period info (taxable only).

---

## Validation & reconciliation
Provide a reconciliation UI that can:
- import broker year-end realized gain/loss exports (CSV if available; PDF parsing optional)
- compare totals (ST/LT, wash sale disallowed)
- show a diff-by-symbol and diff-by-trade

Display common mismatch causes:
- missing trade history around year boundary
- cross-account replacements not captured by broker
- different lot selection method (specific ID vs FIFO)
- corporate actions or assignments not represented in imports
