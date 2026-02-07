# RECONCILIATION.md — Checking app tax results against brokerage reporting

## Purpose
Provide a structured workflow and tooling so the user can compare:
- The app’s selected-year realized gain/loss + wash sale totals
vs
- The broker’s year-end reporting (1099-B / realized gain/loss report)

This file is project requirements for Codex (not a tax memo).

---

## Key principles
1) **Reconcile totals first, then drill down.**
2) **Expect differences** between:
   - broker-style wash sale reporting (usually per account)
   - full IRS-style cross-account wash sale logic
3) Wash sales require **boundary data** (Dec/Jan around loss sales).

---

## Required reconciliation features (MVP)
### A) Broker totals input
Support two inputs:
1) **Import broker CSV** (preferred)
2) **Manual totals entry** (fallback)

Minimum broker CSV fields (any subset; map if needed):
- description or symbol
- sale date or close date
- proceeds
- cost basis
- gain/loss
- wash sale disallowed (if provided)
- ST/LT term (if provided)

### B) App vs broker totals comparison
Show side-by-side:
- proceeds
- basis
- gain/loss
- ST/LT splits
- wash sale disallowed totals

### C) Diff drilldowns
Provide diff tables:
- by symbol
- by sale date (if available)
- by term (ST/LT)

### D) Explain likely mismatch causes
Generate a structured checklist with yes/no flags and links:
- “Missing boundary data?” (detect missing trades around year boundary for tickers with wash sale flags)
- “Cross-account replacements likely?” (detect replacement buys in other accounts)
- “Options replacements likely?” (loss sale + BTO call / option acquisition within window)
- “Lot method mismatch?” (if broker export indicates specific lot or has different basis)
- “Corporate actions present?” (if split detected in price history or symbol changes)

### E) Reconciliation packet export
One-click export:
- app 8949-like CSV
- app summary totals
- broker totals (as imported/entered)
- diff tables
- checklist outcomes

---

## Optional (Phase 2+)
- Import broker 1099-B PDF and extract wash sale lines (needs review)
- Rule-based mapping for common broker export formats
- Better “substantially identical” detection beyond ticker (CUSIP if available)
