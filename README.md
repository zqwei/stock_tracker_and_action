# Portfolio Assistant (Local-First)

This repo is intended to be developed with Codex using `AGENTS.md` as the primary project instruction file.

## What this app does (goal)
- Import trades and cash transfers from CSV files (multiple accounts)
- Compute realized/unrealized P&L for stocks and options
- Track net contributions (bank → brokerage)
- Compare performance vs benchmark proxies (DIA/SPY/QQQ)
- Show P&L calendar, earnings/macro events, and news for current holdings
- Generate educational options strategy ideas (no auto-trading)

## Quickstart (once code exists)
> These steps are a template for the project once Codex scaffolds it.

1) Create virtual environment
```bash
python -m venv .venv
source .venv/bin/activate  # mac/linux
# .venv\Scripts\activate   # windows
```

2) Install dependencies
```bash
pip install -r requirements.txt
# or: pip install -e .
```

3) Run the UI
```bash
streamlit run src/app_streamlit.py
```

4) Import data
- Create an account (Taxable / Trad IRA / Roth IRA)
- Upload trade history CSV and cash activity CSV
- Review Data Quality page for any unmapped columns or unparsed options

## Data privacy
- Data is stored locally (SQLite/DuckDB)
- Do not paste brokerage credentials into any prompt
- Use `.env` for API keys (if/when news/events providers are used)

## Development notes
- Project requirements live in `AGENTS.md`. Update that file first when requirements change.
- For Codex app: after editing `AGENTS.md`, start a new thread/run to ensure it reloads the newest instructions.
