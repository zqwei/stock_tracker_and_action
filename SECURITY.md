# SECURITY.md — Data handling & safety requirements

## Core principles
- Local-first: store data in a local database (SQLite/DuckDB).
- Never store or request brokerage usernames/passwords/2FA.
- Avoid scraping logged-in brokerage pages.

## Secrets
- Store API keys (news/events/price providers) only in `.env`.
- Do not commit `.env` or any token file into git.
- Never print tokens to logs.

## Least privilege
- If broker APIs are added later, start with read-only scope and require user confirmation for any action.

## Sensitive outputs
- Do not include account numbers or full statements in UI exports.
- For troubleshooting exports, redact personally identifying data by default.
