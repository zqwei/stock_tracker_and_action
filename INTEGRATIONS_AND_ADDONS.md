# Integrations & add-ons

This file defines post-Phase-1 add-ons that must remain isolated from core import/P&L flows.

## Scope and gating
- Phase 1 core pages and calculations remain the default workflow.
- Integrations are opt-in via feature flags:
  - `ENABLE_ASK_GPT=false` (default)
  - `ENABLE_DAILY_BRIEFING=false` (default)
  - `ENABLE_WEB_MODE=false` (default)
- If disabled, no GPT/web calls are made by the app.

## Ask GPT (read-only assistant)
- Implemented as an in-app page with strict guardrails:
  - read-only function tools only
  - row-limited outputs (`MAX_TOOL_ROWS`)
  - optional account-scope enforcement via global account filter
- Allowed tools are portfolio analytics reads (accounts, positions, trades, cash, wash-sale flags).
- Prohibited:
  - writing DB rows
  - storing credentials
  - any trade execution behavior
- Response guidance must explicitly state:
  - educational only, not financial/tax advice
  - no guaranteed outcomes

## Optional web-enabled mode
- Web context is disabled by default and only enabled when both:
  - `ENABLE_WEB_MODE=true`
  - user toggles web mode in UI
- When web mode is used, show source visibility in UI from model citations.
- Web mode must not bypass guardrails above.

## Daily briefing pipeline scaffold
- Deterministic layer (local code):
  - snapshot totals
  - risk checks (wash-sale flags, concentration, missing tags/prices, large losses)
  - protective actions list
- Optional GPT narrative layer:
  - can summarize the deterministic payload
  - can use optional web mode with citations if enabled
- Persist briefing artifacts locally under `data/private/briefings/*.json`.

## Non-negotiable guardrails
- Never request/store brokerage credentials.
- Never auto-trade or generate executable trade actions.
- Keep all integration features local-first and additive.
