# What you do next (short checklist)

## 1) Put project spec files into the repo root (and commit)
Required at repo root:
- AGENTS.md, TAX_REPORTING.md, RECONCILIATION.md, TASKS.md, REPORTS.md, DATA_IMPORTS.md, SECURITY.md

Commit them so Codex worktrees inherit the rules.

## 2) Keep personal data out of Git
- Add/confirm .gitignore entries for:
  - *.db, *.sqlite, *.csv, *.pdf, .env, data/private/
- Decide where DB/backups live:
  - recommended: store backups in your local Dropbox folder (encrypted if you want)

## 3) Choose build workflow
- Option A (simplest): Codex app with worktrees (multi-thread “agents”)
- Option B (advanced): Agents SDK orchestrating Codex CLI via MCP (true master/worker)

## 4) If using Codex app
- Create one worktree thread per “role”:
  - DB/Ingest, Tax/WashSale, UI, (later) GPT features
- Merge PRs in order: DB → Tax → UI → GPT.

## 5) If using Agents SDK
- Create a conda env for the orchestrator
- Add OPENAI_API_KEY in .env
- Run orchestrator script that:
  - starts Codex MCP server
  - runs coordinator → hands off to specialists
