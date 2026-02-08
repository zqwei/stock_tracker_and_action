# Setup notes (Codex + Agents SDK + API)

## Codex app
- Codex app supports worktrees and local environments; good for “multi-agent” via parallel threads.

## OpenAI API (needed for GPT features and Agents SDK)
- For in-app GPT features (Responses API + web search + function calling), you need an OpenAI API key.
- For Agents SDK orchestration, you also need an API key.

## Recommended repo files
- environment.yml (conda) committed
- .env (NOT committed)
- .gitignore includes: *.db, *.sqlite, *.csv, *.pdf, data/private/, .env

## Personal data durability
- Keep the DB + imports in a local data directory outside the repo.
- Back up that directory to Dropbox (or similar synced storage).
