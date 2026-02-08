# Codex app runbook (worktree + conda)

## Why worktrees
Worktrees let Codex run multiple independent tasks without conflicting with your main checkout.

## Step-by-step
1) In your main repo folder, commit the instruction/spec files.
2) Open the repo in the Codex app.
3) Start a new thread and select **Worktree** as the target.
4) Use one worktree thread per role:
   - DB/Ingest agent
   - Tax/WashSale agent
   - UI agent
   - GPT features agent

## Local environments (recommended)
Because worktrees are separate directories, dependencies may be missing. Use Codex app “Local environments”:
- Add a setup script that ensures your conda env exists and deps are installed.
- Add actions for:
  - Run app
  - Run tests

### Suggested conda setup script (example)
Adjust env name to your preference.

```bash
ENV_NAME="portfolio_assistant"
if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  conda env update -n "$ENV_NAME" -f environment.yml --prune
else
  conda env create -n "$ENV_NAME" -f environment.yml
fi
```

### Suggested Codex actions (example)
```bash
# Run app
conda run -n portfolio_assistant streamlit run src/app_streamlit.py

# Run tests
conda run -n portfolio_assistant pytest -q
```

## Merge strategy
Merge in this order to reduce conflicts:
1) DB/Ingest PR
2) Tax/WashSale PR
3) UI PR
4) GPT features PR

## Common gotchas
- Worktrees won’t see uncommitted local changes → commit instruction/spec files first.
- Don’t run two Codex threads editing the same files at the same time.
