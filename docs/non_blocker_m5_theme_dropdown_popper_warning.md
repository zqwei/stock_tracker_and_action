# M5 Non-Blocker Tracking: Theme Selector Popper Console Warning

## Classification
- Severity: `NON_BLOCKER`
- Status: `Tracked`
- Release impact: `No functional impact observed`

## Symptom
Interacting with the sidebar `Color theme` selector logs a browser console warning:

`preventOverflow` modifier is required by `hide` modifier in order to work, be sure to include it before `hide`!

## Exact Repro Commands
1. Start app:

```bash
cd /Users/weiz/Documents/Projects/PersonalStockAssistant
conda run -n portfolio_assistant env PA_DATA_DIR=/tmp/pa_m5_nonblocker_track PYTHONPATH=src streamlit run src/portfolio_assistant/ui/streamlit/app.py --server.headless true --server.port 8775 --browser.gatherUsageStats false
```

2. In a second shell, open app + trigger theme selector:

```bash
export CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
export PWCLI="$CODEX_HOME/skills/playwright/scripts/playwright_cli.sh"
export PLAYWRIGHT_CLI_SESSION=qa_m5_nonblocker
cd /Users/weiz/Documents/Projects/PersonalStockAssistant/output/playwright
"$PWCLI" open http://127.0.0.1:8775 --headed
"$PWCLI" goto http://127.0.0.1:8775
"$PWCLI" snapshot
"$PWCLI" click e85
"$PWCLI" console warning
```

3. Confirm warning in captured log:

```bash
sed -n '1,20p' /Users/weiz/Documents/Projects/PersonalStockAssistant/output/playwright/.playwright-cli/console-2026-02-12T19-01-49-542Z.log
```

## Affected Pages / Components
- Component: sidebar `Color theme` combobox/popper stack.
- Pages: shared sidebar shell (observed on `/`, expected on other pages that use the same sidebar controls).

## Risk Assessment
- Type: cosmetic console warning.
- User-visible behavior: none (theme selection works as expected).
- Functional impact: none observed in import/tax/reconciliation/briefing flows.

## Recommendation
- Decision: `Defer` (do not block release).
- Rationale: no user-facing malfunction; warning is diagnostics noise.
- Follow-up: address in UI polish/dep-maintenance cycle (popper modifier order or upstream dependency alignment), then re-check console cleanliness.
