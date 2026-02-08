# Agents SDK runbook (conda, first-time)

## What this is
Agents SDK is a Python/TypeScript library for building multi-agent workflows (handoffs, tools, traces).
For development orchestration, you can run Codex CLI as an MCP server and let Agents SDK coordinate “specialist agents”.

## Requirements
- Python 3.10+
- Node.js 18+ (for `npx`)
- OpenAI API key in a local `.env` file
- Codex CLI available to run (via `npx ... codex ...`)

## Step-by-step (conda-first)
1) Create an orchestrator conda env
```bash
conda create -n portfolio_agents python=3.11 -y
conda activate portfolio_agents
```

2) Install dependencies
```bash
pip install --upgrade openai openai-agents python-dotenv
```

3) Create `.env` in the orchestrator working directory
```bash
printf "OPENAI_API_KEY=sk-..." > .env
```

4) Run Codex CLI as MCP server (Agents SDK will do this for you)
- You’ll launch `npx -y codex mcp-server` from Python using MCPServerStdio.

5) Run the orchestrator script
```bash
python tools/multi_agent_workflow.py
```

## Suggested approach
- Keep the orchestrator in your repo under `tools/`
- Each “agent” uses Codex MCP to edit code in the repo, run tests, and produce changes
- You still review/commit/PR like normal
