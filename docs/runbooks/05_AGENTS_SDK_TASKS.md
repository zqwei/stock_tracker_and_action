# Agents SDK tasks (handoff-oriented)

## Recommended agent roles
- Coordinator: plans + assigns, enforces file boundaries, merges outputs
- DB/Ingest agent: schema + import + mapping + validators
- Tax/WashSale agent: lot engine + wash sale + tax year report + reconciliation math
- UI agent: Streamlit pages + wiring
- QA agent (optional): tests + fixtures + CI sanity checks

## Minimal orchestrator skeleton (put in tools/multi_agent_workflow.py)
```python
import asyncio, os
from pathlib import Path
from dotenv import load_dotenv
from agents import Agent, Runner, set_default_openai_api
from agents.mcp import MCPServerStdio

def read_specs() -> str:
    parts = []
    for name in ["AGENTS.md","TAX_REPORTING.md","RECONCILIATION.md","TASKS.md"]:
        p = Path(name)
        if p.exists():
            parts.append(f"\\n\\n# {name}\\n" + p.read_text(encoding="utf-8"))
    return "\\n".join(parts)

async def main():
    load_dotenv()
    set_default_openai_api(os.environ["OPENAI_API_KEY"])

    async with MCPServerStdio(
        name="Codex CLI",
        params={"command":"npx","args":["-y","codex","mcp-server"]},
        client_session_timeout_seconds=360000,
    ) as codex:
        specs = read_specs()

        db_agent = Agent(
            name="DB/Ingest",
            instructions="ROLE: DB/Ingest. Implement schema+imports only.\\n" + specs,
            mcp_servers=[codex],
        )
        tax_agent = Agent(
            name="Tax/WashSale",
            instructions="ROLE: Tax. Implement lots+wash sale+tax year+recon only.\\n" + specs,
            mcp_servers=[codex],
        )
        ui_agent = Agent(
            name="UI",
            instructions="ROLE: UI. Implement Streamlit pages only.\\n" + specs,
            mcp_servers=[codex],
        )

        coordinator = Agent(
            name="Coordinator",
            instructions="Plan MVP Phase 1 and hand off to specialists. Keep changes small & testable.\\n" + specs,
            handoffs=[db_agent, tax_agent, ui_agent],
        )

        await Runner.run(coordinator, "Start MVP Phase 1 implementation per TASKS.md.")

if __name__ == "__main__":
    asyncio.run(main())
```

## How to use it
- Run the orchestrator.
- Review what it changed.
- Commit + push + PR as usual.
- Iterate until MVP is complete.
