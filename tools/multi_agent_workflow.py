from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency for runtime
    load_dotenv = None


DEFAULT_SPEC_FILES = [
    "AGENTS.md",
    "TASKS.md",
    "TAX_REPORTING.md",
    "RECONCILIATION.md",
    "INTEGRATIONS_AND_ADDONS.md",
]


@dataclass(frozen=True)
class WorkflowConfig:
    mode: str
    task: str
    timeout_seconds: int
    codex_command: str
    codex_args: list[str]
    spec_files: list[str]
    dry_run: bool


def _read_specs(repo_root: Path, names: list[str]) -> str:
    parts: list[str] = []
    for name in names:
        path = repo_root / name
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        parts.append(f"\n\n# {name}\n{content}")
    return "\n".join(parts).strip()


def _compose_role_instructions(role: str, specs: str, mode: str) -> str:
    common = (
        "Follow repository specs exactly. Keep changes small, testable, and deterministic. "
        "Run lightweight checks before finishing."
    )
    role_map = {
        "db": (
            "ROLE: DB/Ingest specialist. Own schema, migrations, import pipelines, mapping, "
            "validation, and indexing strategy."
        ),
        "tax": (
            "ROLE: Tax/WashSale specialist. Own lots, realized/unrealized P&L, wash-sale risk, "
            "and tax-year report logic."
        ),
        "ui": (
            "ROLE: Streamlit UI specialist. Own page wiring, interaction flow, and data-quality UX."
        ),
        "qa": (
            "ROLE: QA specialist. Add/adjust tests and run focused verification for changed paths."
        ),
        "coordinator": (
            "ROLE: Coordinator. Plan sequence, delegate to specialists, resolve overlaps, "
            "and ensure a coherent final diff."
        ),
    }
    mode_hint = (
        "Priority: database optimization and import throughput." if mode == "db-opt" else
        "Priority: complete Phase 1 MVP tasks end-to-end."
    )
    return f"{role_map[role]}\n{common}\n{mode_hint}\n\n{specs}"


def _resolve_config(args: argparse.Namespace) -> WorkflowConfig:
    default_task = (
        "Optimize database schema and ingestion performance. Propose and implement safe changes, "
        "then run focused checks."
        if args.mode == "db-opt"
        else "Implement Phase 1 MVP tasks from TASKS.md in small, testable increments."
    )
    task = args.task.strip() if args.task else default_task
    return WorkflowConfig(
        mode=args.mode,
        task=task,
        timeout_seconds=args.timeout_seconds,
        codex_command=args.codex_command,
        codex_args=args.codex_args,
        spec_files=args.spec_files,
        dry_run=args.dry_run,
    )


def _print_plan(config: WorkflowConfig, repo_root: Path) -> None:
    print(f"Mode: {config.mode}")
    print(f"Task: {config.task}")
    print(f"Repo: {repo_root}")
    print(f"MCP command: {config.codex_command} {' '.join(config.codex_args)}")
    print(f"Spec files: {', '.join(config.spec_files)}")


async def _run_async(config: WorkflowConfig, repo_root: Path) -> int:
    specs = _read_specs(repo_root=repo_root, names=config.spec_files)
    if not specs:
        raise RuntimeError("No spec files found. Check --spec-files paths.")

    if load_dotenv is not None:
        load_dotenv(dotenv_path=repo_root / ".env")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required in environment or .env file.")

    try:
        from agents import Agent, Runner, set_default_openai_api
        from agents.mcp import MCPServerStdio
    except Exception as exc:  # pragma: no cover - optional dependency at runtime
        raise RuntimeError(
            "Agents SDK dependencies are missing. Install: "
            "'pip install --upgrade openai openai-agents python-dotenv'"
        ) from exc

    # Older/newer SDK versions may vary; env var still configures auth regardless.
    try:
        set_default_openai_api(api_key)
    except Exception:
        pass

    params = {
        "command": config.codex_command,
        "args": config.codex_args,
        "cwd": str(repo_root),
    }

    async with MCPServerStdio(
        name="Codex CLI",
        params=params,
        client_session_timeout_seconds=config.timeout_seconds,
    ) as codex:
        db_agent = Agent(
            name="DB/Ingest",
            instructions=_compose_role_instructions("db", specs, config.mode),
            mcp_servers=[codex],
        )
        qa_agent = Agent(
            name="QA",
            instructions=_compose_role_instructions("qa", specs, config.mode),
            mcp_servers=[codex],
        )

        handoffs = [db_agent, qa_agent]
        if config.mode == "phase1":
            tax_agent = Agent(
                name="Tax/WashSale",
                instructions=_compose_role_instructions("tax", specs, config.mode),
                mcp_servers=[codex],
            )
            ui_agent = Agent(
                name="UI",
                instructions=_compose_role_instructions("ui", specs, config.mode),
                mcp_servers=[codex],
            )
            handoffs = [db_agent, tax_agent, ui_agent, qa_agent]

        coordinator = Agent(
            name="Coordinator",
            instructions=_compose_role_instructions("coordinator", specs, config.mode),
            handoffs=handoffs,
            mcp_servers=[codex],
        )

        result = await Runner.run(coordinator, config.task)
        final_output = getattr(result, "final_output", None)
        if final_output:
            print("\n=== Final Output ===")
            print(final_output)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Agents SDK orchestration for portfolio assistant development."
    )
    parser.add_argument(
        "--mode",
        choices=["db-opt", "phase1"],
        default="db-opt",
        help="Workflow profile to run.",
    )
    parser.add_argument(
        "--task",
        default="",
        help="Custom task prompt for the coordinator.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=360000,
        help="MCP client timeout in seconds.",
    )
    parser.add_argument(
        "--codex-command",
        default="npx",
        help="Command used to launch Codex MCP server.",
    )
    parser.add_argument(
        "--codex-args",
        nargs="+",
        default=["-y", "codex", "mcp-server"],
        help="Arguments for Codex MCP command.",
    )
    parser.add_argument(
        "--spec-files",
        nargs="+",
        default=DEFAULT_SPEC_FILES,
        help="Spec documents injected into agent instructions.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved config and exit without running agents.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = _resolve_config(args)
    repo_root = Path(__file__).resolve().parents[1]

    _print_plan(config=config, repo_root=repo_root)
    if config.dry_run:
        return 0

    try:
        return asyncio.run(_run_async(config=config, repo_root=repo_root))
    except KeyboardInterrupt:
        print("Cancelled.")
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
