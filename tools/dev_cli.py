from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portfolio_assistant.config.paths import (
    BACKUP_DIR,
    DATA_DIR,
    EXPORTS_DIR,
    IMPORTS_DIR,
    PRIVATE_DIR,
    ensure_data_dirs,
)


def _cmd_init_db(_: argparse.Namespace) -> int:
    from portfolio_assistant.db.migrate import migrate

    migrate()
    print("Initialized database schema.")
    return 0


def _cmd_paths(_: argparse.Namespace) -> int:
    ensure_data_dirs()
    print(f"DATA_DIR={DATA_DIR}")
    print(f"IMPORTS_DIR={IMPORTS_DIR}")
    print(f"PRIVATE_DIR={PRIVATE_DIR}")
    print(f"EXPORTS_DIR={EXPORTS_DIR}")
    print(f"BACKUP_DIR={BACKUP_DIR}")
    return 0


def _cmd_run_app(_: argparse.Namespace) -> int:
    cmd = [
        "streamlit",
        "run",
        str(REPO_ROOT / "src/portfolio_assistant/ui/streamlit/app.py"),
    ]
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


def _cmd_run_agents(args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "tools/multi_agent_workflow.py"),
        "--mode",
        args.mode,
    ]
    if args.task:
        cmd.extend(["--task", args.task])
    if args.dry_run:
        cmd.append("--dry-run")
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Portfolio Assistant developer CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sp_init_db = subparsers.add_parser("init-db", help="Create/update local database schema")
    sp_init_db.set_defaults(func=_cmd_init_db)

    sp_paths = subparsers.add_parser("paths", help="Print configured project paths")
    sp_paths.set_defaults(func=_cmd_paths)

    sp_run = subparsers.add_parser("run-app", help="Run Streamlit app")
    sp_run.set_defaults(func=_cmd_run_app)

    sp_agents = subparsers.add_parser(
        "run-agents", help="Run Agents SDK orchestrator (DB optimization or full phase)"
    )
    sp_agents.add_argument(
        "--mode",
        choices=["db-opt", "phase1"],
        default="db-opt",
        help="Workflow profile to run.",
    )
    sp_agents.add_argument(
        "--task",
        default="",
        help="Optional custom coordinator task prompt.",
    )
    sp_agents.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved orchestrator config without running agents.",
    )
    sp_agents.set_defaults(func=_cmd_run_agents)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
