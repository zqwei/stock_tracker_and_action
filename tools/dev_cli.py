"""Local developer CLI for common project actions."""

from __future__ import annotations

import argparse

from portfolio_assistant.db.migrate import run_migrations


def main() -> None:
    parser = argparse.ArgumentParser(description="Portfolio Assistant development CLI")
    parser.add_argument("command", choices=["migrate"], help="Action to run")
    args = parser.parse_args()

    if args.command == "migrate":
        run_migrations()
        print("Migrations complete")


if __name__ == "__main__":
    main()
