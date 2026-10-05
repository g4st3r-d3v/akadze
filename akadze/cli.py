"""CLI entrypoints: worker, beat, migrate (stubs until core lands)."""

from __future__ import annotations

import argparse
import sys

from akadze.__about__ import __version__


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="akadze", description="Postgres-backed task queue")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("worker", help="Run a worker loop (claim + execute tasks)")
    sub.add_parser("beat", help="Run the scheduler (enqueue due periodic tasks)")
    sub.add_parser("migrate", help="Apply schema migrations")

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    print(
        f"akadze {__version__}: `{args.command}` is not implemented yet. "
        "See kb/ for the plan.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
