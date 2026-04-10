from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path

from collector_config import CollectorConfig
from collector_discovery import discover_matched_options
from collector_lark import notify_exit
from collector_planner import build_runtime_plan
from collector_runtime import run_collector


class TerminationRequested(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect matched Binance/Deribit option data.")
    parser.add_argument(
        "command",
        choices=("discover", "run"),
        nargs="?",
        default="run",
        help="discover matched instruments or run the collector",
    )
    parser.add_argument(
        "--underlyings",
        default="BTC",
        help="comma separated underlyings, for example BTC or BTC,ETH",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="base output directory for CSV files",
    )
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> CollectorConfig:
    underlyings = tuple(
        part.strip().upper()
        for part in args.underlyings.split(",")
        if part.strip()
    )
    return CollectorConfig(
        underlyings=underlyings or ("BTC",),
        output_dir=Path(args.output_dir),
    )


def install_signal_handlers() -> dict[int, object]:
    previous_handlers: dict[int, object] = {}

    def _handle_termination(signum, _frame) -> None:
        signal_name = signal.Signals(signum).name
        raise TerminationRequested(f"received {signal_name}")

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, _handle_termination)

    return previous_handlers


def restore_signal_handlers(previous_handlers: dict[int, object]) -> None:
    for signum, handler in previous_handlers.items():
        signal.signal(signum, handler)


def main() -> int:
    args = parse_args()
    config = build_config(args)
    exit_code = 0
    reason = "completed normally"
    previous_handlers = install_signal_handlers()

    try:
        if args.command == "discover":
            matches = discover_matched_options(config)
            plan = build_runtime_plan(matches, config)
            print(
                json.dumps(
                    {
                        "underlyings": list(config.underlyings),
                        "match_count": len(matches),
                        "binance_group_count": len(plan.binance_groups),
                        "deribit_group_count": len(plan.deribit_groups),
                        "matches": [match.__dict__ for match in matches],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        asyncio.run(run_collector(config))
        return 0
    except TerminationRequested as exc:
        exit_code = 130
        reason = str(exc)
        return exit_code
    except KeyboardInterrupt:
        exit_code = 130
        reason = "received SIGINT"
        return exit_code
    except Exception as exc:
        exit_code = 1
        reason = f"{type(exc).__name__}: {exc}"
        print(reason, file=sys.stderr)
        return exit_code
    finally:
        try:
            notify_exit(
                command=args.command,
                underlyings=config.underlyings,
                exit_code=exit_code,
                reason=reason,
            )
        except Exception as exc:
            print(f"Lark notification failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        restore_signal_handlers(previous_handlers)


if __name__ == "__main__":
    raise SystemExit(main())
