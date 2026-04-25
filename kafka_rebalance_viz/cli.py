"""CLI entry point for kafka-rebalance-viz."""

import argparse
import sys
from pathlib import Path

from kafka_rebalance_viz.parser import parse_log
from kafka_rebalance_viz.analyzer import analyze
from kafka_rebalance_viz.formatter import format_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka-rebalance-viz",
        description="Visualize and analyze Kafka consumer group rebalancing events from logs.",
    )
    parser.add_argument(
        "log_file",
        metavar="LOG_FILE",
        type=Path,
        help="Path to the Kafka log file to analyze.",
    )
    parser.add_argument(
        "--group",
        metavar="GROUP_ID",
        default=None,
        help="Filter events by consumer group ID.",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each parsed rebalance event before the summary.",
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_file: Path = args.log_file
    if not log_file.exists():
        print(f"Error: file not found: {log_file}", file=sys.stderr)
        return 1
    if not log_file.is_file():
        print(f"Error: not a regular file: {log_file}", file=sys.stderr)
        return 1

    try:
        text = log_file.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"Error reading file: {exc}", file=sys.stderr)
        return 1

    parse_result = parse_log(text)
    summary = analyze(parse_result, group_filter=args.group)

    if args.verbose:
        for event in parse_result.events:
            if args.group and event.group_id != args.group:
                continue
            print(event)
        print()

    if args.output == "json":
        import json
        import dataclasses
        print(json.dumps(dataclasses.asdict(summary), indent=2, default=str))
    else:
        print(format_summary(summary))

    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
