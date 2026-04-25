"""CLI entry point for kafka-rebalance-viz."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from kafka_rebalance_viz.analyzer import analyze
from kafka_rebalance_viz.exporter import export
from kafka_rebalance_viz.formatter import format_summary
from kafka_rebalance_viz.parser import parse_log
from kafka_rebalance_viz.timeline import build_timeline, render_timeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka-rebalance-viz",
        description="Visualize and analyze Kafka consumer group rebalancing events from logs.",
    )
    parser.add_argument("log_file", help="Path to the Kafka log file to analyze.")
    parser.add_argument(
        "--output",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--timeline",
        action="store_true",
        help="Render an ASCII timeline of rebalance events.",
    )
    parser.add_argument(
        "--timeline-width",
        type=int,
        default=80,
        dest="timeline_width",
        help="Width of the ASCII timeline bar (default: 80).",
    )
    parser.add_argument(
        "--out-file",
        dest="out_file",
        default=None,
        help="Write output to this file instead of stdout.",
    )
    return parser


def run(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    path = Path(args.log_file)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        return 1
    if not path.is_file():
        print(f"Error: not a file: {path}", file=sys.stderr)
        return 1

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"Error reading file: {exc}", file=sys.stderr)
        return 1

    parse_result = parse_log(text)
    summary = analyze(parse_result)

    if args.output == "text":
        output_text = format_summary(summary)
        if args.timeline:
            tl = build_timeline(parse_result)
            output_text += "\n\n" + render_timeline(tl, width=args.timeline_width)
    elif args.output == "json":
        output_text = export(summary, fmt="json")
    elif args.output == "csv":
        output_text = export(summary, fmt="csv")
    else:
        output_text = format_summary(summary)

    if args.out_file:
        try:
            Path(args.out_file).write_text(output_text, encoding="utf-8")
        except OSError as exc:
            print(f"Error writing output file: {exc}", file=sys.stderr)
            return 1
    else:
        print(output_text)

    return 0


def main() -> None:  # pragma: no cover
    sys.exit(run())
