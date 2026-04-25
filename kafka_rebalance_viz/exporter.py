"""Export rebalance summaries to various output formats."""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Dict

from kafka_rebalance_viz.analyzer import RebalanceSummary


def summary_to_dict(summary: RebalanceSummary) -> Dict[str, Any]:
    """Convert a RebalanceSummary to a plain dictionary suitable for serialization."""
    return {
        "total_events": summary.total_events,
        "event_type_counts": summary.event_type_counts,
        "unique_members": sorted(summary.unique_members),
        "duration_seconds": (
            summary.duration.total_seconds() if summary.duration is not None else None
        ),
        "first_event_ts": (
            summary.first_event_ts.isoformat() if summary.first_event_ts is not None else None
        ),
        "last_event_ts": (
            summary.last_event_ts.isoformat() if summary.last_event_ts is not None else None
        ),
    }


def export_json(summary: RebalanceSummary, indent: int = 2) -> str:
    """Serialize a RebalanceSummary to a JSON string."""
    return json.dumps(summary_to_dict(summary), indent=indent)


def export_csv(summary: RebalanceSummary) -> str:
    """Serialize a RebalanceSummary to CSV with two columns: key, value.

    Event type counts are expanded as individual rows prefixed with 'event_type:'.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["key", "value"])

    data = summary_to_dict(summary)
    scalar_keys = [
        "total_events",
        "duration_seconds",
        "first_event_ts",
        "last_event_ts",
    ]
    for key in scalar_keys:
        writer.writerow([key, data[key]])

    for event_type, count in sorted(data["event_type_counts"].items()):
        writer.writerow([f"event_type:{event_type}", count])

    for member in data["unique_members"]:
        writer.writerow(["member", member])

    return output.getvalue()


def export(summary: RebalanceSummary, fmt: str) -> str:
    """Export a RebalanceSummary in the requested format ('json' or 'csv').

    Raises ValueError for unsupported formats.
    """
    fmt = fmt.lower()
    if fmt == "json":
        return export_json(summary)
    if fmt == "csv":
        return export_csv(summary)
    raise ValueError(f"Unsupported export format: {fmt!r}. Choose 'json' or 'csv'.")
