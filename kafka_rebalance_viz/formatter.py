"""Format a RebalanceSummary for human-readable CLI output."""

from kafka_rebalance_viz.analyzer import RebalanceSummary

_DIVIDER = "-" * 44


def format_summary(summary: RebalanceSummary) -> str:
    """Return a formatted string representation of the rebalance summary."""
    lines = [
        _DIVIDER,
        "  Kafka Rebalance Summary",
        _DIVIDER,
        f"  Total events parsed   : {summary.total_events}",
        f"  Rebalance starts      : {summary.rebalance_starts}",
        f"  Rebalance completions : {summary.rebalance_completions}",
        f"  Member joins          : {summary.member_joins}",
        f"  Member leaves         : {summary.member_leaves}",
        f"  Unique members        : {len(summary.unique_members)}",
    ]

    if summary.unique_members:
        lines.append("")
        lines.append("  Members:")
        for member in summary.unique_members:
            lines.append(f"    - {member}")

    lines.append("")
    lines.append("  Events by type:")
    if summary.events_by_type:
        for event_type, count in sorted(summary.events_by_type.items()):
            lines.append(f"    {event_type:<28}: {count}")
    else:
        lines.append("    (none)")

    lines.append("")
    lines.append("  Rebalance durations (ms):")
    if summary.avg_rebalance_duration_ms is not None:
        lines.append(f"    avg : {summary.avg_rebalance_duration_ms:.2f}")
        lines.append(f"    min : {summary.min_rebalance_duration_ms:.2f}")
        lines.append(f"    max : {summary.max_rebalance_duration_ms:.2f}")
    else:
        lines.append("    n/a (no matched start/complete pairs)")

    lines.append(_DIVIDER)
    return "\n".join(lines)
