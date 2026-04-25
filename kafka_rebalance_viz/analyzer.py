"""Analyze parsed rebalance events to produce summary statistics."""

from dataclasses import dataclass, field
from collections import defaultdict
from typing import List, Dict, Optional

from kafka_rebalance_viz.parser import ParseResult, RebalanceEvent


@dataclass
class RebalanceSummary:
    total_events: int = 0
    rebalance_starts: int = 0
    rebalance_completions: int = 0
    member_joins: int = 0
    member_leaves: int = 0
    unique_members: List[str] = field(default_factory=list)
    avg_rebalance_duration_ms: Optional[float] = None
    max_rebalance_duration_ms: Optional[float] = None
    min_rebalance_duration_ms: Optional[float] = None
    events_by_type: Dict[str, int] = field(default_factory=dict)


def analyze(parse_result: ParseResult) -> RebalanceSummary:
    """Produce a summary from a ParseResult."""
    summary = RebalanceSummary()
    summary.total_events = len(parse_result.events)

    members: set = set()
    type_counts: Dict[str, int] = defaultdict(int)
    durations: List[float] = []

    pending_starts: Dict[str, RebalanceEvent] = {}

    for event in parse_result.events:
        event_type = event.event_type
        type_counts[event_type] += 1

        if event_type == "REBALANCE_START":
            summary.rebalance_starts += 1
            key = event.member_id or "unknown"
            pending_starts[key] = event

        elif event_type == "REBALANCE_COMPLETE":
            summary.rebalance_completions += 1
            key = event.member_id or "unknown"
            start_event = pending_starts.pop(key, None)
            if start_event and start_event.timestamp and event.timestamp:
                delta = (event.timestamp - start_event.timestamp).total_seconds() * 1000
                durations.append(delta)

        elif event_type == "MEMBER_JOIN":
            summary.member_joins += 1
            if event.member_id:
                members.add(event.member_id)

        elif event_type == "MEMBER_LEAVE":
            summary.member_leaves += 1
            if event.member_id:
                members.add(event.member_id)

    summary.unique_members = sorted(members)
    summary.events_by_type = dict(type_counts)

    if durations:
        summary.avg_rebalance_duration_ms = sum(durations) / len(durations)
        summary.max_rebalance_duration_ms = max(durations)
        summary.min_rebalance_duration_ms = min(durations)

    return summary
