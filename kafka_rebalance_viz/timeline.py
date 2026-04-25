"""Timeline rendering for Kafka rebalance events."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from kafka_rebalance_viz.parser import ParseResult, RebalanceEvent


@dataclass
class TimelineEntry:
    """A single entry in the rendered timeline."""

    timestamp: Optional[datetime]
    event_type: str
    member_id: Optional[str]
    label: str
    offset_seconds: Optional[float] = None


@dataclass
class Timeline:
    """Ordered collection of timeline entries with metadata."""

    entries: List[TimelineEntry] = field(default_factory=list)
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    @property
    def span_seconds(self) -> Optional[float]:
        if self.start and self.end:
            return (self.end - self.start).total_seconds()
        return None


def _make_label(event: RebalanceEvent) -> str:
    parts = [event.event_type.upper()]
    if event.member_id:
        short = event.member_id[:16] + "..." if len(event.member_id) > 16 else event.member_id
        parts.append(f"member={short}")
    if event.group_id:
        parts.append(f"group={event.group_id}")
    return " | ".join(parts)


def build_timeline(result: ParseResult) -> Timeline:
    """Convert a ParseResult into a Timeline of ordered entries."""
    events = sorted(
        (e for e in result.events if e.timestamp is not None),
        key=lambda e: e.timestamp,  # type: ignore[arg-type]
    )
    # Append events without timestamps at the end, preserving original order
    no_ts = [e for e in result.events if e.timestamp is None]

    timeline = Timeline()
    if events:
        timeline.start = events[0].timestamp
        timeline.end = events[-1].timestamp

    for event in events + no_ts:
        offset: Optional[float] = None
        if timeline.start and event.timestamp:
            offset = (event.timestamp - timeline.start).total_seconds()
        entry = TimelineEntry(
            timestamp=event.timestamp,
            event_type=event.event_type,
            member_id=event.member_id,
            label=_make_label(event),
            offset_seconds=offset,
        )
        timeline.entries.append(entry)

    return timeline


def render_timeline(timeline: Timeline, width: int = 60) -> str:
    """Render a Timeline as a human-readable ASCII string."""
    lines: List[str] = []
    span = timeline.span_seconds or 0.0
    bar_width = max(width - 30, 10)

    lines.append(f"{'TIMESTAMP':<26}  {'EVENT':<30}")
    lines.append("-" * (26 + 2 + 30))

    for entry in timeline.entries:
        ts_str = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S") if entry.timestamp else "(no timestamp)   "
        bar = ""
        if span > 0 and entry.offset_seconds is not None:
            pos = int((entry.offset_seconds / span) * (bar_width - 1))
            bar = " " * pos + "*"
        label = entry.label[:50]
        lines.append(f"{ts_str:<26}  {label:<50}  {bar}")

    if timeline.span_seconds is not None:
        lines.append("")
        lines.append(f"Total span: {timeline.span_seconds:.2f}s over {len(timeline.entries)} event(s)")

    return "\n".join(lines)
