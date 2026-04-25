"""ASCII timeline renderer for rebalance events."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from kafka_rebalance_viz.timeline import Timeline, TimelineEntry, span_seconds

BAR_CHAR = "█"
EMPTY_CHAR = "░"
DEFAULT_WIDTH = 60


@dataclass
class RenderedTimeline:
    header: str
    rows: List[str]
    footer: str

    def __str__(self) -> str:
        lines = [self.header] + self.rows + [self.footer]
        return "\n".join(lines)


def _render_bar(entry: TimelineEntry, timeline: Timeline, width: int) -> str:
    """Render a single timeline entry as an ASCII bar."""
    total = span_seconds(timeline)
    if total <= 0:
        bar = BAR_CHAR
    else:
        entry_start = (entry.timestamp - timeline.start).total_seconds()
        entry_span = max(entry.duration_seconds or 0.5, 0.5)
        offset = int((entry_start / total) * width)
        length = max(int((entry_span / total) * width), 1)
        offset = min(offset, width - 1)
        length = min(length, width - offset)
        bar = (
            EMPTY_CHAR * offset
            + BAR_CHAR * length
            + EMPTY_CHAR * max(width - offset - length, 0)
        )
    label = f" {entry.label}"
    return f"|{bar}|{label}"


def render_timeline(timeline: Timeline, width: int = DEFAULT_WIDTH) -> RenderedTimeline:
    """Render a Timeline into an ASCII RenderedTimeline."""
    if not timeline.entries:
        return RenderedTimeline(
            header="No timeline entries to display.",
            rows=[],
            footer="",
        )

    start_str = timeline.start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = timeline.end.strftime("%Y-%m-%d %H:%M:%S")
    total = span_seconds(timeline)
    header = f"Timeline: {start_str} → {end_str}  (span: {total:.1f}s)"
    separator = "+" + "-" * width + "+"

    rows: List[str] = [separator]
    for entry in timeline.entries:
        rows.append(_render_bar(entry, timeline, width))
    rows.append(separator)

    footer = f"Total events: {len(timeline.entries)}"
    return RenderedTimeline(header=header, rows=rows, footer=footer)
