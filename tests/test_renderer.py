"""Tests for kafka_rebalance_viz.renderer."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from kafka_rebalance_viz.renderer import (
    BAR_CHAR,
    DEFAULT_WIDTH,
    EMPTY_CHAR,
    RenderedTimeline,
    render_timeline,
)
from kafka_rebalance_viz.timeline import Timeline, TimelineEntry


def _ts(offset_seconds: float = 0.0) -> datetime:
    base = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    from datetime import timedelta
    return base + timedelta(seconds=offset_seconds)


def _entry(label: str, offset: float = 0.0, duration: float | None = None) -> TimelineEntry:
    return TimelineEntry(timestamp=_ts(offset), label=label, duration_seconds=duration)


def _timeline(*entries: TimelineEntry) -> Timeline:
    lst = list(entries)
    if not lst:
        return Timeline(entries=[], start=_ts(), end=_ts())
    start = min(e.timestamp for e in lst)
    end = max(e.timestamp for e in lst)
    return Timeline(entries=lst, start=start, end=end)


def test_render_empty_timeline_shows_message():
    tl = _timeline()
    result = render_timeline(tl)
    assert "No timeline entries" in result.header
    assert result.rows == []
    assert isinstance(result, RenderedTimeline)


def test_render_str_contains_header_and_footer():
    tl = _timeline(_entry("JOIN", 0), _entry("SYNC", 10))
    result = render_timeline(tl)
    text = str(result)
    assert "Timeline:" in text
    assert "Total events:" in text


def test_render_row_count_matches_entries_plus_separators():
    entries = [_entry("E1", 0), _entry("E2", 5), _entry("E3", 10)]
    tl = _timeline(*entries)
    result = render_timeline(tl)
    # rows = top separator + N entries + bottom separator
    assert len(result.rows) == len(entries) + 2


def test_render_bar_contains_bar_char():
    tl = _timeline(_entry("JOIN", 0, duration=5), _entry("SYNC", 10, duration=5))
    result = render_timeline(tl)
    entry_rows = result.rows[1:-1]
    for row in entry_rows:
        assert BAR_CHAR in row


def test_render_bar_width_respects_default():
    tl = _timeline(_entry("JOIN", 0), _entry("SYNC", 20))
    result = render_timeline(tl)
    for row in result.rows[1:-1]:
        inner = row[1: row.index("|", 1)]
        assert len(inner) == DEFAULT_WIDTH


def test_render_bar_width_custom():
    tl = _timeline(_entry("JOIN", 0), _entry("SYNC", 10))
    result = render_timeline(tl, width=40)
    for row in result.rows[1:-1]:
        inner = row[1: row.index("|", 1)]
        assert len(inner) == 40


def test_render_label_appears_in_row():
    tl = _timeline(_entry("MEMBER_JOIN", 0), _entry("LEADER_ELECTED", 10))
    result = render_timeline(tl)
    entry_rows = result.rows[1:-1]
    assert any("MEMBER_JOIN" in r for r in entry_rows)
    assert any("LEADER_ELECTED" in r for r in entry_rows)


def test_render_footer_shows_correct_count():
    entries = [_entry(f"E{i}", i * 2) for i in range(5)]
    tl = _timeline(*entries)
    result = render_timeline(tl)
    assert "5" in result.footer
