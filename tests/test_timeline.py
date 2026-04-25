"""Tests for kafka_rebalance_viz.timeline."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from kafka_rebalance_viz.parser import ParseResult, RebalanceEvent
from kafka_rebalance_viz.timeline import (
    Timeline,
    TimelineEntry,
    build_timeline,
    render_timeline,
)


def _make_event(
    event_type: str,
    ts: Optional[datetime] = None,
    member_id: Optional[str] = None,
    group_id: Optional[str] = None,
) -> RebalanceEvent:
    return RebalanceEvent(
        event_type=event_type,
        timestamp=ts,
        member_id=member_id,
        group_id=group_id,
        raw_line=f"raw {event_type}",
    )


def _ts(second: int) -> datetime:
    return datetime(2024, 1, 1, 12, 0, second, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# build_timeline
# ---------------------------------------------------------------------------

def test_build_timeline_empty_result():
    result = ParseResult(events=[], errors=[])
    tl = build_timeline(result)
    assert tl.entries == []
    assert tl.start is None
    assert tl.end is None
    assert tl.span_seconds is None


def test_build_timeline_orders_by_timestamp():
    events = [
        _make_event("rebalance_start", _ts(30)),
        _make_event("member_join", _ts(10)),
        _make_event("rebalance_end", _ts(50)),
    ]
    result = ParseResult(events=events, errors=[])
    tl = build_timeline(result)
    types = [e.event_type for e in tl.entries]
    assert types == ["member_join", "rebalance_start", "rebalance_end"]


def test_build_timeline_sets_start_end():
    events = [_make_event("a", _ts(5)), _make_event("b", _ts(15))]
    result = ParseResult(events=events, errors=[])
    tl = build_timeline(result)
    assert tl.start == _ts(5)
    assert tl.end == _ts(15)
    assert tl.span_seconds == pytest.approx(10.0)


def test_build_timeline_no_ts_events_appended_at_end():
    events = [
        _make_event("rebalance_start", _ts(10)),
        _make_event("unknown"),  # no timestamp
    ]
    result = ParseResult(events=events, errors=[])
    tl = build_timeline(result)
    assert tl.entries[-1].event_type == "unknown"
    assert tl.entries[-1].offset_seconds is None


def test_build_timeline_offset_seconds_computed():
    events = [_make_event("a", _ts(0)), _make_event("b", _ts(20))]
    result = ParseResult(events=events, errors=[])
    tl = build_timeline(result)
    assert tl.entries[0].offset_seconds == pytest.approx(0.0)
    assert tl.entries[1].offset_seconds == pytest.approx(20.0)


def test_build_timeline_label_includes_event_type():
    events = [_make_event("member_join", _ts(1), member_id="consumer-abc")]
    result = ParseResult(events=events, errors=[])
    tl = build_timeline(result)
    assert "MEMBER_JOIN" in tl.entries[0].label


# ---------------------------------------------------------------------------
# render_timeline
# ---------------------------------------------------------------------------

def test_render_timeline_returns_string():
    tl = Timeline(entries=[], start=None, end=None)
    output = render_timeline(tl)
    assert isinstance(output, str)


def test_render_timeline_contains_event_type():
    entry = TimelineEntry(
        timestamp=_ts(0),
        event_type="rebalance_start",
        member_id=None,
        label="REBALANCE_START",
        offset_seconds=0.0,
    )
    tl = Timeline(entries=[entry], start=_ts(0), end=_ts(0))
    output = render_timeline(tl)
    assert "REBALANCE_START" in output


def test_render_timeline_shows_span():
    entry1 = TimelineEntry(timestamp=_ts(0), event_type="a", member_id=None, label="A", offset_seconds=0.0)
    entry2 = TimelineEntry(timestamp=_ts(30), event_type="b", member_id=None, label="B", offset_seconds=30.0)
    tl = Timeline(entries=[entry1, entry2], start=_ts(0), end=_ts(30))
    output = render_timeline(tl)
    assert "30.00s" in output
