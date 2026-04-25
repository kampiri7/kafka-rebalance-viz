"""Tests for kafka_rebalance_viz.filter."""

from datetime import datetime, timezone

import pytest

from kafka_rebalance_viz.parser import ParseResult, RebalanceEvent
from kafka_rebalance_viz.filter import FilterCriteria, filter_events


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 15, hour, minute, 0, tzinfo=timezone.utc)


def _make_event(
    event_type: str = "MEMBER_JOIN",
    member_id: str = "consumer-1",
    group_id: str = "my-group",
    timestamp: datetime | None = None,
) -> RebalanceEvent:
    return RebalanceEvent(
        event_type=event_type,
        member_id=member_id,
        group_id=group_id,
        timestamp=timestamp or _ts(10),
        raw_line=f"[{event_type}] {member_id}",
    )


def _result(*events: RebalanceEvent) -> ParseResult:
    return ParseResult(events=list(events), errors=[])


# ---------------------------------------------------------------------------
# event_types filter
# ---------------------------------------------------------------------------

def test_filter_by_event_type_keeps_matching():
    r = _result(_make_event("MEMBER_JOIN"), _make_event("REBALANCE_START"))
    out = filter_events(r, FilterCriteria(event_types=["MEMBER_JOIN"]))
    assert len(out.events) == 1
    assert out.events[0].event_type == "MEMBER_JOIN"


def test_filter_by_event_type_empty_when_none_match():
    r = _result(_make_event("MEMBER_JOIN"))
    out = filter_events(r, FilterCriteria(event_types=["REBALANCE_START"]))
    assert out.events == []


# ---------------------------------------------------------------------------
# member_id filter
# ---------------------------------------------------------------------------

def test_filter_by_member_id_substring_match():
    r = _result(
        _make_event(member_id="consumer-1-abc"),
        _make_event(member_id="consumer-2-xyz"),
    )
    out = filter_events(r, FilterCriteria(member_id="consumer-1"))
    assert len(out.events) == 1
    assert "consumer-1" in out.events[0].member_id


def test_filter_by_member_id_none_member_excluded():
    event = _make_event()
    event.member_id = None
    r = _result(event)
    out = filter_events(r, FilterCriteria(member_id="consumer-1"))
    assert out.events == []


# ---------------------------------------------------------------------------
# since / until filters
# ---------------------------------------------------------------------------

def test_filter_since_excludes_earlier_events():
    r = _result(_make_event(timestamp=_ts(8)), _make_event(timestamp=_ts(12)))
    out = filter_events(r, FilterCriteria(since=_ts(10)))
    assert len(out.events) == 1
    assert out.events[0].timestamp == _ts(12)


def test_filter_until_excludes_later_events():
    r = _result(_make_event(timestamp=_ts(8)), _make_event(timestamp=_ts(12)))
    out = filter_events(r, FilterCriteria(until=_ts(10)))
    assert len(out.events) == 1
    assert out.events[0].timestamp == _ts(8)


# ---------------------------------------------------------------------------
# group_id filter
# ---------------------------------------------------------------------------

def test_filter_by_group_id():
    r = _result(
        _make_event(group_id="group-a"),
        _make_event(group_id="group-b"),
    )
    out = filter_events(r, FilterCriteria(group_id="group-a"))
    assert len(out.events) == 1
    assert out.events[0].group_id == "group-a"


# ---------------------------------------------------------------------------
# errors are preserved
# ---------------------------------------------------------------------------

def test_filter_preserves_errors():
    r = ParseResult(events=[_make_event()], errors=["bad line"])
    out = filter_events(r, FilterCriteria(event_types=["REBALANCE_START"]))
    assert out.errors == ["bad line"]


# ---------------------------------------------------------------------------
# no criteria — all events pass
# ---------------------------------------------------------------------------

def test_filter_no_criteria_returns_all():
    r = _result(_make_event(), _make_event(event_type="REBALANCE_START"))
    out = filter_events(r, FilterCriteria())
    assert len(out.events) == 2
