"""Tests for the analyzer module."""

from datetime import datetime
from kafka_rebalance_viz.parser import ParseResult, RebalanceEvent
from kafka_rebalance_viz.analyzer import analyze
from kafka_rebalance_viz.formatter import format_summary


def _make_event(event_type: str, member_id: str = None, timestamp: datetime = None) -> RebalanceEvent:
    return RebalanceEvent(
        event_type=event_type,
        member_id=member_id,
        timestamp=timestamp,
        raw_line=f"raw {event_type}",
    )


def test_analyze_empty_result():
    result = ParseResult(events=[], skipped_lines=0)
    summary = analyze(result)
    assert summary.total_events == 0
    assert summary.rebalance_starts == 0
    assert summary.rebalance_completions == 0
    assert summary.avg_rebalance_duration_ms is None


def test_analyze_counts_event_types():
    events = [
        _make_event("REBALANCE_START"),
        _make_event("REBALANCE_COMPLETE"),
        _make_event("MEMBER_JOIN", member_id="consumer-1"),
        _make_event("MEMBER_LEAVE", member_id="consumer-2"),
    ]
    result = ParseResult(events=events, skipped_lines=2)
    summary = analyze(result)
    assert summary.total_events == 4
    assert summary.rebalance_starts == 1
    assert summary.rebalance_completions == 1
    assert summary.member_joins == 1
    assert summary.member_leaves == 1


def test_analyze_unique_members_deduped():
    events = [
        _make_event("MEMBER_JOIN", member_id="consumer-1"),
        _make_event("MEMBER_JOIN", member_id="consumer-1"),
        _make_event("MEMBER_LEAVE", member_id="consumer-2"),
    ]
    result = ParseResult(events=events, skipped_lines=0)
    summary = analyze(result)
    assert summary.unique_members == ["consumer-1", "consumer-2"]


def test_analyze_computes_duration():
    t_start = datetime(2024, 1, 1, 12, 0, 0)
    t_end = datetime(2024, 1, 1, 12, 0, 1)  # 1 second later => 1000 ms
    events = [
        _make_event("REBALANCE_START", member_id="consumer-1", timestamp=t_start),
        _make_event("REBALANCE_COMPLETE", member_id="consumer-1", timestamp=t_end),
    ]
    result = ParseResult(events=events, skipped_lines=0)
    summary = analyze(result)
    assert summary.avg_rebalance_duration_ms == 1000.0
    assert summary.min_rebalance_duration_ms == 1000.0
    assert summary.max_rebalance_duration_ms == 1000.0


def test_format_summary_contains_key_fields():
    events = [
        _make_event("MEMBER_JOIN", member_id="consumer-1"),
    ]
    result = ParseResult(events=events, skipped_lines=0)
    summary = analyze(result)
    output = format_summary(summary)
    assert "Kafka Rebalance Summary" in output
    assert "consumer-1" in output
    assert "MEMBER_JOIN" in output
    assert "n/a" in output
