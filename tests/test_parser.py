"""Tests for kafka_rebalance_viz.parser module."""

import pytest
from datetime import datetime
from kafka_rebalance_viz.parser import parse_line, parse_log, parse_timestamp, RebalanceEvent


SAMPLE_LINES = [
    (
        "2024-03-15 10:22:01,123 INFO  Revoking previously assigned partitions group=my-group",
        "start",
        "my-group",
    ),
    (
        "2024-03-15T10:22:05.456 INFO  Setting newly assigned partitions group=my-group",
        "complete",
        "my-group",
    ),
    (
        "2024-03-15 10:22:00,000 INFO  Member consumer-1-uuid joining group analytics-group",
        "member_join",
        "analytics-group",
    ),
]


@pytest.mark.parametrize("line, expected_type, expected_group", SAMPLE_LINES)
def test_parse_line_event_type(line, expected_type, expected_group):
    event = parse_line(line)
    assert event is not None, f"Expected event from line: {line}"
    assert event.event_type == expected_type
    assert event.group == expected_group


def test_parse_line_returns_none_for_unrelated_log():
    line = "2024-03-15 10:22:01,123 INFO  Heartbeat sent to coordinator"
    assert parse_line(line) is None


def test_parse_line_preserves_raw_line():
    line = "2024-03-15 10:22:01,123 INFO  Revoking previously assigned partitions group=g1"
    event = parse_line(line)
    assert event is not None
    assert event.raw_line == line


def test_parse_line_member_join_captures_member_id():
    line = "2024-03-15 10:22:00,000 INFO  Member consumer-42-abc joining group test-group"
    event = parse_line(line)
    assert event is not None
    assert event.member_id == "consumer-42-abc"


def test_parse_timestamp_iso_with_microseconds():
    ts = parse_timestamp("2024-03-15T10:22:05.456")
    assert isinstance(ts, datetime)
    assert ts.year == 2024
    assert ts.minute == 22


def test_parse_timestamp_space_separated_with_comma():
    ts = parse_timestamp("2024-03-15 10:22:01,123")
    assert isinstance(ts, datetime)
    assert ts.second == 1


def test_parse_timestamp_invalid_returns_none():
    ts = parse_timestamp("not-a-timestamp")
    assert ts is None


def test_parse_log_counts_lines():
    lines = [
        "2024-03-15 10:22:01,123 INFO  Revoking previously assigned partitions group=g1",
        "2024-03-15 10:22:02,000 INFO  Some unrelated log message",
        "2024-03-15 10:22:05,456 INFO  Setting newly assigned partitions group=g1",
    ]
    result = parse_log(lines)
    assert result.total_lines == 3
    assert len(result.events) == 2
    assert result.unmatched_lines == 1


def test_parse_log_event_order():
    lines = [
        "2024-03-15 10:22:01,000 INFO  Revoking previously assigned partitions group=g1",
        "2024-03-15 10:22:05,000 INFO  Setting newly assigned partitions group=g1",
    ]
    result = parse_log(lines)
    assert result.events[0].event_type == "start"
    assert result.events[1].event_type == "complete"


def test_parse_log_empty_input():
    result = parse_log([])
    assert result.total_lines == 0
    assert result.events == []
    assert result.unmatched_lines == 0
