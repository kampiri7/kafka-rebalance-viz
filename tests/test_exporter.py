"""Tests for kafka_rebalance_viz.exporter."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timedelta, timezone

import pytest

from kafka_rebalance_viz.analyzer import RebalanceSummary
from kafka_rebalance_viz.exporter import export, export_csv, export_json, summary_to_dict


def _make_summary(
    total_events: int = 4,
    event_type_counts: dict | None = None,
    unique_members: set | None = None,
    duration_seconds: float | None = 12.5,
) -> RebalanceSummary:
    ts_first = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    ts_last = (
        ts_first + timedelta(seconds=duration_seconds)
        if duration_seconds is not None
        else None
    )
    return RebalanceSummary(
        total_events=total_events,
        event_type_counts=event_type_counts or {"JOIN": 2, "SYNC": 2},
        unique_members=unique_members or {"member-1", "member-2"},
        duration=timedelta(seconds=duration_seconds) if duration_seconds is not None else None,
        first_event_ts=ts_first,
        last_event_ts=ts_last,
    )


def test_summary_to_dict_scalar_fields():
    summary = _make_summary()
    d = summary_to_dict(summary)
    assert d["total_events"] == 4
    assert d["duration_seconds"] == pytest.approx(12.5)
    assert d["first_event_ts"] == "2024-01-15T10:00:00+00:00"


def test_summary_to_dict_none_duration():
    summary = _make_summary(duration_seconds=None)
    d = summary_to_dict(summary)
    assert d["duration_seconds"] is None
    assert d["last_event_ts"] is None


def test_summary_to_dict_members_sorted():
    summary = _make_summary(unique_members={"zebra", "alpha", "mango"})
    d = summary_to_dict(summary)
    assert d["unique_members"] == ["alpha", "mango", "zebra"]


def test_export_json_is_valid_json():
    summary = _make_summary()
    result = export_json(summary)
    parsed = json.loads(result)
    assert parsed["total_events"] == 4
    assert "JOIN" in parsed["event_type_counts"]


def test_export_json_contains_members():
    summary = _make_summary(unique_members={"m-1"})
    result = json.loads(export_json(summary))
    assert result["unique_members"] == ["m-1"]


def test_export_csv_has_header():
    summary = _make_summary()
    result = export_csv(summary)
    reader = csv.reader(io.StringIO(result))
    header = next(reader)
    assert header == ["key", "value"]


def test_export_csv_event_type_rows():
    summary = _make_summary(event_type_counts={"JOIN": 3})
    result = export_csv(summary)
    rows = {row[0]: row[1] for row in csv.reader(io.StringIO(result)) if len(row) == 2}
    assert rows["event_type:JOIN"] == "3"


def test_export_csv_member_rows():
    summary = _make_summary(unique_members={"member-X"})
    result = export_csv(summary)
    rows = [row for row in csv.reader(io.StringIO(result)) if row[0] == "member"]
    assert any(r[1] == "member-X" for r in rows)


def test_export_dispatch_json():
    summary = _make_summary()
    result = export(summary, "json")
    assert json.loads(result)["total_events"] == 4


def test_export_dispatch_csv():
    summary = _make_summary()
    result = export(summary, "csv")
    assert "key,value" in result


def test_export_unsupported_format_raises():
    summary = _make_summary()
    with pytest.raises(ValueError, match="Unsupported export format"):
        export(summary, "xml")
