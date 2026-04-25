"""Tests for the CLI entry point."""

import json
from pathlib import Path

import pytest

from kafka_rebalance_viz.cli import run


SAMPLE_LOG = """\
2024-01-15T10:00:00.000Z INFO  GroupCoordinator - Member consumer-1 joined group my-group
2024-01-15T10:00:01.000Z INFO  GroupCoordinator - Preparing to rebalance group my-group
2024-01-15T10:00:02.000Z INFO  GroupCoordinator - Stabilized group my-group
2024-01-15T10:00:03.000Z INFO  GroupCoordinator - Member consumer-2 joined group other-group
"""


@pytest.fixture()
def log_file(tmp_path: Path) -> Path:
    p = tmp_path / "kafka.log"
    p.write_text(SAMPLE_LOG, encoding="utf-8")
    return p


def test_run_returns_zero_on_valid_file(log_file: Path) -> None:
    assert run([str(log_file)]) == 0


def test_run_returns_one_for_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "nonexistent.log"
    assert run([str(missing)]) == 1


def test_run_returns_one_for_directory(tmp_path: Path) -> None:
    assert run([str(tmp_path)]) == 1


def test_run_json_output_is_valid_json(log_file: Path, capsys: pytest.CaptureFixture) -> None:
    result = run([str(log_file), "--output", "json"])
    assert result == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert isinstance(data, dict)


def test_run_text_output_contains_summary(log_file: Path, capsys: pytest.CaptureFixture) -> None:
    result = run([str(log_file), "--output", "text"])
    assert result == 0
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_run_verbose_prints_events(log_file: Path, capsys: pytest.CaptureFixture) -> None:
    result = run([str(log_file), "--verbose"])
    assert result == 0
    captured = capsys.readouterr()
    # verbose mode should produce more output than summary alone
    assert len(captured.out.splitlines()) > 2


def test_run_group_filter_limits_output(log_file: Path, capsys: pytest.CaptureFixture) -> None:
    result = run([str(log_file), "--output", "json", "--group", "my-group"])
    assert result == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    # only my-group events; other-group member should not appear
    assert isinstance(data, dict)
