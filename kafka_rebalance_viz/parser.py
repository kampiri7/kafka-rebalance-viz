"""Parser module for extracting Kafka consumer group rebalancing events from log lines."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# Regex patterns for common Kafka consumer log formats
REBALANCE_START_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]?\d*).*"
    r"(?:Revoking previously assigned partitions|rebalance started|Starting rebalance)"
    r".*group=(?P<group>[\w.-]+)?",
    re.IGNORECASE,
)

REBALANCE_COMPLETE_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]?\d*).*"
    r"(?:Setting newly assigned partitions|rebalance complete|Rebalance complete)"
    r".*group=(?P<group>[\w.-]+)?",
    re.IGNORECASE,
)

MEMBER_JOIN_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]?\d*).*"
    r"Member (?P<member_id>[\w.-]+) joining group (?P<group>[\w.-]+)",
    re.IGNORECASE,
)


@dataclass
class RebalanceEvent:
    event_type: str  # 'start', 'complete', 'member_join'
    timestamp: datetime
    group: Optional[str] = None
    member_id: Optional[str] = None
    raw_line: str = ""


@dataclass
class ParseResult:
    events: list = field(default_factory=list)
    unmatched_lines: int = 0
    total_lines: int = 0


TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S,%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S,%f",
    "%Y-%m-%d %H:%M:%S",
]


def parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Attempt to parse a timestamp string using known formats."""
    ts_str = ts_str.strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return None


def parse_line(line: str) -> Optional[RebalanceEvent]:
    """Parse a single log line and return a RebalanceEvent if matched."""
    for pattern, event_type in [
        (REBALANCE_START_PATTERN, "start"),
        (REBALANCE_COMPLETE_PATTERN, "complete"),
        (MEMBER_JOIN_PATTERN, "member_join"),
    ]:
        match = pattern.search(line)
        if match:
            ts = parse_timestamp(match.group("timestamp"))
            if ts is None:
                continue
            group = match.groupdict().get("group")
            member_id = match.groupdict().get("member_id")
            return RebalanceEvent(
                event_type=event_type,
                timestamp=ts,
                group=group,
                member_id=member_id,
                raw_line=line.rstrip(),
            )
    return None


def parse_log(lines) -> ParseResult:
    """Parse an iterable of log lines and return a ParseResult."""
    result = ParseResult()
    for line in lines:
        result.total_lines += 1
        event = parse_line(line)
        if event:
            result.events.append(event)
        else:
            result.unmatched_lines += 1
    return result
