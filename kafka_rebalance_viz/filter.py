"""Filtering utilities for rebalance events and parse results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from kafka_rebalance_viz.parser import ParseResult, RebalanceEvent


@dataclass
class FilterCriteria:
    """Criteria used to filter rebalance events."""

    event_types: Optional[List[str]] = None
    member_id: Optional[str] = None
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    group_id: Optional[str] = None


def _matches(event: RebalanceEvent, criteria: FilterCriteria) -> bool:
    """Return True if *event* satisfies all criteria."""
    if criteria.event_types is not None:
        if event.event_type not in criteria.event_types:
            return False

    if criteria.member_id is not None:
        if event.member_id is None or criteria.member_id not in event.member_id:
            return False

    if criteria.group_id is not None:
        if event.group_id is None or criteria.group_id not in event.group_id:
            return False

    if criteria.since is not None and event.timestamp is not None:
        if event.timestamp < criteria.since:
            return False

    if criteria.until is not None and event.timestamp is not None:
        if event.timestamp > criteria.until:
            return False

    return True


def filter_events(
    result: ParseResult, criteria: FilterCriteria
) -> ParseResult:
    """Return a new :class:`ParseResult` containing only matching events."""
    filtered = [e for e in result.events if _matches(e, criteria)]
    return ParseResult(events=filtered, errors=result.errors)
