"""Helpers that wire CLI filter flags into :class:`FilterCriteria`."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from kafka_rebalance_viz.filter import FilterCriteria


def add_filter_args(parser: argparse.ArgumentParser) -> None:
    """Attach filter-related arguments to *parser* in-place."""
    grp = parser.add_argument_group("filtering")
    grp.add_argument(
        "--event-types",
        metavar="TYPE",
        nargs="+",
        help="Only include events of these types (e.g. MEMBER_JOIN REBALANCE_START).",
    )
    grp.add_argument(
        "--member-id",
        metavar="MEMBER",
        help="Only include events whose member-id contains this substring.",
    )
    grp.add_argument(
        "--group-id",
        metavar="GROUP",
        help="Only include events whose group-id contains this substring.",
    )
    grp.add_argument(
        "--since",
        metavar="DATETIME",
        help="Exclude events before this ISO-8601 timestamp (UTC assumed if no tz).",
    )
    grp.add_argument(
        "--until",
        metavar="DATETIME",
        help="Exclude events after this ISO-8601 timestamp (UTC assumed if no tz).",
    )


def _parse_dt(value: str) -> datetime:
    """Parse an ISO-8601 string; attach UTC if no timezone is present."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def criteria_from_args(args: argparse.Namespace) -> Optional[FilterCriteria]:
    """Build a :class:`FilterCriteria` from parsed CLI args.

    Returns *None* when no filter flags were supplied so callers can skip
    the filtering step entirely.
    """
    since = _parse_dt(args.since) if getattr(args, "since", None) else None
    until = _parse_dt(args.until) if getattr(args, "until", None) else None
    event_types = getattr(args, "event_types", None)
    member_id = getattr(args, "member_id", None)
    group_id = getattr(args, "group_id", None)

    if not any([since, until, event_types, member_id, group_id]):
        return None

    return FilterCriteria(
        event_types=event_types,
        member_id=member_id,
        group_id=group_id,
        since=since,
        until=until,
    )
