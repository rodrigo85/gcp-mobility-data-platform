"""Streaming pipeline transforms, kept apart from I/O so they can be tested."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, NamedTuple

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import trigger, window
from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.windowed_value import PaneInfoTiming

from mobility_beam.schemas import (
    EVENT_FIELDS,
    EVENT_TYPES,
    MAX_ZONE_ID,
    MIN_ZONE_ID,
    SUPPORTED_SCHEMA_VERSIONS,
)

DLQ_TAG = "dlq"


class InvalidEventError(ValueError):
    """The payload does not honor the event contract."""


def parse_event(data: bytes, attributes: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Decode and validate an event. Raise InvalidEventError if it is invalid."""
    try:
        event = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidEventError(f"invalid json: {exc}") from exc
    if not isinstance(event, dict):
        raise InvalidEventError("payload is not a JSON object")

    for name, (types, required) in EVENT_FIELDS.items():
        if name not in event:
            if required:
                raise InvalidEventError(f"missing required field: {name}")
            event[name] = None
            continue
        value = event[name]
        # bool is a subclass of int in Python; we must not accept true as a zone
        if isinstance(value, bool) or not isinstance(value, types):
            raise InvalidEventError(f"invalid type for {name}: {type(value).__name__}")

    unknown = set(event) - set(EVENT_FIELDS)
    if unknown:
        raise InvalidEventError(f"unknown fields: {sorted(unknown)}")
    if event["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS:
        raise InvalidEventError(f"unsupported schema_version: {event['schema_version']}")
    if event["event_type"] not in EVENT_TYPES:
        raise InvalidEventError(f"unknown event_type: {event['event_type']}")
    for zone in ("pickup_zone_id", "dropoff_zone_id"):
        if not MIN_ZONE_ID <= event[zone] <= MAX_ZONE_ID:
            raise InvalidEventError(f"{zone} out of range: {event[zone]}")
    if event["event_type"] == "completed" and event["fare_amount"] is None:
        raise InvalidEventError("completed event without fare_amount")
    if attributes and attributes.get("event_id") not in (None, event["event_id"]):
        raise InvalidEventError("event_id attribute differs from the payload")

    try:
        datetime.fromisoformat(event["event_ts"])
    except ValueError as exc:
        raise InvalidEventError(f"invalid event_ts: {event['event_ts']}") from exc
    return event


class ParseEvent(beam.DoFn):
    """Split valid from invalid events; invalid ones go to the DLQ with the reason."""

    def __init__(self) -> None:
        self.valid = Metrics.counter("ride_events", "valid")
        self.invalid = Metrics.counter("ride_events", "invalid")

    def process(self, message):
        data = getattr(message, "data", message)
        attributes = getattr(message, "attributes", None) or {}
        try:
            event = parse_event(data, attributes)
        except InvalidEventError as exc:
            self.invalid.inc()
            yield beam.pvalue.TaggedOutput(
                DLQ_TAG,
                {
                    "received_ts": datetime.now(UTC).isoformat(),
                    "error": str(exc),
                    "raw_payload": data.decode("utf-8", errors="replace"),
                    "attributes": json.dumps(dict(attributes)),
                },
            )
            return
        self.valid.inc()
        yield event


class DeduplicateEvents(beam.PTransform):
    """Drop Pub/Sub redeliveries and producer duplicates by event_id.

    Pub/Sub delivers at least once; without this, revenue would be double counted.
    The state for each event_id lives for `ttl_seconds` of event time.
    """

    def __init__(self, ttl_seconds: int) -> None:
        super().__init__()
        self.ttl_seconds = ttl_seconds

    def expand(self, events):
        return (
            events
            | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
            | "Dedup" >> DeduplicatePerKey(event_time_duration=Duration(seconds=self.ttl_seconds))
            | "DropKey" >> beam.Values()
        )


class ZoneDemand(NamedTuple):
    requested: int = 0
    completed: int = 0
    cancelled: int = 0
    revenue: float = 0.0


class ZoneDemandFn(beam.CombineFn):
    """Aggregate demand and revenue per pickup zone."""

    def create_accumulator(self) -> ZoneDemand:
        return ZoneDemand()

    def add_input(self, acc: ZoneDemand, event: dict[str, Any]) -> ZoneDemand:
        kind = event["event_type"]
        return ZoneDemand(
            requested=acc.requested + (kind == "requested"),
            completed=acc.completed + (kind == "completed"),
            cancelled=acc.cancelled + (kind == "cancelled"),
            revenue=acc.revenue + (event["fare_amount"] or 0.0 if kind == "completed" else 0.0),
        )

    def merge_accumulators(self, accumulators) -> ZoneDemand:
        accs = list(accumulators)
        return ZoneDemand(
            requested=sum(a.requested for a in accs),
            completed=sum(a.completed for a in accs),
            cancelled=sum(a.cancelled for a in accs),
            revenue=sum(a.revenue for a in accs),
        )

    def extract_output(self, acc: ZoneDemand) -> ZoneDemand:
        return acc


class FormatZoneDemand(beam.DoFn):
    """Flatten the window result and record whether the pane is on time or late.

    With ACCUMULATING, each late pane carries the corrected window total;
    dbt keeps only the last pane per (zone, window).
    """

    def process(self, element, win=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        zone_id, demand = element
        yield {
            "pickup_zone_id": zone_id,
            "window_start": win.start.to_utc_datetime().isoformat(),
            "window_end": win.end.to_utc_datetime().isoformat(),
            "requested": demand.requested,
            "completed": demand.completed,
            "cancelled": demand.cancelled,
            "revenue": round(demand.revenue, 2),
            "pane_index": pane.index,
            "pane_timing": PaneInfoTiming.to_string(pane.timing),
        }


class WindowZoneDemand(beam.PTransform):
    """Fixed event-time windows, fired at the watermark and corrected by late data."""

    def __init__(self, window_seconds: int, allowed_lateness_seconds: int) -> None:
        super().__init__()
        self.window_seconds = window_seconds
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def expand(self, events):
        return (
            events
            | "Window"
            >> beam.WindowInto(
                window.FixedWindows(self.window_seconds),
                trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=self.allowed_lateness_seconds),
            )
            | "KeyByZone" >> beam.Map(lambda e: (e["pickup_zone_id"], e))
            | "Aggregate" >> beam.CombinePerKey(ZoneDemandFn())
            | "Format" >> beam.ParDo(FormatZoneDemand())
        )


def add_ingest_ts(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "ingest_ts": datetime.now(UTC).isoformat()}
