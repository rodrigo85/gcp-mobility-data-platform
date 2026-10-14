"""Synthetic ride lifecycle event generation.

The simulator deliberately produces the problems the streaming pipeline must
handle: late, duplicated and malformed events. Each problem is controlled by a
configurable fraction, so experiments are reproducible with a fixed seed.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta

SCHEMA_VERSION = 1
# Valid TLC zone IDs (264 and 265 mean unknown/outside NYC)
VALID_ZONE_IDS = range(1, 264)

EVENT_SEQUENCE_COMPLETED = ("requested", "accepted", "started", "completed")
EVENT_SEQUENCE_CANCELLED = ("requested", "cancelled")


@dataclass(frozen=True)
class RideEvent:
    event_id: str
    ride_id: str
    event_type: str
    event_ts: str  # RFC 3339 in UTC, with milliseconds
    rider_id: str
    driver_id: str | None
    pickup_zone_id: int
    dropoff_zone_id: int
    fare_amount: float | None
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")


@dataclass
class ChaosConfig:
    """Fractions of problematic events (0.0 to 1.0)."""

    late_fraction: float = 0.02
    late_max_delay: timedelta = timedelta(minutes=8)
    duplicate_fraction: float = 0.01
    malformed_fraction: float = 0.005
    cancel_fraction: float = 0.1


@dataclass
class Publishable:
    """A message ready for Pub/Sub: payload and attributes."""

    data: bytes
    attributes: dict[str, str] = field(default_factory=dict)


def format_ts(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def simulate_ride(now: datetime, rng: random.Random, chaos: ChaosConfig) -> list[RideEvent]:
    """Generate every event of one ride, with increasing event timestamps."""
    ride_id = str(uuid.UUID(int=rng.getrandbits(128)))
    rider_id = f"r_{rng.randrange(1_000_000):06d}"
    driver_id = f"d_{rng.randrange(50_000):05d}"
    pickup, dropoff = rng.choice(VALID_ZONE_IDS), rng.choice(VALID_ZONE_IDS)
    cancelled = rng.random() < chaos.cancel_fraction
    sequence = EVENT_SEQUENCE_CANCELLED if cancelled else EVENT_SEQUENCE_COMPLETED

    events: list[RideEvent] = []
    ts = now
    for event_type in sequence:
        events.append(
            RideEvent(
                event_id=str(uuid.UUID(int=rng.getrandbits(128))),
                ride_id=ride_id,
                event_type=event_type,
                event_ts=format_ts(ts),
                rider_id=rider_id,
                driver_id=None if event_type in ("requested", "cancelled") else driver_id,
                pickup_zone_id=pickup,
                dropoff_zone_id=dropoff,
                fare_amount=round(rng.uniform(5, 80), 2) if event_type == "completed" else None,
            )
        )
        ts += timedelta(seconds=rng.randint(20, 900))
    return events


def to_messages(
    events: list[RideEvent], rng: random.Random, chaos: ChaosConfig
) -> list[Publishable]:
    """Turn events into messages and inject delays, duplicates and garbage."""
    messages: list[Publishable] = []
    for event in events:
        if rng.random() < chaos.malformed_fraction:
            # Broken payload, but with event_ts: Beam reads event time from the attribute
            # and needs it to route the message to the DLQ instead of failing on read.
            messages.append(
                Publishable(
                    data=b'{"event_id": "broken", "fare_amount": "abc"',
                    attributes={"event_ts": event.event_ts},
                )
            )
            continue

        msg = Publishable(
            data=event.to_json(),
            attributes={"event_id": event.event_id, "event_ts": event.event_ts},
        )
        if rng.random() < chaos.late_fraction:
            # The event keeps its original timestamp but is published later:
            # the publisher holds the message for `delay` before sending it.
            delay = rng.uniform(0, chaos.late_max_delay.total_seconds())
            msg.attributes["publish_delay_s"] = f"{delay:.1f}"
        messages.append(msg)
        if rng.random() < chaos.duplicate_fraction:
            messages.append(Publishable(data=msg.data, attributes=dict(msg.attributes)))
    return messages


def utcnow() -> datetime:
    return datetime.now(UTC)
