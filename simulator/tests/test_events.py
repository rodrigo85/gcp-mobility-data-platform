import json
import random
from datetime import UTC, datetime

from ride_simulator.events import ChaosConfig, simulate_ride, to_messages

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
NO_CHAOS = ChaosConfig(
    late_fraction=0, duplicate_fraction=0, malformed_fraction=0, cancel_fraction=0
)


def test_completed_ride_has_full_lifecycle_in_order():
    events = simulate_ride(NOW, random.Random(1), NO_CHAOS)

    assert [e.event_type for e in events] == ["requested", "accepted", "started", "completed"]
    assert [e.event_ts for e in events] == sorted(e.event_ts for e in events)
    assert len({e.ride_id for e in events}) == 1
    assert events[-1].fare_amount is not None
    assert all(e.fare_amount is None for e in events[:-1])


def test_messages_carry_event_time_and_id_as_attributes():
    rng = random.Random(2)
    events = simulate_ride(NOW, rng, NO_CHAOS)
    messages = to_messages(events, rng, NO_CHAOS)

    assert len(messages) == len(events)
    for msg, event in zip(messages, events, strict=True):
        assert msg.attributes == {"event_id": event.event_id, "event_ts": event.event_ts}
        assert json.loads(msg.data)["event_id"] == event.event_id


def test_chaos_injects_duplicates_and_malformed_payloads():
    rng = random.Random(3)
    chaos = ChaosConfig(duplicate_fraction=1.0, malformed_fraction=0.0, late_fraction=0)
    events = simulate_ride(NOW, rng, chaos)

    assert len(to_messages(events, rng, chaos)) == 2 * len(events)

    broken = to_messages(events, rng, ChaosConfig(malformed_fraction=1.0))
    assert all("event_id" not in m.attributes and "event_ts" in m.attributes for m in broken)


def test_same_seed_is_reproducible():
    a = simulate_ride(NOW, random.Random(7), NO_CHAOS)
    b = simulate_ride(NOW, random.Random(7), NO_CHAOS)
    assert a == b
