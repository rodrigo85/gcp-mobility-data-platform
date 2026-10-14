import json

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from mobility_beam.transforms import DLQ_TAG, InvalidEventError, ParseEvent, parse_event


def make_event(**overrides):
    event = {
        "event_id": "e1",
        "ride_id": "r1",
        "event_type": "completed",
        "event_ts": "2026-01-01T12:00:00.000Z",
        "rider_id": "r_000001",
        "driver_id": "d_00001",
        "pickup_zone_id": 132,
        "dropoff_zone_id": 236,
        "fare_amount": 42.5,
        "schema_version": 1,
    }
    event.update(overrides)
    return event


def encode(event) -> bytes:
    return json.dumps(event).encode()


def test_valid_event_is_returned_unchanged():
    assert parse_event(encode(make_event())) == make_event()


def test_optional_fields_default_to_none():
    event = make_event(event_type="requested")
    del event["driver_id"], event["fare_amount"]
    parsed = parse_event(encode(event))
    assert parsed["driver_id"] is None
    assert parsed["fare_amount"] is None


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"{not json", "invalid json"),
        (b"[1, 2]", "not a JSON object"),
        (encode({k: v for k, v in make_event().items() if k != "ride_id"}), "ride_id"),
        (encode(make_event(pickup_zone_id="132")), "invalid type"),
        (encode(make_event(pickup_zone_id=True)), "invalid type"),
        (encode(make_event(pickup_zone_id=999)), "out of range"),
        (encode(make_event(event_type="teleported")), "event_type"),
        (encode(make_event(fare_amount=None)), "without fare_amount"),
        (encode(make_event(schema_version=2)), "schema_version"),
        (encode(make_event(event_ts="yesterday")), "event_ts"),
        (encode({**make_event(), "surge": 1.5}), "unknown fields"),
    ],
)
def test_invalid_events_are_rejected_with_reason(payload, reason):
    with pytest.raises(InvalidEventError, match=reason):
        parse_event(payload)


def test_attribute_event_id_must_match_payload():
    with pytest.raises(InvalidEventError, match="differs"):
        parse_event(encode(make_event()), {"event_id": "other"})


def test_parse_routes_invalid_messages_to_dlq():
    with TestPipeline() as p:
        results = (
            p
            | beam.Create([encode(make_event()), b"garbage"])
            | beam.ParDo(ParseEvent()).with_outputs(DLQ_TAG, main="valid")
        )
        assert_that(
            results.valid | "Ids" >> beam.Map(lambda e: e["event_id"]),
            equal_to(["e1"]),
            label="valid",
        )
        assert_that(
            results[DLQ_TAG] | "Payloads" >> beam.Map(lambda r: r["raw_payload"]),
            equal_to(["garbage"]),
            label="dlq",
        )
