"""Streaming semantics: late data and duplicates, with controlled event time."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from mobility_beam.transforms import DeduplicateEvents, WindowZoneDemand


def streaming_pipeline() -> TestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return TestPipeline(options=options)


def event(event_id, event_type="requested", zone=1, fare=None):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "pickup_zone_id": zone,
        "fare_amount": fare,
    }


def test_late_event_emits_corrected_pane_for_the_same_window():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(event("a"), 10), TimestampedValue(event("b"), 20)])
        # The watermark passes the end of window [0, 60): the on-time pane fires
        .advance_watermark_to(70)
        # An event for the same window arrives late, within allowed_lateness
        .add_elements([TimestampedValue(event("c"), 30)])
        .advance_watermark_to_infinity()
    )

    with streaming_pipeline() as p:
        panes = (
            p
            | stream
            | WindowZoneDemand(window_seconds=60, allowed_lateness_seconds=600)
            | beam.Map(lambda r: (r["window_start"][11:19], r["requested"], r["pane_timing"]))
        )
        assert_that(
            panes,
            equal_to([("00:00:00", 2, "ON_TIME"), ("00:00:00", 3, "LATE")]),
        )


def test_event_beyond_allowed_lateness_is_dropped():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(event("a"), 10)])
        .advance_watermark_to(1000)
        .add_elements([TimestampedValue(event("too-late"), 15)])
        .advance_watermark_to_infinity()
    )

    with streaming_pipeline() as p:
        counts = (
            p
            | stream
            | WindowZoneDemand(window_seconds=60, allowed_lateness_seconds=600)
            | beam.Map(lambda r: r["requested"])
        )
        assert_that(counts, equal_to([1]))


def test_revenue_counts_only_completed_events():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements(
            [
                TimestampedValue(event("a", "completed", fare=10.0), 5),
                TimestampedValue(event("b", "completed", fare=2.5), 6),
                TimestampedValue(event("c", "cancelled"), 7),
            ]
        )
        .advance_watermark_to_infinity()
    )

    with streaming_pipeline() as p:
        rows = (
            p
            | stream
            | WindowZoneDemand(window_seconds=60, allowed_lateness_seconds=0)
            | beam.Map(lambda r: (r["completed"], r["cancelled"], r["revenue"]))
        )
        assert_that(rows, equal_to([(2, 1, 12.5)]))


def test_duplicate_event_ids_are_removed():
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements(
            [
                TimestampedValue(event("a"), 10),
                TimestampedValue(event("a"), 11),  # Pub/Sub redelivery
                TimestampedValue(event("b"), 12),
            ]
        )
        .advance_watermark_to_infinity()
    )

    with streaming_pipeline() as p:
        ids = p | stream | DeduplicateEvents(ttl_seconds=3600) | beam.Map(lambda e: e["event_id"])
        assert_that(ids, equal_to(["a", "b"]))
