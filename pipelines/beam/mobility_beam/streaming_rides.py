"""Streaming pipeline: Pub/Sub → validation → dedup → BigQuery bronze.

Three outputs:
  - bronze.ride_events      valid, deduplicated events
  - bronze.zone_demand_1m   demand per zone in 1-minute windows (with late panes)
  - bronze.ride_events_dlq  invalid events, with the reason

Writes use FILE_LOADS (periodic load jobs), which are free in BigQuery,
instead of streaming inserts, which are billed. The cost is latency: data
lands in BigQuery every `bq_triggering_frequency` seconds.

    python -m mobility_beam.streaming_rides --runner DirectRunner --streaming \\
        --input_subscription projects/P/subscriptions/S --sink log
"""

from __future__ import annotations

import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from mobility_beam.schemas import TABLE_DLQ, TABLE_EVENTS, TABLE_ZONE_DEMAND
from mobility_beam.transforms import (
    DLQ_TAG,
    DeduplicateEvents,
    ParseEvent,
    WindowZoneDemand,
    add_ingest_ts,
)


class RideOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_subscription", required=True)
        parser.add_argument("--sink", choices=["log", "bigquery"], default="log")
        parser.add_argument("--bq_project", default=None)
        parser.add_argument("--bq_dataset", default="bronze")
        parser.add_argument("--bq_triggering_frequency", type=int, default=300)
        parser.add_argument("--window_seconds", type=int, default=60)
        parser.add_argument("--allowed_lateness_seconds", type=int, default=600)
        parser.add_argument("--dedup_ttl_seconds", type=int, default=3600)


def _log_row(row: dict, table: str) -> None:
    logging.info("%s %s", table, row)


def build(pipeline: beam.Pipeline, opts: RideOptions) -> None:
    parsed = (
        pipeline
        | "ReadPubSub"
        >> beam.io.ReadFromPubSub(
            subscription=opts.input_subscription,
            with_attributes=True,
            timestamp_attribute="event_ts",  # event time, not publish time
        )
        | "Parse" >> beam.ParDo(ParseEvent()).with_outputs(DLQ_TAG, main="valid")
    )

    events = parsed.valid | "Dedup" >> DeduplicateEvents(opts.dedup_ttl_seconds)
    demand = events | "ZoneDemand" >> WindowZoneDemand(
        opts.window_seconds, opts.allowed_lateness_seconds
    )

    outputs = {
        TABLE_EVENTS: events | "IngestTsEvents" >> beam.Map(add_ingest_ts),
        TABLE_ZONE_DEMAND: demand | "IngestTsDemand" >> beam.Map(add_ingest_ts),
        TABLE_DLQ: parsed[DLQ_TAG],
    }
    for table, pcoll in outputs.items():
        if opts.sink == "log":
            _ = pcoll | f"Log_{table}" >> beam.Map(_log_row, table)
        else:
            _ = pcoll | f"Write_{table}" >> WriteToBigQuery(
                table=f"{opts.bq_project}:{opts.bq_dataset}.{table}",
                method=WriteToBigQuery.Method.FILE_LOADS,
                triggering_frequency=opts.bq_triggering_frequency,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )


def run(argv: list[str] | None = None) -> None:
    opts = PipelineOptions(argv)
    opts.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=opts) as pipeline:
        build(pipeline, opts.view_as(RideOptions))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
