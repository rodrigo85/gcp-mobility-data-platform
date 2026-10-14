"""Ride event contract and destination table names.

The BigQuery tables are created by Terraform (infra/terraform/bigquery.tf),
not by the pipeline: the schema is a versioned contract, and the pipeline writes
with CREATE_NEVER so it fails loudly if the contract drifts.
"""

EVENT_TYPES = frozenset({"requested", "accepted", "started", "completed", "cancelled"})
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})

# Field -> (accepted types, required)
EVENT_FIELDS: dict[str, tuple[tuple[type, ...], bool]] = {
    "event_id": ((str,), True),
    "ride_id": ((str,), True),
    "event_type": ((str,), True),
    "event_ts": ((str,), True),
    "rider_id": ((str,), True),
    "driver_id": ((str, type(None)), False),
    "pickup_zone_id": ((int,), True),
    "dropoff_zone_id": ((int,), True),
    "fare_amount": ((int, float, type(None)), False),
    "schema_version": ((int,), True),
}

MIN_ZONE_ID, MAX_ZONE_ID = 1, 265

TABLE_EVENTS = "ride_events"
TABLE_ZONE_DEMAND = "zone_demand_1m"
TABLE_DLQ = "ride_events_dlq"
