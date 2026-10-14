-- Beam deduplicates within its state TTL, but a retried load job can rewrite
-- a batch. This layer guarantees uniqueness per event_id.
with source as (

    select * from {{ source('rides', 'ride_events') }}
    {{ limit_dev_data('event_ts', 'timestamp') }}

)

select
    event_id,
    ride_id,
    event_type,
    event_ts,
    rider_id,
    driver_id,
    pickup_zone_id,
    dropoff_zone_id,
    cast(fare_amount as numeric) as fare_amount,
    schema_version,
    ingest_ts
from source
qualify row_number() over (partition by event_id order by ingest_ts) = 1
