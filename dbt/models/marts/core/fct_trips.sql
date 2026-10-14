{#
    TLC trips fact.

    insert_overwrite replaces whole daily partitions: when a month is reloaded
    into bronze, all its rows get a new _ingested_at, enter the incremental
    batch and those days' partitions are rewritten.
    Idempotent and needs no MERGE (cheaper in BigQuery).
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
        cluster_by=['pickup_zone_id', 'dropoff_zone_id', 'payment_type'],
        require_partition_filter=true,
        on_schema_change='fail',
        tags=['batch'],
    )
}}

select
    trip_id,
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pickup_zone_id,
    pickup_borough,
    dropoff_zone_id,
    dropoff_borough,
    vendor_id,
    payment_type,
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    avg_speed_mph,
    fare_amount,
    tip_amount,
    tip_pct,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    is_suspicious,
    _batch_month,
    _ingested_at
from {{ ref('int_trips__enriched') }}
{% if is_incremental() %}
where _ingested_at > (select max(_ingested_at) from {{ this }} where pickup_date is not null)
{% endif %}
