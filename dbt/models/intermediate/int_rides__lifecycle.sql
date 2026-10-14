{#
    One row per ride, built from the streaming events.

    Incremental merge: each run fully recomputes the rides that received new
    events (including late ones) and updates their row.
    incremental_predicates limits the target scan to recent partitions;
    without it, the MERGE would read the whole table on every run.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='ride_id',
        partition_by={'field': 'ride_date', 'data_type': 'date'},
        cluster_by=['pickup_zone_id'],
        incremental_predicates=[
            "DBT_INTERNAL_DEST.ride_date >= date_sub(current_date(), interval 7 day)"
        ],
        on_schema_change='append_new_columns',
        tags=['streaming'],
    )
}}

with touched_rides as (

    select distinct ride_id
    from {{ ref('stg_rides__events') }}
    {% if is_incremental() %}
    where ingest_ts > (
        select timestamp_sub(max(_last_ingest_ts), interval {{ var('rides_lookback_hours') }} hour)
        from {{ this }}
    )
    {% endif %}

),

events as (

    select events.*
    from {{ ref('stg_rides__events') }} as events
    inner join touched_rides using (ride_id)

),

pivoted as (

    select
        ride_id,
        any_value(rider_id) as rider_id,
        max(driver_id) as driver_id,
        any_value(pickup_zone_id) as pickup_zone_id,
        any_value(dropoff_zone_id) as dropoff_zone_id,
        min(event_ts) as first_event_at,
        min(if(event_type = 'requested', event_ts, null)) as requested_at,
        min(if(event_type = 'accepted', event_ts, null)) as accepted_at,
        min(if(event_type = 'started', event_ts, null)) as started_at,
        min(if(event_type = 'completed', event_ts, null)) as completed_at,
        min(if(event_type = 'cancelled', event_ts, null)) as cancelled_at,
        max(if(event_type = 'completed', fare_amount, null)) as fare_amount,
        count(*) as event_count,
        max(ingest_ts) as _last_ingest_ts
    from events
    group by ride_id

)

select
    ride_id,
    date(first_event_at) as ride_date,
    rider_id,
    driver_id,
    pickup_zone_id,
    dropoff_zone_id,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    case
        when cancelled_at is not null then 'cancelled'
        when completed_at is not null then 'completed'
        when started_at is not null then 'in_progress'
        when accepted_at is not null then 'accepted'
        else 'requested'
    end as ride_status,
    timestamp_diff(accepted_at, requested_at, second) as seconds_to_accept,
    timestamp_diff(completed_at, started_at, second) as ride_duration_seconds,
    fare_amount,
    -- A later-phase event without the earlier one signals loss or delay
    (completed_at is not null and started_at is null)
    or (started_at is not null and accepted_at is null) as has_missing_events,
    event_count,
    _last_ingest_ts
from pivoted
