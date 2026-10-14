with trips as (

    select * from {{ ref('stg_tlc__yellow_trips') }}

),

zones as (

    select * from {{ ref('stg_tlc__taxi_zones') }}

),

measured as (

    select
        trips.*,
        datetime_diff(dropoff_datetime, pickup_datetime, second) / 60.0 as trip_duration_minutes
    from trips

)

select
    measured.*,
    pickup.borough as pickup_borough,
    pickup.zone_name as pickup_zone_name,
    dropoff.borough as dropoff_borough,
    dropoff.zone_name as dropoff_zone_name,
    safe_divide(trip_distance_miles, trip_duration_minutes / 60.0) as avg_speed_mph,
    -- Tips are only recorded for card payments; cash trips report zero
    if(
        payment_type = 'credit_card',
        cast(safe_divide(tip_amount, nullif(fare_amount, 0)) as float64),
        null
    ) as tip_pct,
    -- Suspicious rows are flagged, not dropped: analysis decides what to do
    (
        trip_duration_minutes > {{ var('max_trip_duration_minutes') }}
        or trip_duration_minutes = 0 and trip_distance_miles > 0
        or safe_divide(trip_distance_miles, trip_duration_minutes / 60.0) > {{ var('max_avg_speed_mph') }}
        or total_amount < 0
    ) as is_suspicious
from measured
left join zones as pickup on measured.pickup_zone_id = pickup.zone_id
left join zones as dropoff on measured.dropoff_zone_id = dropoff.zone_id
