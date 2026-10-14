{{ config(tags=['batch', 'streaming']) }}

with tlc as (

    select
        pickup_date as kpi_date,
        'tlc_yellow' as source,
        count(*) as trips,
        countif(is_suspicious) as suspicious_trips,
        sum(total_amount) as revenue,
        cast(null as float64) as cancellation_rate,
        cast(null as float64) as p50_seconds_to_accept
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01'
    group by 1

),

realtime as (

    select
        ride_date as kpi_date,
        'realtime_simulator' as source,
        countif(ride_status = 'completed') as trips,
        countif(has_missing_events) as suspicious_trips,
        sum(fare_amount) as revenue,
        safe_divide(countif(ride_status = 'cancelled'), count(*)) as cancellation_rate,
        approx_quantiles(seconds_to_accept, 100)[offset(50)] as p50_seconds_to_accept
    from {{ ref('fct_rides') }}
    group by 1

)

select * from tlc
union all
select * from realtime
