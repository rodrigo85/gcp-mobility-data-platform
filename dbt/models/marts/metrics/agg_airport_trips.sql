{#
    Airport trips (EWR, JFK, LGA) by direction. Airports are a small share of
    trips but a large share of revenue: long distances, tolls and airport fees.
#}
{{ config(tags=['batch']) }}

with trips as (

    select
        pickup_date,
        {{ airport_code('pickup_zone_id') }} as pickup_airport,
        {{ airport_code('dropoff_zone_id') }} as dropoff_airport,
        dropoff_borough,
        pickup_borough,
        payment_type,
        trip_distance_miles,
        trip_duration_minutes,
        total_amount,
        tolls_amount,
        airport_fee,
        tip_pct
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01' and not is_suspicious

),

totals as (

    select
        count(*) as all_trips,
        sum(total_amount) as all_revenue
    from trips

),

airport_trips as (

    -- A trip between two airports counts once, as a departure from the first
    select
        coalesce(pickup_airport, dropoff_airport) as airport,
        if(pickup_airport is not null, 'from_airport', 'to_airport') as direction,
        if(pickup_airport is not null, dropoff_borough, pickup_borough) as city_side_borough,
        *
    from trips
    where pickup_airport is not null or dropoff_airport is not null

),

summary as (

    select
        airport,
        direction,
        count(*) as trips,
        count(distinct pickup_date) as days_observed,
        sum(total_amount) as revenue,
        sum(tolls_amount) as toll_revenue,
        sum(airport_fee) as airport_fee_revenue,
        approx_quantiles(trip_distance_miles, 100)[offset(50)] as p50_distance_miles,
        approx_quantiles(trip_duration_minutes, 100)[offset(50)] as p50_duration_minutes,
        approx_quantiles(trip_duration_minutes, 100)[offset(90)] as p90_duration_minutes,
        approx_quantiles(tip_pct, 100)[offset(50)] as p50_card_tip_pct,
        countif(payment_type = 'credit_card') as card_trips,
        approx_top_count(city_side_borough, 1)[safe_offset(0)].value as top_city_side_borough
    from airport_trips
    group by 1, 2

)

select
    summary.airport,
    summary.direction,
    summary.trips,
    summary.revenue,
    summary.toll_revenue,
    summary.airport_fee_revenue,
    summary.p50_distance_miles,
    summary.p50_duration_minutes,
    summary.p90_duration_minutes,
    summary.p50_card_tip_pct,
    summary.top_city_side_borough,
    safe_divide(summary.trips, summary.days_observed) as avg_trips_per_day,
    safe_divide(summary.revenue, summary.trips) as avg_ticket,
    safe_divide(summary.card_trips, summary.trips) as card_share,
    safe_divide(summary.trips, totals.all_trips) as share_of_all_trips,
    safe_divide(summary.revenue, totals.all_revenue) as share_of_all_revenue
from summary
cross join totals
