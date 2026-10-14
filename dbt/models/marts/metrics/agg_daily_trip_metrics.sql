{#
    One row per day with the operational KPIs of the TLC fleet: volume, revenue,
    unit economics, service speed, payment behaviour, airport and congestion-toll
    exposure, plus trend columns (7-day moving average, week-over-week change).
    Suspicious trips are excluded from every metric and reported as a share.
#}
{{ config(tags=['batch']) }}

with trips as (

    select
        pickup_date,
        is_suspicious,
        payment_type,
        passenger_count,
        trip_distance_miles,
        trip_duration_minutes,
        avg_speed_mph,
        tip_pct,
        total_amount,
        cbd_congestion_fee,
        {{ airport_code('pickup_zone_id') }} as pickup_airport,
        {{ airport_code('dropoff_zone_id') }} as dropoff_airport
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01'

),

daily as (

    select
        pickup_date,
        countif(not is_suspicious) as trips,
        countif(is_suspicious) as suspicious_trips,
        sum(if(is_suspicious, 0, total_amount)) as revenue,
        sum(if(is_suspicious, 0, trip_distance_miles)) as total_miles,
        avg(if(is_suspicious, null, passenger_count)) as avg_passengers,
        approx_quantiles(if(is_suspicious, null, trip_distance_miles), 100)[offset(50)] as p50_distance_miles,
        approx_quantiles(if(is_suspicious, null, trip_duration_minutes), 100)[offset(50)] as p50_duration_minutes,
        approx_quantiles(if(is_suspicious, null, trip_duration_minutes), 100)[offset(90)] as p90_duration_minutes,
        approx_quantiles(if(is_suspicious, null, avg_speed_mph), 100)[offset(50)] as p50_speed_mph,
        approx_quantiles(if(is_suspicious, null, tip_pct), 100)[offset(50)] as p50_card_tip_pct,
        countif(not is_suspicious and payment_type = 'credit_card') as card_trips,
        countif(not is_suspicious and payment_type = 'cash') as cash_trips,
        countif(not is_suspicious and payment_type = 'dispute') as disputed_trips,
        countif(not is_suspicious and payment_type = 'flex_fare') as flex_fare_trips,
        countif(not is_suspicious and coalesce(pickup_airport, dropoff_airport) is not null) as airport_trips,
        countif(not is_suspicious and cbd_congestion_fee > 0) as cbd_tolled_trips,
        sum(if(is_suspicious, 0, cbd_congestion_fee)) as cbd_toll_revenue
    from trips
    group by 1

),

ratios as (

    select
        pickup_date,
        trips,
        revenue,
        cbd_toll_revenue,
        avg_passengers,
        p50_distance_miles,
        p50_duration_minutes,
        p90_duration_minutes,
        p50_speed_mph,
        p50_card_tip_pct,
        safe_divide(revenue, trips) as avg_ticket,
        -- total_amount, not fare_amount: Flex Fare trips do not itemize the fare
        safe_divide(revenue, total_miles) as revenue_per_mile,
        safe_divide(card_trips, trips) as card_share,
        safe_divide(cash_trips, trips) as cash_share,
        safe_divide(disputed_trips, trips) as dispute_rate,
        safe_divide(flex_fare_trips, trips) as flex_fare_share,
        safe_divide(airport_trips, trips) as airport_share,
        safe_divide(cbd_tolled_trips, trips) as cbd_tolled_share,
        safe_divide(suspicious_trips, trips + suspicious_trips) as suspicious_share
    from daily

)

select
    ratios.*,
    dates.day_name,
    dates.is_weekend,
    if(
        ratios.pickup_date >= date '{{ var("cbd_toll_start_date") }}', 'after_toll', 'before_toll'
    ) as cbd_toll_period,
    avg(ratios.trips) over trailing_week as trips_7d_avg,
    avg(ratios.revenue) over trailing_week as revenue_7d_avg,
    safe_divide(ratios.trips, lag(ratios.trips, 7) over by_day) - 1 as trips_wow_change,
    safe_divide(ratios.revenue, lag(ratios.revenue, 7) over by_day) - 1 as revenue_wow_change,
    safe_divide(ratios.revenue, sum(ratios.revenue) over by_month_total) as share_of_month_revenue
from ratios
left join {{ ref('dim_date') }} as dates on ratios.pickup_date = dates.date_day
window
    by_day as (order by ratios.pickup_date),
    trailing_week as (order by unix_date(ratios.pickup_date) range between 6 preceding and current row),
    by_month_total as (partition by date_trunc(ratios.pickup_date, month))
