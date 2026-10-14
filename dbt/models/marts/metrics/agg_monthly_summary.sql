{#
    Executive summary: one row per month, built on the daily metrics, with
    month-over-month growth and normalisation by calendar days (February is short).
#}
{{ config(tags=['batch']) }}

with monthly as (

    select
        date_trunc(pickup_date, month) as pickup_month,
        count(*) as days_observed,
        countif(not is_weekend) as weekdays_observed,
        sum(trips) as trips,
        sum(revenue) as revenue,
        sum(cbd_toll_revenue) as cbd_toll_revenue,
        sum(trips * airport_share) as airport_trips,
        sum(trips * card_share) as card_trips,
        sum(trips * cbd_tolled_share) as cbd_tolled_trips,
        avg(p50_speed_mph) as avg_daily_p50_speed_mph,
        avg(p50_duration_minutes) as avg_daily_p50_duration_minutes,
        avg(if(is_weekend, null, trips)) as avg_weekday_trips,
        avg(if(is_weekend, trips, null)) as avg_weekend_trips,
        max_by(pickup_date, trips) as busiest_day,
        max(trips) as busiest_day_trips
    from {{ ref('agg_daily_trip_metrics') }}
    group by 1

),

top_zone as (

    select
        date_trunc(trips.pickup_date, month) as pickup_month,
        approx_top_count(zones.zone_name, 1)[safe_offset(0)].value as top_pickup_zone
    from {{ ref('fct_trips') }} as trips
    left join {{ ref('dim_zones') }} as zones
        on trips.pickup_zone_id = zones.zone_id and zones.is_current
    where trips.pickup_date >= '2020-01-01' and not trips.is_suspicious
    group by 1

)

select
    monthly.pickup_month,
    format_date('%Y-%m', monthly.pickup_month) as year_month,
    monthly.days_observed,
    monthly.trips,
    monthly.revenue,
    monthly.cbd_toll_revenue,
    monthly.avg_daily_p50_speed_mph,
    monthly.avg_daily_p50_duration_minutes,
    monthly.avg_weekday_trips,
    monthly.avg_weekend_trips,
    monthly.busiest_day,
    monthly.busiest_day_trips,
    top_zone.top_pickup_zone,
    safe_divide(monthly.trips, monthly.days_observed) as avg_trips_per_day,
    safe_divide(monthly.revenue, monthly.days_observed) as avg_revenue_per_day,
    safe_divide(monthly.revenue, monthly.trips) as avg_ticket,
    safe_divide(monthly.airport_trips, monthly.trips) as airport_share,
    safe_divide(monthly.card_trips, monthly.trips) as card_share,
    safe_divide(monthly.cbd_tolled_trips, monthly.trips) as cbd_tolled_share,
    safe_divide(monthly.avg_weekend_trips, monthly.avg_weekday_trips) as weekend_to_weekday_ratio,
    -- Growth on per-day averages, so month length does not distort it
    safe_divide(
        safe_divide(monthly.trips, monthly.days_observed),
        lag(safe_divide(monthly.trips, monthly.days_observed)) over by_month
    ) - 1 as trips_per_day_mom_change,
    safe_divide(
        safe_divide(monthly.revenue, monthly.days_observed),
        lag(safe_divide(monthly.revenue, monthly.days_observed)) over by_month
    ) - 1 as revenue_per_day_mom_change,
    safe_divide(monthly.revenue, monthly.trips)
    / lag(safe_divide(monthly.revenue, monthly.trips)) over by_month - 1 as avg_ticket_mom_change
from monthly
left join top_zone on monthly.pickup_month = top_zone.pickup_month
window by_month as (order by monthly.pickup_month)
