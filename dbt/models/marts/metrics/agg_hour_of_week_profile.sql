{#
    Typical week: one row per day of week and hour of day. Volumes are averaged
    per calendar day (a month has four or five Mondays), so cells are comparable.
    Feeds the demand heatmap and the "when is traffic slowest" view.
#}
{{ config(tags=['batch']) }}

with trips as (

    select
        pickup_date,
        extract(dayofweek from pickup_datetime) as day_of_week,
        format_datetime('%A', pickup_datetime) as day_name,
        extract(hour from pickup_datetime) as hour_of_day,
        total_amount,
        trip_duration_minutes,
        avg_speed_mph,
        tip_pct
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01' and not is_suspicious

),

profile as (

    select
        day_of_week,
        any_value(day_name) as day_name,
        hour_of_day,
        count(distinct pickup_date) as days_observed,
        count(*) as trips,
        sum(total_amount) as revenue,
        approx_quantiles(trip_duration_minutes, 100)[offset(50)] as p50_duration_minutes,
        approx_quantiles(avg_speed_mph, 100)[offset(50)] as p50_speed_mph,
        approx_quantiles(tip_pct, 100)[offset(50)] as p50_card_tip_pct
    from trips
    group by 1, 3

)

select
    day_of_week,
    day_name,
    hour_of_day,
    day_of_week in (1, 7) as is_weekend,
    case
        when hour_of_day between 7 and 9 then 'am_peak'
        when hour_of_day between 16 and 19 then 'pm_peak'
        when hour_of_day between 10 and 15 then 'midday'
        when hour_of_day between 20 and 23 then 'evening'
        else 'overnight'
    end as day_part,
    days_observed,
    p50_duration_minutes,
    p50_speed_mph,
    p50_card_tip_pct,
    safe_divide(trips, days_observed) as avg_trips_per_day,
    safe_divide(revenue, days_observed) as avg_revenue_per_day,
    safe_divide(revenue, trips) as avg_ticket,
    safe_divide(trips, sum(trips) over ()) as share_of_week_trips,
    -- 1.0 = average hour of the week; 2.0 = twice as busy
    safe_divide(
        safe_divide(trips, days_observed),
        avg(safe_divide(trips, days_observed)) over ()
    ) as demand_vs_avg_hour,
    rank() over (order by safe_divide(trips, days_observed) desc) as busiest_rank,
    rank() over (order by p50_speed_mph asc) as slowest_traffic_rank
from profile
