{#
    One row per pickup zone: volume, revenue, unit economics and service speed,
    with ranks and a Pareto curve (how few zones make most of the revenue).
#}
{{ config(tags=['batch']) }}

with by_zone as (

    select
        pickup_zone_id,
        count(*) as trips,
        count(distinct pickup_date) as active_days,
        sum(total_amount) as revenue,
        sum(trip_distance_miles) as total_miles,
        approx_quantiles(trip_duration_minutes, 100)[offset(50)] as p50_duration_minutes,
        approx_quantiles(avg_speed_mph, 100)[offset(50)] as p50_speed_mph,
        approx_quantiles(tip_pct, 100)[offset(50)] as p50_card_tip_pct,
        countif(cbd_congestion_fee > 0) as cbd_tolled_trips,
        countif(pickup_zone_id = dropoff_zone_id) as same_zone_trips,
        approx_top_count(dropoff_zone_id, 1)[safe_offset(0)].value as top_dropoff_zone_id
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01' and not is_suspicious
    group by 1

),

zones as (

    select
        zone_id,
        zone_name,
        borough,
        service_zone
    from {{ ref('dim_zones') }}
    where is_current

),

ranked as (

    select
        by_zone.*,
        zones.zone_name,
        zones.borough,
        zones.service_zone,
        top_dropoff.zone_name as top_dropoff_zone_name,
        {{ airport_code('by_zone.pickup_zone_id') }} is not null as is_airport,
        rank() over (order by by_zone.revenue desc) as revenue_rank,
        rank() over (order by by_zone.trips desc) as trips_rank,
        rank() over (partition by zones.borough order by by_zone.revenue desc) as revenue_rank_in_borough,
        safe_divide(by_zone.revenue, sum(by_zone.revenue) over ()) as share_of_revenue
    from by_zone
    left join zones on by_zone.pickup_zone_id = zones.zone_id
    left join zones as top_dropoff on by_zone.top_dropoff_zone_id = top_dropoff.zone_id

)

select
    pickup_zone_id,
    zone_name,
    borough,
    service_zone,
    is_airport,
    top_dropoff_zone_name,
    trips,
    revenue,
    p50_duration_minutes,
    p50_speed_mph,
    p50_card_tip_pct,
    revenue_rank,
    trips_rank,
    revenue_rank_in_borough,
    share_of_revenue,
    safe_divide(trips, active_days) as avg_trips_per_day,
    safe_divide(revenue, trips) as avg_ticket,
    safe_divide(revenue, total_miles) as revenue_per_mile,
    safe_divide(cbd_tolled_trips, trips) as cbd_tolled_share,
    safe_divide(same_zone_trips, trips) as same_zone_share,
    sum(share_of_revenue) over (order by revenue desc, pickup_zone_id rows unbounded preceding) as cumulative_revenue_share,
    case
        when revenue_rank <= 10 then 'top_10'
        when sum(share_of_revenue) over (order by revenue desc, pickup_zone_id rows unbounded preceding) <= 0.8
            then 'core_80pct'
        else 'long_tail'
    end as revenue_tier
from ranked
