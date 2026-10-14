{#
    Origin-destination matrix between boroughs: where trips start, where they
    end and what each flow is worth. Intra-borough flows (Manhattan to Manhattan)
    dominate volume; cross-borough and airport flows dominate ticket size.
#}
{{ config(tags=['batch']) }}

with flows as (

    select
        coalesce(pickup_borough, 'Unknown') as pickup_borough,
        coalesce(dropoff_borough, 'Unknown') as dropoff_borough,
        count(*) as trips,
        sum(total_amount) as revenue,
        sum(trip_distance_miles) as total_miles,
        approx_quantiles(trip_distance_miles, 100)[offset(50)] as p50_distance_miles,
        approx_quantiles(trip_duration_minutes, 100)[offset(50)] as p50_duration_minutes,
        approx_quantiles(avg_speed_mph, 100)[offset(50)] as p50_speed_mph,
        countif(cbd_congestion_fee > 0) as cbd_tolled_trips
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01' and not is_suspicious
    group by 1, 2

)

select
    pickup_borough,
    dropoff_borough,
    concat(pickup_borough, ' → ', dropoff_borough) as flow,
    pickup_borough = dropoff_borough as is_intra_borough,
    trips,
    revenue,
    p50_distance_miles,
    p50_duration_minutes,
    p50_speed_mph,
    safe_divide(revenue, trips) as avg_ticket,
    safe_divide(revenue, total_miles) as revenue_per_mile,
    safe_divide(cbd_tolled_trips, trips) as cbd_tolled_share,
    safe_divide(trips, sum(trips) over ()) as share_of_trips,
    safe_divide(revenue, sum(revenue) over ()) as share_of_revenue,
    -- Of everything leaving this borough, the share going to this destination
    safe_divide(trips, sum(trips) over (partition by pickup_borough)) as share_of_origin_trips,
    rank() over (order by trips desc) as volume_rank
from flows
