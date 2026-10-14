{#
    Weekly tracker of the NYC congestion relief zone toll (started on the
    cbd_toll_start_date var): how many trips pay it, how much it collects, and
    the speed of Manhattan traffic at weekday peaks, the street-level effect
    the toll is meant to improve.
#}
{{ config(tags=['batch']) }}

with trips as (

    select
        date_trunc(pickup_date, isoweek) as week_start,
        coalesce(pickup_borough, 'Unknown') as pickup_borough,
        cbd_congestion_fee,
        total_amount,
        avg_speed_mph,
        pickup_borough = 'Manhattan' and dropoff_borough = 'Manhattan' as is_intra_manhattan,
        extract(dayofweek from pickup_datetime) between 2 and 6
        and extract(hour from pickup_datetime) in (8, 9, 17, 18) as is_weekday_peak
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01' and not is_suspicious

),

weekly as (

    select
        week_start,
        pickup_borough,
        count(*) as trips,
        countif(cbd_congestion_fee > 0) as tolled_trips,
        sum(cbd_congestion_fee) as toll_revenue,
        sum(total_amount) as revenue,
        approx_quantiles(if(is_intra_manhattan and is_weekday_peak, avg_speed_mph, null), 100)[
            offset(50)
        ] as p50_manhattan_peak_speed_mph
    from trips
    group by 1, 2

)

select
    week_start,
    pickup_borough,
    trips,
    tolled_trips,
    toll_revenue,
    p50_manhattan_peak_speed_mph,
    week_start >= date_trunc(date '{{ var("cbd_toll_start_date") }}', isoweek) as is_toll_active,
    safe_divide(tolled_trips, trips) as tolled_share,
    safe_divide(toll_revenue, tolled_trips) as avg_toll_per_tolled_trip,
    safe_divide(toll_revenue, revenue) as toll_share_of_revenue,
    safe_divide(toll_revenue, sum(toll_revenue) over (partition by week_start)) as share_of_week_toll_revenue
from weekly
