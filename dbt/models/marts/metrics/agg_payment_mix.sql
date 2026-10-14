{#
    Payment behaviour by borough and month: card vs cash vs app-based flex fares,
    tipping (recorded for card payments only) and dispute rates.
#}
{{ config(tags=['batch']) }}

with mix as (

    select
        date_trunc(pickup_date, month) as pickup_month,
        coalesce(pickup_borough, 'Unknown') as pickup_borough,
        payment_type,
        count(*) as trips,
        sum(total_amount) as revenue,
        sum(tip_amount) as tip_revenue,
        approx_quantiles(tip_pct, 100)[offset(50)] as p50_card_tip_pct,
        countif(tip_amount > 0) as tipped_trips
    from {{ ref('fct_trips') }}
    where pickup_date >= '2020-01-01' and not is_suspicious
    group by 1, 2, 3

)

select
    pickup_month,
    pickup_borough,
    payment_type,
    trips,
    revenue,
    tip_revenue,
    p50_card_tip_pct,
    safe_divide(revenue, trips) as avg_ticket,
    safe_divide(tipped_trips, trips) as tipped_share,
    safe_divide(trips, sum(trips) over (partition by pickup_month, pickup_borough)) as share_of_borough_trips,
    safe_divide(revenue, sum(revenue) over (partition by pickup_month, pickup_borough)) as share_of_borough_revenue,
    safe_divide(trips, sum(trips) over (partition by pickup_month)) as share_of_month_trips
from mix
