{#
    Demand per zone and hour, compared with the average for the same hour of
    the week over the previous 8 weeks. Answers questions like: "is demand at
    JFK at 6 pm today above normal for a Friday?".
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'pickup_date', 'data_type': 'date'},
        cluster_by=['pickup_zone_id'],
        tags=['batch'],
    )
}}

{#
    The incremental cut-off is resolved before the query runs: a filter built
    from a subquery cannot be used for partition elimination, and fct_trips
    requires one (require_partition_filter).
#}
{% if is_incremental() %}
    {% set last_date = run_query("select max(pickup_date) from " ~ this).columns[0].values()[0] if execute else none %}
{% endif %}

with hourly as (

    select
        pickup_date,
        datetime_trunc(pickup_datetime, hour) as pickup_hour,
        pickup_zone_id,
        any_value(pickup_borough) as pickup_borough,
        count(*) as trips,
        sum(total_amount) as revenue,
        approx_quantiles(trip_duration_minutes, 100)[offset(50)] as p50_duration_minutes,
        approx_quantiles(trip_duration_minutes, 100)[offset(90)] as p90_duration_minutes,
        avg(tip_pct) as avg_tip_pct
    from {{ ref('fct_trips') }}
    where not is_suspicious
    {% if is_incremental() %}
        -- 8 weeks of history for the rolling average + the new period
        and pickup_date >= date_sub(date '{{ last_date }}', interval 63 day)
    {% else %}
        and pickup_date >= '2020-01-01'
    {% endif %}
    group by 1, 2, 3

),

with_baseline as (

    select
        *,
        avg(trips) over (
            partition by pickup_zone_id, extract(dayofweek from pickup_hour), extract(hour from pickup_hour)
            order by unix_date(pickup_date)
            range between 56 preceding and 1 preceding
        ) as baseline_trips
    from hourly

)

select
    *,
    safe_divide(trips, baseline_trips) as demand_index
from with_baseline
{% if is_incremental() %}
where pickup_date > date '{{ last_date }}'
{% endif %}
