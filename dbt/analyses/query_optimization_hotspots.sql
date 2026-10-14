-- Case study for docs/query-optimization.md.
-- "Which zones had the most revenue on Fridays in January 2025?"

-- BEFORE: reads the staging view (no useful partitioning), applies a function
-- to the date column (prevents pruning) and joins before aggregating.
--
-- select z.zone_name, sum(t.total_amount) as revenue
-- from {{ ref('stg_tlc__yellow_trips') }} t
-- join {{ ref('stg_tlc__taxi_zones') }} z on t.pickup_zone_id = z.zone_id
-- where format_date('%Y-%m', date(t.pickup_datetime)) = '2025-01'
--   and extract(dayofweek from t.pickup_datetime) = 6
-- group by 1 order by 2 desc limit 10

-- AFTER: reads the partitioned, clustered fact, filters the partition with a
-- literal range (pruning), aggregates before the join and joins only 10 rows.
with top_zones as (

    select
        pickup_zone_id,
        sum(total_amount) as revenue
    from {{ ref('fct_trips') }}
    where
        pickup_date between '2025-01-01' and '2025-01-31'
        and extract(dayofweek from pickup_date) = 6
    group by pickup_zone_id
    order by revenue desc
    limit 10

)

select
    zones.zone_name,
    top_zones.revenue
from top_zones
inner join {{ ref('dim_zones') }} as zones
    on top_zones.pickup_zone_id = zones.zone_id
    and zones.is_current
order by top_zones.revenue desc

-- Measurement: run both versions and compare them in
-- region-us-central1.INFORMATION_SCHEMA.JOBS (total_bytes_processed,
-- total_slot_ms) using the query in docs/query-optimization.md.
