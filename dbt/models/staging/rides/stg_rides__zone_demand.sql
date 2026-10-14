-- With ACCUMULATING, each pane carries the corrected window total.
-- Keep only the last pane of each (zone, window).
with source as (

    select * from {{ source('rides', 'zone_demand_1m') }}
    {{ limit_dev_data('window_start', 'timestamp') }}

)

select
    pickup_zone_id,
    window_start,
    window_end,
    requested,
    completed,
    cancelled,
    cast(revenue as numeric) as revenue,
    pane_index,
    pane_timing,
    pane_timing = 'LATE' as was_corrected_by_late_data,
    ingest_ts
from source
qualify row_number() over (
    partition by pickup_zone_id, window_start
    order by pane_index desc, ingest_ts desc
) = 1
