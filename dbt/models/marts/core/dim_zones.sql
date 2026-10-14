{{ config(tags=['batch', 'streaming']) }}

-- Zone dimension with history (SCD type 2) built from the snapshot
select
    {{ dbt_utils.generate_surrogate_key(['zone_id', 'dbt_valid_from']) }} as zone_key,
    zone_id,
    borough,
    zone_name,
    service_zone,
    is_unknown_zone,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31')) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_taxi_zones') }}
