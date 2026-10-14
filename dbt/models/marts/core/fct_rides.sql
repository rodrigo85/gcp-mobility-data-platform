{{
    config(
        partition_by={'field': 'ride_date', 'data_type': 'date'},
        cluster_by=['pickup_zone_id', 'ride_status'],
        tags=['streaming'],
    )
}}

select
    lifecycle.*,
    pickup.borough as pickup_borough,
    pickup.zone_name as pickup_zone_name
from {{ ref('int_rides__lifecycle') }} as lifecycle
left join {{ ref('dim_zones') }} as pickup
    on lifecycle.pickup_zone_id = pickup.zone_id
    and pickup.is_current
