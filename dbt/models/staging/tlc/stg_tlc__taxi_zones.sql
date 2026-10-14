select
    LocationID as zone_id,
    Borough as borough,
    Zone as zone_name,
    service_zone,
    LocationID in (264, 265) as is_unknown_zone
from {{ ref('taxi_zone_lookup') }}
