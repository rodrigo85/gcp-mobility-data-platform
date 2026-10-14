-- Financial integrity: the total must match the sum of its components.
-- 1-cent tolerance for rounding; suspicious trips are excluded.
{{ config(severity='warn', warn_if='>100', error_if='>10000') }}

select
    trip_id,
    total_amount,
    fare_amount + extra_amount + mta_tax + tip_amount + tolls_amount
    + improvement_surcharge + congestion_surcharge + airport_fee + cbd_congestion_fee as parts_sum
from {{ ref('int_trips__enriched') }}
where
    not is_suspicious
    and abs(
        total_amount
        - (
            fare_amount + extra_amount + mta_tax + tip_amount + tolls_amount
            + improvement_surcharge + congestion_surcharge + airport_fee + cbd_congestion_fee
        )
    ) > 0.01
