-- Financial integrity: the total must match the sum of its components.
--
-- Known source issue (found in 2025-01, ~17% of trips): TLC sometimes leaves
-- congestion_surcharge, cbd_congestion_fee and/or airport_fee out of
-- total_amount while still reporting them. A total that matches the sum
-- minus any combination of those fees is accepted; anything else fails.
-- congestion_surcharge is sometimes NULL even though the standard USD 2.50 is
-- inside the total, so a NULL is treated as "unknown": 0 or 2.50.
-- Flex Fare trips (app-based upfront price) are excluded: TLC does not itemize
-- them (fare_amount is often negative, fees are NULL) and only total_amount is
-- reliable. They were 16% of trips in 2025-01 and 29% in 2025-06, which is why
-- including them made this test look worse every month.
-- 1-cent tolerance for rounding; suspicious trips are excluded.
{{ config(severity='warn', warn_if='>100', error_if='>10000') }}

with trips as (

    select
        trip_id,
        total_amount,
        fare_amount + extra_amount + mta_tax + tip_amount + tolls_amount + improvement_surcharge
            as base_sum,
        congestion_surcharge,
        cbd_congestion_fee,
        airport_fee
    from {{ ref('int_trips__enriched') }}
    where not is_suspicious and payment_type != 'flex_fare'

),

checked as (

    select
        *,
        exists (
            select 1
            from unnest([0, 1]) as with_congestion
            cross join unnest([0, 1]) as with_cbd
            cross join unnest([0, 1]) as with_airport
            where abs(
                total_amount
                - (
                    base_sum
                    + with_congestion * coalesce(congestion_surcharge, 2.50)
                    + with_cbd * coalesce(cbd_congestion_fee, 0)
                    + with_airport * coalesce(airport_fee, 0)
                )
            ) <= 0.01
        ) as reconciles
    from trips

)

select trip_id, total_amount, base_sum, congestion_surcharge, cbd_congestion_fee, airport_fee
from checked
where not reconciles
