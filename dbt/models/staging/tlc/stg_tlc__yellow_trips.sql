with source as (

    select * from {{ source('tlc', 'yellow_trips') }}
    {{ limit_dev_data('pickup_datetime') }}

)

select
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    date(pickup_datetime) as pickup_date,
    passenger_count,
    trip_distance as trip_distance_miles,
    rate_code_id,
    coalesce(store_and_fwd_flag = 'Y', false) as is_store_and_forward,
    pickup_zone_id,
    dropoff_zone_id,
    payment_type as payment_type_id,
    case payment_type
        when 0 then 'flex_fare'
        when 1 then 'credit_card'
        when 2 then 'cash'
        when 3 then 'no_charge'
        when 4 then 'dispute'
        when 5 then 'unknown'
        when 6 then 'voided'
        else 'other'
    end as payment_type,
    fare_amount,
    extra as extra_amount,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    coalesce(airport_fee, 0) as airport_fee,
    coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
    total_amount,
    _batch_month,
    _ingested_at
from source
