# Data quality findings

Issues found in the source data by the pipeline's tests, with the decision taken for each.

## TLC yellow trips, 2025-01

Measured on 3,475,226 raw rows by the Spark quality rules and `assert_trip_amounts_reconcile`.

| # | Finding | Rows | Decision |
|---|---|---|---|
| 1 | Drop-off before pick-up | 124 | Quarantined by Spark (`dropoff_before_pickup`) |
| 2 | Pick-up outside the file's month | 22 | Quarantined by Spark (`pickup_outside_batch_month`); the monthly load replaces one partition and must not touch others |
| 3 | `extra` already includes the congestion and CBD fees that are also reported in their own columns | ~560,000 | Accepted: the reconciliation test allows totals that leave any combination of `congestion_surcharge`, `cbd_congestion_fee` and `airport_fee` out of the sum |
| 4 | `congestion_surcharge` is NULL although the standard USD 2.50 is inside `total_amount` | included in #3 | Accepted: a NULL surcharge is treated as unknown (0 or 2.50) |
| 5 | Total does not match any combination of the components | 85,095 (2.4%) | Kept as a test **warning**; rows stay in `fct_trips` and can be inspected with the test query |

How the mismatches were diagnosed (cheap: one month, a few columns):

```sql
select
  round(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount
        + improvement_surcharge), 2) as total_minus_base,
  extra, congestion_surcharge, cbd_congestion_fee, airport_fee, count(*) as trips
from `<PROJECT_ID>.bronze.yellow_trips`
where _batch_month = '2025-01'
group by 1, 2, 3, 4, 5
order by trips desc
```
