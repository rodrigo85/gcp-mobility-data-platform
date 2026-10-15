# Data quality findings

Issues found in the source data by the pipeline's tests, with the decision taken for each.

## TLC yellow trips, 2025-01: amounts

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

## TLC yellow trips, 2025-01 to 2025-06: Flex Fare

With six months loaded, the reconciliation warning grew from 85,095 rows (2.4%) to 889,534 (3.7%), and the rate went up almost every month. The source did not get worse. The share of a payment type that the test could never reconcile went up.

Measured with Spark on the normalized Parquet files on the VPS (no BigQuery bytes), same rules as the dbt test:

| Month | Flex Fare share of trips | Flex Fare trips that do not reconcile | Other trips that do not reconcile |
|---|---|---|---|
| 2025-01 | 15.7% | 15.2% | 0.12% |
| 2025-02 | 22.9% | 16.0% | 0.11% |
| 2025-03 | 22.6% | 15.5% | 0.12% |
| 2025-04 | 19.3% | 15.5% | 0.12% |
| 2025-05 | 26.3% | 16.9% | 0.13% |
| 2025-06 | 29.0% | 16.8% | 0.13% |

- 97.5% of the mismatches are `payment_type = 0` (Flex Fare: an upfront price agreed in the app), almost all from vendor 2.
- In those rows TLC does not itemize the price: `fare_amount` averages **USD −3.73**, while `congestion_surcharge` and `airport_fee` are NULL. Only `total_amount` is reliable.
- Outside Flex Fare, the mismatch rate is flat at about 0.12% a month.

| # | Finding | Decision |
|---|---|---|
| 6 | Flex Fare trips are not itemized | Excluded from `assert_trip_amounts_reconcile`; the test now tracks a stable ~0.12% that would expose a real regression |
| 7 | `fare_amount` is negative or meaningless for Flex Fare | Marts compute unit economics from `total_amount` (`revenue_per_mile`, `avg_ticket`), never from `fare_amount` |
| 8 | Flex Fare adoption nearly doubled in six months | Reported as a business metric (`flex_fare_share` in `agg_daily_trip_metrics` and `agg_monthly_summary`) |

Lesson: a quality metric that drifts upward is not always data getting worse. Here the mix of the population behind it changed, and splitting the metric by segment exposed that.

## Analytics marts and the dashboard (2026-10-15)

Found while building the [dashboard](../dashboard/index.html) from the nine marts.

| # | Finding | Decision |
|---|---|---|
| 9 | The marts count `not is_suspicious`, so they total 23,425,975 trips against 24,082,454 rows in the six monthly loads. About 650,000 suspicious trips (2.7% of all trips, rebuilt from each day's `suspicious_share`) explain almost all of that gap; about 6,500 trips (0.03%) remain unexplained | The dashboard says so in its footer and its README section. The remaining 0.03% is open: check the `pickup_date >= '2020-01-01'` filter and trips outside the month of their file |
| 10 | `is_toll_active` in `agg_congestion_toll_weekly` is `true` in all 27 Manhattan weeks, including the week of December 30, so it does not separate before from after the toll | The dashboard marks the toll start with a fixed date (January 5, 2025) and does not use the column. Fix or drop the column in dbt |
| 11 | The first and last weeks of `agg_congestion_toll_weekly` are partial (data starts on January 1 and ends on June 30) | Noted under the weekly toll chart so the low bars are not read as a drop |
