# Data SLOs

| Dataset | Indicator | Objective | How it is measured | Alert |
|---|---|---|---|---|
| `bronze.yellow_trips` | Freshness | A month published by the TLC is loaded within 24 h | Sensor + `dbt source freshness` (warn 45 d, error 75 d since the last load) | DAG failure |
| `bronze.yellow_trips` | Completeness | 100% of Spark's valid rows land in BigQuery | `reconcile_counts` task | DAG failure |
| `bronze.yellow_trips` | Quarantine | < 2% of the month's rows | Spark metrics (`quarantine_rows / raw_rows`) | To do: alert |
| `bronze.ride_events` | Freshness | Events in BigQuery within 15 min | `dbt source freshness` (warn 15 min, error 60 min) | `dbt_streaming` failure |
| `bronze.ride_events_dlq` | Rejection rate | < 1% of events | Beam `ride_events/invalid` counters | To do: alert |
| `gold.fct_trips` | Financial integrity | ≤ 100 rides per month whose total ≠ sum of components | `assert_trip_amounts_reconcile` test | warn > 100, error > 10,000 |
| `gold.*` | Availability | dbt build green on 99% of runs | Run history in Airflow | DAG failure |

## Error budget

SLOs are monthly. When an indicator exhausts its budget, reliability takes priority over new features until it recovers.
