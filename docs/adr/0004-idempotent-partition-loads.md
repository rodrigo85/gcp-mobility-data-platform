# ADR 0004: Idempotent loads through partition replacement

**Status:** accepted · **Date:** 2026-10-14

## Context

Reprocessing is routine: the TLC republishes corrected files, a Spark bug forces a full-year rerun, a task fails halfway. Every rerun must produce exactly the same result as the first run.

## Decision

The unit of reprocessing is the partition, and every write replaces the whole partition:

| Step | Mechanism |
|---|---|
| Download | `.part` file and atomic rename; skipped if the size matches |
| Spark | `mode("overwrite")` on the `batch_month=YYYY-MM` directory |
| GCS | Clear the month's prefix before uploading (Spark file names change on every run) |
| BigQuery bronze | Load job with `WRITE_TRUNCATE` into `yellow_trips$YYYYMM` |
| dbt gold | `insert_overwrite` per day, selected by `_ingested_at` |

`trip_id` is a hash of the canonical fields: the same ride always gets the same ID, on any run.

## Consequences

- A backfill is just `airflow dags backfill` (or clearing the runs) over the desired range.
- A load job with a partition decorator requires every row to belong to that month. Spark guarantees this: rows from other months go to quarantine with the reason `pickup_outside_batch_month`.
- We do not use MERGE in bronze: it would scan the target table and cost money.
