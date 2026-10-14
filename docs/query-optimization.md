# Query optimization report

> **Status:** methodology ready; the numbers will be filled in with real measurements after the first 2025 load.

## Methodology

Each case runs in a "before" and an "after" version, with the cache disabled (`use_query_cache=false`). Numbers come from `INFORMATION_SCHEMA`, not the console:

```sql
select
  job_id,
  (select value from unnest(labels) where key = 'case') as case_name,
  (select value from unnest(labels) where key = 'variant') as variant,
  total_bytes_processed / pow(1024, 3) as gib_processed,
  total_bytes_billed / pow(1024, 3) as gib_billed,
  total_slot_ms / 1000 as slot_seconds,
  timestamp_diff(end_time, start_time, millisecond) as elapsed_ms
from `region-us-central1`.INFORMATION_SCHEMA.JOBS
where creation_time > timestamp_sub(current_timestamp(), interval 1 day)
  and job_type = 'QUERY'
  and exists (select 1 from unnest(labels) where key = 'case')
order by case_name, variant
```

Before each variant, label the session with `set @@query_label = "case:hotspots,variant:before";` (or `variant:after`).

## Cases

| # | Case | Technique | Bytes before | Bytes after | Slot-s before | Slot-s after |
|---|---|---|---|---|---|---|
| 1 | Revenue per zone on Fridays in Jan 2025 ([analyses/query_optimization_hotspots.sql](../dbt/analyses/query_optimization_hotspots.sql)) | Partition pruning, aggregate before the join | — | — | — | — |
| 2 | Trips for a specific zone | Clustering on `pickup_zone_id` | — | — | — | — |
| 3 | Demand vs. the average for the same hour of the week | `range between` window instead of a self-join | — | — | — | — |
| 4 | Distinct passenger count per day | `approx_count_distinct` vs `count(distinct)` | — | — | — | — |
| 5 | MERGE in `int_rides__lifecycle` | `incremental_predicates` limiting the target scan | — | — | — | — |

## Anti-patterns found

Record every problematic query investigated here: symptom, execution plan (the stage with the highest `slot_ms` in `INFORMATION_SCHEMA.JOBS.job_stages`), root cause and fix.
