# Runbook: backlog on the Beam subscription

**Symptom:** `dbt source freshness` fails on `rides.ride_events` (more than 60 min without data), or the `subscription/num_undelivered_messages` metric for `ride-events-beam` keeps growing.

## Diagnosis

1. **Is the pipeline running?**
   - Dataflow: `gcloud dataflow jobs list --region us-central1 --status active`
   - Local/VPS: is the `mobility_beam.streaming_rides` process alive?
2. **Are messages going to the dead letter topic?**
   `gcloud pubsub subscriptions pull ride-events-dead-letter-inspection --limit 5` shows messages that failed 10 times. If there are many, a bug is crashing processing: check the worker logs.
3. **Is Beam's DLQ growing?**
   ```sql
   select error, count(*) from bronze.ride_events_dlq
   where received_ts > timestamp_sub(current_timestamp(), interval 1 hour)
   group by error order by 2 desc
   ```
   A new dominant error usually means the producer changed its schema.
4. **Are load jobs failing?** Look for failed jobs in `INFORMATION_SCHEMA.JOBS` over the last hour. Common cause: the table schema (Terraform) does not match the payload.

## Mitigation

| Cause | Action |
|---|---|
| Pipeline stopped | Restart it. The subscription retains 24 h; nothing is lost within that window |
| Producer schema change | Update the contract in `schemas.py` and `bigquery.tf`, then reprocess the DLQ |
| Volume above capacity | Dataflow: temporarily raise `max_num_workers` |
| Messages in the dead letter topic | Fix the bug and republish from `ride-events-dead-letter-inspection` |

## After the incident

- LATE panes correct the affected windows for up to 10 min after each window ends. Beyond that, events are dropped and demand for that window is undercounted. Record the affected interval.
- `int_rides__lifecycle` reprocesses rides with new events within the 2 h lookback. If the delay was longer, run `dbt build --select int_rides__lifecycle+ --full-refresh`.
