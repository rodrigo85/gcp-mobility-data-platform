# ADR 0003: Cost guardrails for BigQuery and GCP

**Status:** accepted · **Date:** 2026-10-14

## Context

The project has to fit in the free tier: 1 TiB of queries and 10 GiB of storage per month in BigQuery, 5 GB in Cloud Storage and 10 GiB in Pub/Sub. A single unfiltered `select *` on a large table, or a CI loop, could blow through that.

## Decision

Layered guardrails, from the closest to the query to the broadest:

| Layer | Where | Effect |
|---|---|---|
| `maximum_bytes_billed` | `dbt/profiles.yml` (10 GB in dev, 5 GB in CI, 50 GB in prod) | The query fails before running if it would exceed the cap |
| `require_partition_filter` | `fct_trips` | Queries without a partition filter are rejected |
| Dev window | `limit_dev_data` macro | Dev and CI read only one month of bronze |
| Daily query quota | `cost_guardrails.tf` (optional) | Blocks the whole project after N GiB per day |
| Budget | `cost_guardrails.tf` | Email alerts at 50%, 90% and 100% of US$1 |

Plus architecture choices that avoid cost:

- **Load jobs instead of streaming inserts.** Load jobs are free. Beam uses `FILE_LOADS` with `triggering_frequency=300`, trading 5 minutes of latency for zero ingestion cost.
- **Soft delete disabled on the bucket.** The 7-day default bills storage for deleted objects, and the data in `raw/` is reproducible.
- **Lifecycle on `raw/`** of 30 days: once loaded, the Parquet in GCS is redundant.
- **Physical storage billing** (`storage_billing_model = PHYSICAL`) on the datasets: billed on compressed data.
- **Dataflow on demand only**, with `max_num_workers=1` and `e2-small`.

## Consequences

- Minimum bronze latency for streaming is about 5 minutes.
- Developers need to adjust `dev_window_start/end` to work with other months.
- The daily quota's unit must be checked in the console before it is enabled (`enable_query_quota`).

## Amendment (2026-10-14): hard cap of BRL 100

A budget alone only sends e-mails. The project now has a hard cap:

| Layer | Setting | Effect |
|---|---|---|
| Budget | BRL 100/month, gross of credits (`EXCLUDE_ALL_CREDITS`) | E-mails at 25%, 50%, 75%, 90% and 100% |
| Kill switch | `infra/functions/budget_kill_switch`, triggered via Pub/Sub at 70% (BRL 70) | Unlinks the billing account: every paid service stops |
| Query quota | 30,720 MiB/day (unit verified as `MiBy` in the Service Usage API) | BigQuery rejects queries beyond ~30 GiB/day, keeping the month under the 1 TiB free tier |

The kill switch fires at 70% because Cloud Billing reports cost with a delay of several hours; the margin absorbs what is spent in that window. Re-enabling is manual: re-link the billing account in the console. Stored data is kept, but Google may delete resources if billing stays disabled for long.
