# Design doc: GCP Mobility Data Platform

| | |
|---|---|
| Author | Rodrigo Andreatta |
| Status | In progress (skeleton) |
| Last reviewed | 2026-10-14 |

## 1. Context

A mobility company needs two views of the same business:

- **A trustworthy history** for planning, pricing and regulatory reporting: years of rides, with numbers that reconcile to the cent.
- **Real-time operations** for dispatch and surge pricing: per-zone demand minute by minute, tolerant of late and duplicate events.

This project implements both views on real data (NYC TLC, ~3.5 million rides per month) and an event simulator, on GCP, at a monthly cost close to zero.

## 2. Goals

1. Ingest the TLC history idempotently: any month can be reprocessed without duplicating data.
2. Process streaming events with event-time semantics: windows, late data, deduplication and a DLQ.
3. Model silver and gold in dbt with contracts, tests and dimension history (SCD2).
4. Make cost and performance observable and bounded by design.
5. Put every change through CI: tests, lint, infrastructure validation and a dbt build in an ephemeral environment.

## 3. Non-goals

- Serving data with sub-minute latency. Bronze is loaded every 5 minutes through free load jobs (see ADR 0003).
- High availability for the orchestrator. Airflow runs on a single VPS (see ADR 0002).
- Machine learning. The gold layer is designed to feed models but does not include any.

## 4. Architecture

```
TLC (monthly Parquet) ─► Airflow: sensor + download ─► Spark (VPS) ─► GCS raw/ ─► BigQuery bronze
Simulator ─► Pub/Sub ─► Beam (DirectRunner/Dataflow) ─────────────────────────────► BigQuery bronze
BigQuery bronze ─► dbt (silver: staging/intermediate) ─► dbt (gold: facts, dimensions, metrics)
```

| Layer | Owner | Format | Guarantee |
|---|---|---|---|
| bronze | Spark, Beam | Tables with a fixed schema defined in Terraform | Column contract; idempotent per-partition load |
| silver | dbt | Views and incremental models | Uniqueness, relationships, types |
| gold | dbt | Partitioned and clustered tables | Model contract, mandatory partition filter |

### 4.1 Batch

1. `tlc_monthly_ingestion` waits for the month's file with a sensor in reschedule mode, which does not hold a worker slot.
2. The download is atomic (`.part` file, then rename) and is skipped if the size already matches.
3. Spark normalizes schema drift across years, generates a deterministic `trip_id`, splits quarantined rows out with a reason, and deduplicates.
4. The month's GCS prefix is cleared, the new files are uploaded, and the load job writes with `WRITE_TRUNCATE` into `yellow_trips$YYYYMM`.
5. A reconciliation step compares the row count in BigQuery with the valid rows counted by Spark.
6. The bronze asset update triggers `dbt_batch`.

### 4.2 Streaming

1. The simulator publishes events with `event_ts` and `event_id` as attributes.
2. Beam reads with `timestamp_attribute=event_ts` (event time), validates against the contract and routes invalid messages to the DLQ.
3. Deduplication by `event_id`, with state kept for 1 hour of event time.
4. Fixed 1-minute windows, fired at the watermark, with 10 minutes of `allowed_lateness` in ACCUMULATING mode. dbt keeps only the latest pane per window.
5. Writes use FILE_LOADS every 5 minutes.

## 5. Alternatives considered

| Decision | Choice | Alternative | Why |
|---|---|---|---|
| Streaming | Beam | Spark Structured Streaming | Portability to Dataflow and a more expressive event-time model (ADR 0001) |
| Backfill | Spark on the VPS | Dataflow batch | Zero cost on a cluster that already exists (ADR 0001) |
| Orchestration | Airflow on the VPS | Cloud Composer | Composer costs hundreds of dollars per month, even when idle (ADR 0002) |
| BigQuery writes | Load jobs | Streaming inserts / Storage Write API | Load jobs are free (ADR 0003) |
| Bronze updates | WRITE_TRUNCATE per partition | MERGE | Atomic, idempotent and no scan cost (ADR 0004) |
| Gold incrementals | `insert_overwrite` | `merge` | Cheaper for append-only partitioned facts |

## 6. Quality and reliability

- Freshness and completeness SLOs in [data-slos.md](data-slos.md), measured by `dbt source freshness` and the row-count reconciliation.
- Tests at four levels: unit (pytest, dbt unit tests), contract (Terraform and dbt contracts), integrity (dbt generic and singular tests) and reconciliation (Airflow).
- Runbooks for known incidents in [runbooks/](runbooks/).

## 7. Security and governance

- One service account per role, with least privilege (`infra/terraform/iam.tf`).
- CI uses no JSON key: it exchanges GitHub's OIDC token for short-lived credentials (Workload Identity Federation).
- `rider_id` carries a policy tag: only the pipeline and the orchestrator can read that column.
- Bronze has `deletion_protection`. The DLQ expires after 30 days.

## 8. Cost

See ADR 0003. Target: under US$1 per month, excluding occasional Dataflow runs.

## 9. Delivery plan

| Phase | Deliverable | Status |
|---|---|---|
| 1 | Terraform, Spark and batch ingestion up to bronze | Skeleton ready |
| 2 | Simulator and Beam up to bronze | Skeleton ready |
| 3 | dbt silver/gold, snapshots, contracts and unit tests | Skeleton ready |
| 4 | Airflow on the VPS and orchestrated dbt | Skeleton ready |
| 5 | Full CI/CD, with slim CI and an ephemeral dataset | Skeleton ready |
| 6 | Optimization report with real numbers, dashboard and lineage (OpenLineage) | To do |

## 10. Open questions

- Upgrade the VPS Airflow to version 3 (the DAGs are already compatible).
- Add lineage: the OpenLineage provider is already installed on the VPS Airflow; a backend (Marquez) is still missing.
- Elementary for test result history, once a release supports dbt 1.12.
