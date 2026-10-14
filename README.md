# GCP Mobility Data Platform

An urban mobility data platform on Google Cloud, modeled on what Uber and Lyft run in production. It is built on the public New York City taxi data ([NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)) and a real-time ride simulator.

> **Status:** skeleton. The structure, contracts and extension points are in place, and `TODO`s mark what is left to implement. The full plan is in [docs/design-doc.md](docs/design-doc.md).

## Architecture

```
                       ┌────────────── Airflow 2.10 (self-hosted VPS) ──────────────┐
                       │ idempotent DAGs · backfill · sensors · asset-driven        │
                       │ scheduling · dbt (Cosmos-ready)                             │
                       └─────────────────────────────────────────────────────────────┘
 TLC Parquet (monthly) ─► PySpark 3.5 (schema normalization, quarantine) ─► GCS raw/
                                                                     │ load job (free),
                                                                     │ WRITE_TRUNCATE on the partition
 Ride simulator ─► Pub/Sub ─► Apache Beam (Dataflow) ────────────────►▼
   (late, duplicate,          windows · watermark ·              BigQuery bronze
    malformed events)         dedup · DLQ                              │ dbt
                                                                       ▼
                                silver: staging + intermediate (incremental, SCD2)
                                                                       ▼
                                gold: contracted facts/dimensions + metrics
                                                                       ▼
                         Looker Studio · dbt tests (quality) · Marquez (lineage, planned)
```

## Runtime

Orchestration and Spark run on a self-hosted VPS that already hosts Airflow 2.10 and a Spark 3.5 cluster, so they cost nothing (see [docs/deploy-vps.md](docs/deploy-vps.md) and [ADR 0002](docs/adr/0002-local-airflow-instead-of-composer.md)). The Airflow scheduler mounts the project read-only. Spark jobs and TLC downloads therefore run inside the Spark master container via `docker exec`, and the scheduler only reads what they produce. Python 3.11 is used everywhere, matching the VPS containers. The DAGs are written to be Airflow 3-compatible.

## What each part demonstrates

| Area | Where | Highlights |
|---|---|---|
| Streaming | `pipelines/beam/` | Event time, fixed windows, late data with `allowed_lateness`, deduplication by `event_id`, typed DLQ, the same code on DirectRunner and Dataflow |
| Batch / backfill | `pipelines/spark/` | Normalizing *schema drift* across TLC years, deterministic IDs, quarantine with a reason, per-batch metrics |
| Transformation | `dbt/` | Incremental models (`insert_overwrite` and `merge`), SCD2 snapshot, model contracts, unit tests, integrity tests, cost guardrail in dev |
| Orchestration | `orchestration/airflow/` | Sensors, idempotent per-partition loads, data-aware scheduling (assets), dbt with a Cosmos path and a plain `dbt build` fallback |
| Infrastructure as code | `infra/terraform/` | Buckets, datasets, Pub/Sub with dead-lettering, least-privilege service accounts, Workload Identity Federation for GitHub, policy tags, budget and quota |
| DataOps | `.github/workflows/` | Lint (Ruff, SQLFluff), tests, `terraform validate`, *slim CI* `dbt build` in an ephemeral dataset per PR |
| Performance and cost | `docs/query-optimization.md`, `dbt/analyses/` | Before/after measured through `INFORMATION_SCHEMA.JOBS` |

## Validated so far

| What | Where | Result |
|---|---|---|
| Spark normalization of TLC 2025-01 | VPS Spark 3.5 cluster | 3,475,226 raw rows → 3,475,080 valid, 146 quarantined (124 `dropoff_before_pickup`, 22 `pickup_outside_batch_month`), 0 duplicates, ~90 s |
| Parquet output vs. BigQuery contract | Spark output schema | Types match: `DATETIME` via INT64 with `isAdjustedToUTC=false`, `NUMERIC(12,2)` for money |
| Streaming end to end | Pub/Sub emulator on the VPS | 1,117 messages published (5% duplicates, 5% late, 2% malformed) → 1,047 unique events, 21 to the DLQ, 275 window rows, including a LATE pane that corrected an already-emitted window |
| Test suites | Local and VPS | Simulator 4, Beam 19, Spark 3 (on the VPS cluster), Airflow DAG integrity 7 (on the VPS Airflow 2.10.4); `dbt parse` clean; `terraform validate` clean |

## Layout

```
.
├── docs/                   design doc, ADRs, SLOs, runbooks, optimization report
├── infra/terraform/        all GCP infrastructure
├── simulator/              ride event generator → Pub/Sub
├── pipelines/
│   ├── beam/               streaming: Pub/Sub → BigQuery bronze
│   └── spark/              batch: TLC Parquet → normalized Parquet
├── dbt/                    silver and gold in BigQuery
├── orchestration/airflow/  DAGs and integrity tests
└── .github/workflows/      CI, dbt per PR, Terraform
```

## Cost

The project is designed to fit in the GCP free tier. Details are in [ADR 0003](docs/adr/0003-bigquery-cost-guardrails.md).

| Resource | Strategy |
|---|---|
| BigQuery | Free tier (1 TiB of queries and 10 GiB of storage per month). Load jobs are free. `maximum_bytes_billed` on every dbt profile and an optional daily query quota on the project |
| Cloud Storage | 5 GB free in `us-central1`, with a lifecycle rule that deletes old `raw/` data |
| Pub/Sub | 10 GiB/month free. The local emulator is used for development |
| Dataflow | No free tier. Develop on DirectRunner; occasional Dataflow runs are paid with trial credits |
| Airflow and Spark | Self-hosted VPS. Cloud Composer is not used ([ADR 0002](docs/adr/0002-local-airflow-instead-of-composer.md)) |
| Budget | US$1 alert via Terraform |

## Getting started

Prerequisites: Python 3.11 (the same version as the VPS Airflow and Spark), Java 17 (for Spark), Terraform ≥ 1.9, an authenticated `gcloud`, and a GCP project with billing enabled (the free tier requires a billing account).

Create a `.env` file at the repository root (it is git-ignored) with:

```bash
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1               # everything in us-central1 for the GCS free tier
GCS_BUCKET=your-project-id-mobility-lake
PUBSUB_TOPIC=ride-events
PUBSUB_SUBSCRIPTION=ride-events-beam
# PUBSUB_EMULATOR_HOST=localhost:8085 # uncomment to use the emulator
DBT_TARGET=dev
DBT_DEV_DATASET_PREFIX=dev_yourname
DBT_MAXIMUM_BYTES_BILLED=10000000000 # 10 GB cap per query
LOCAL_DATA_DIR=./data
```

Then:

```bash
make venvs                           # one virtual environment per component
make tf-init tf-plan                 # review the plan before applying
make tf-apply
make test                            # unit tests for every component
make dbt-deps dbt-seed dbt-build     # silver and gold in your dev dataset
```

To run streaming locally against the Pub/Sub emulator:

```bash
gcloud beta emulators pubsub start --project=local-project   # in another terminal
make emulator-topics   # creates the topic and subscription in the emulator
make simulate          # publishes events
make beam-local        # consumes, aggregates and logs
```

On the VPS (run from a shell with SSH access, e.g. WSL on Windows; the host is the SSH alias in `VPS`, default `vpsmouts`):

```bash
make vps-deploy                            # rsync the repository
make vps-setup                             # isolated venvs (dbt, Beam) and dbt deps
make vps-dag-test                          # DAG integrity tests on the VPS Airflow
make vps-download MONTH=2025-01            # download one TLC month
make vps-spark-normalize MONTH=2025-01     # spark-submit on the cluster
```

## Documentation

- [Design doc](docs/design-doc.md)
- [ADRs](docs/adr/)
- [Data SLOs](docs/data-slos.md)
- [Query optimization report](docs/query-optimization.md)
- [VPS deployment](docs/deploy-vps.md)
- [Runbook: streaming backlog](docs/runbooks/streaming-backlog.md)

## License

MIT
