# VPS deployment

Airflow and Spark run on a self-hosted VPS that also hosts other projects (see [ADR 0002](adr/0002-local-airflow-instead-of-composer.md)). The address and key live in the operator's `~/.ssh/config`; the repository only knows the SSH alias (the `VPS` variable, default `vpsmouts`).

## Topology

| Container | Role in this project | Project path | Mount |
|---|---|---|---|
| `da-airflow-scheduler` (Airflow 2.10, Python 3.11, LocalExecutor) | Runs the DAGs; uploads to GCS, reconciles, runs dbt | `/pipeline/gcp-mobility-data-platform` | **read-only** |
| `da-spark-master` (Spark 3.5, Python 3.11) | Cluster master; runs downloads and `spark-submit` on behalf of the DAGs (`docker exec`) | same | read-write |
| `docker-spark-worker-1` | Executors | same | read-write |

On the host, everything lives in `/opt/spark-cluster/projects/gcp-mobility-data-platform`, mounted as `/pipeline/...` in all three containers, so the driver and executors read and write the same files under `data/`.

The scheduler mounts the project read-only. Anything that writes to disk therefore runs in the Spark master: the DAGs call `docker exec da-spark-master ...` (the scheduler has the Docker CLI and the socket mounted) for the TLC download and for `spark-submit`. The scheduler only reads the outputs (upload to GCS, row-count reconciliation).

## Isolation

The VPS is shared, so this project **does not modify** the Airflow or Spark Python environments:

| Tool | Where | Created by |
|---|---|---|
| dbt (Python 3.11) | `.venvs/dbt` | `make vps-setup` |
| Beam and simulator (host Python 3.12) | `.venvs/beam` | `make vps-setup` |
| gcloud CLI + Pub/Sub emulator + OpenJDK 17 | Host (apt) | Installed once (done) |

The dbt venv is created from the Spark master, because the scheduler cannot write to the project. Both containers ship the same Python 3.11 under `/usr/local`, so the venv also runs in the scheduler. Since the project is read-only there, dbt writes `target/` and `logs/` to `/tmp/mobility-dbt` (`DBT_TARGET_PATH`, `DBT_LOG_PATH`), and `dbt deps` runs at deploy time (in `make vps-setup`), never at run time.

The `.airflowignore` at the repository root makes Airflow parse only `orchestration/airflow/dags`. Every DAG is created paused (`is_paused_upon_creation=True`), so publishing files never starts work on the shared cluster.

## Step by step

The `make vps-*` targets run from a machine with SSH access to the VPS (on Windows, inside WSL).

```bash
make vps-deploy      # rsync the repository (without .venvs, data, .secrets, .git)
make vps-setup       # isolated virtual environments + dbt deps
make vps-dag-test    # DAG integrity tests on the VPS Airflow
make vps-download MONTH=2025-01          # download one TLC month (in the Spark master)
make vps-spark-normalize MONTH=2025-01   # spark-submit on the cluster
```

### GCP credentials

1. Create a key for the `mobility-orchestrator` service account (created by Terraform).
2. Copy it to `/opt/spark-cluster/projects/gcp-mobility-data-platform/.secrets/orchestrator.json` with `chmod 600`.
3. In Airflow, create the `mobility_gcp` connection (Google Cloud type; a dedicated ID so the shared Airflow keeps `google_cloud_default` free) pointing to that file.
4. Create the `mobility_config` Variable:

```json
{
  "gcp_project": "your-project-id",
  "gcs_bucket": "your-project-id-mobility-lake",
  "gcp_keyfile": "/pipeline/gcp-mobility-data-platform/.secrets/orchestrator.json"
}
```

The DAGs are created paused. Unpause `tlc_monthly_ingestion` only after `make tf-apply`.

## Streaming on the VPS

The Pub/Sub emulator runs on the host at no cost:

```bash
gcloud beta emulators pubsub start --project=local-project --host-port=127.0.0.1:8085
export PUBSUB_EMULATOR_HOST=127.0.0.1:8085
```

With the emulator up, the simulator and the Beam pipeline run from `.venvs/beam` (see `make simulate` and `make beam-local`). Create the topic and subscription in the emulator first with `make emulator-topics`.

### Against real Pub/Sub and BigQuery

The same pipeline runs on the VPS host with the DirectRunner and writes to BigQuery through free load jobs, so a bounded test costs nothing. It uses the `mobility-pipelines` service account (publish, subscribe, write to bronze), never the orchestrator's:

```bash
# On the VPS, once: the key stays in .secrets/ (git-ignored, excluded from rsync)
gcloud iam service-accounts keys create .secrets/pipelines.json \
  --iam-account mobility-pipelines@<PROJECT_ID>.iam.gserviceaccount.com
chmod 600 .secrets/pipelines.json

# From your machine (WSL on Windows)
make vps-stream GCP_PROJECT_ID=<PROJECT_ID>              # ~6 min: Beam + 3 min of simulated rides
make vps-dbt-build GCP_PROJECT_ID=<PROJECT_ID> DBT_SELECT=tag:streaming
make vps-dag-run DAG=dbt_streaming DATE=$(date -u +%Y-%m-%dT%H:00:00+00:00)
```

`make vps-stream` runs [scripts/stream_to_bigquery.sh](../scripts/stream_to_bigquery.sh): Beam starts first, the simulator publishes for `SIM_SECONDS`, and `timeout` stops the unbounded pipeline after two more trigger periods so the last panes are loaded. It ends by printing the row counts of the three bronze streaming tables.

`dbt_streaming` starts with a source freshness check on `bronze.ride_events` (warn after 15 minutes, error after 60). Run the DAG within the hour after the stream, otherwise it stops at that gate on purpose: stale real-time data must not refresh the KPIs silently.
