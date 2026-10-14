"""Monthly TLC ingestion: download → Spark → GCS → BigQuery bronze.

On the VPS, download and spark-submit run in the Spark master container (the
scheduler only reads the project); see mobility_common.spark_submit_command.

Each run processes the month of its data interval and is idempotent end to end:
  - the download is skipped if the local file already has the published size;
  - Spark overwrites only the month's partition;
  - the month's GCS prefix is cleared before upload (Spark file names change);
  - the load job uses WRITE_TRUNCATE on the partition decorator (yellow_trips$YYYYMM).

Rerunning a month, or backfilling a year, never duplicates data.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from mobility_common import (
    BRONZE_YELLOW_TRIPS,
    DAG,
    DATA_DIR,
    DEFAULT_ARGS,
    GCP_CONN_ID,
    GCP_PROJECT,
    GCS_BUCKET,
    MOBILITY_HOME,
    SPARK_PYTHON,
    TLC_URL,
    BashOperator,
    in_spark_container,
    spark_submit_command,
    task,
)

MONTH = '{{ data_interval_start.strftime("%Y-%m") }}'  # double quotes survive shlex quoting
PARTITION = '{{ data_interval_start.strftime("%Y%m") }}'
LANDING = DATA_DIR / "landing" / "yellow"
NORMALIZED = DATA_DIR / "normalized" / "yellow_trips"
# Single source of truth for the bronze contract, shared with Terraform
BRONZE_SCHEMA = json.loads(
    (MOBILITY_HOME / "infra/schemas/bronze_yellow_trips.json").read_text(encoding="utf-8")
)
GCS_PREFIX = f"raw/yellow_trips/batch_month={MONTH}/"

with DAG(
    dag_id="tlc_monthly_ingestion",
    description="TLC yellow trips: download, Spark normalization and bronze load",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    is_paused_upon_creation=True,  # backfill only after terraform apply and the Variable
    max_active_runs=2,
    dagrun_timeout=timedelta(hours=3),
    default_args=DEFAULT_ARGS,
    tags=["mobility", "batch", "bronze"],
) as dag:

    @task.sensor(poke_interval=6 * 3600, timeout=75 * 24 * 3600, mode="reschedule")
    def wait_for_tlc_file(month: str) -> bool:
        """The TLC publishes each month ~2 months late; the sensor waits without holding a slot."""
        import requests

        return requests.head(TLC_URL.format(month=month), timeout=30).status_code == 200

    download = BashOperator(
        task_id="download_tlc_month",
        bash_command=in_spark_container(
            SPARK_PYTHON,
            str(MOBILITY_HOME / "pipelines/spark/mobility_spark/download_tlc.py"),
            "--month",
            MONTH,
            "--dest",
            str(LANDING),
        ),
        execution_timeout=timedelta(minutes=20),
    )

    normalize = BashOperator(
        task_id="normalize_with_spark",
        bash_command=spark_submit_command(
            application=MOBILITY_HOME / "pipelines/spark/mobility_spark/normalize_tlc.py",
            app_args=[
                "--month",
                MONTH,
                "--input",
                str(LANDING / f"yellow_tripdata_{MONTH}.parquet"),
                "--output",
                str(NORMALIZED),
            ],
            name=f"normalize_tlc_{PARTITION}",
            # A TLC month has ~3.5 million rows: no need for the cluster's 20g default
            conf={
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2",
                "spark.cores.max": "4",
                "spark.sql.shuffle.partitions": "16",
            },
        ),
        execution_timeout=timedelta(minutes=45),
    )

    clear_gcs_prefix = GCSDeleteObjectsOperator(
        task_id="clear_gcs_month_prefix",
        bucket_name=GCS_BUCKET,
        prefix=GCS_PREFIX,
        gcp_conn_id=GCP_CONN_ID,
    )

    upload = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=str(NORMALIZED / f"valid/batch_month={MONTH}/*.parquet"),
        dst=GCS_PREFIX,
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_partition = BigQueryInsertJobOperator(
        task_id="load_bronze_partition",
        gcp_conn_id=GCP_CONN_ID,
        location="us-central1",
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_PREFIX}*.parquet"],
                "sourceFormat": "PARQUET",
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": "bronze",
                    # Partition decorator: atomically replaces only that month
                    "tableId": f"yellow_trips${PARTITION}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
                # Explicit schema: without it BigQuery infers TIMESTAMP for Parquet
                # timestamps without a time zone and rejects the DATETIME columns
                "schema": {"fields": BRONZE_SCHEMA},
                "parquetOptions": {"enableListInference": True},
            }
        },
        outlets=[BRONZE_YELLOW_TRIPS],
    )

    @task
    def reconcile_counts(month: str, project: str) -> dict:
        """Rows in BigQuery must match the valid rows Spark counted."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        metrics_dir = NORMALIZED / "_metrics" / f"batch_month={month}"
        metrics = json.loads(next(metrics_dir.glob("part-*")).read_text())

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location="us-central1", use_legacy_sql=False)
        loaded = hook.get_records(
            f"select count(*) from `{project}.bronze.yellow_trips` where _batch_month = '{month}'"
        )[0][0]
        if loaded != metrics["valid_rows"]:
            raise ValueError(f"{month}: bigquery={loaded} spark={metrics['valid_rows']}")
        return {**metrics, "bigquery_rows": loaded}

    (
        wait_for_tlc_file(MONTH)
        >> download
        >> normalize
        >> clear_gcs_prefix
        >> upload
        >> load_partition
        >> reconcile_counts(MONTH, GCP_PROJECT)
    )
