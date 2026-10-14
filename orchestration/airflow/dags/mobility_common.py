"""Configuration and compatibility shared by the platform's DAGs.

The DAGs run on the VPS's Airflow 2.10 and are written to migrate to
Airflow 3 without a rewrite: the imports below resolve to the right API in
each version.

On the VPS, the scheduler mounts the project read-only. Anything that writes
to disk (download, spark-submit) runs in the Spark master container via
`docker exec`; the scheduler only reads the produced files. On a cloud, the
equivalent would be swapping spark_submit_command for a DataprocSubmitJobOperator.

Runtime configuration: Airflow Variable `mobility_config` (JSON)
    {
      "gcp_project": "my-project",
      "gcs_bucket": "my-project-mobility-lake",
      "gcp_keyfile": "/pipeline/gcp-mobility-data-platform/.secrets/orchestrator.json"
    }
Templates read the Variable only at run time, never at DAG parse time.
"""

from __future__ import annotations

import os
import shlex
from datetime import timedelta
from pathlib import Path

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, Asset, task
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.datasets import Dataset as Asset
    from airflow.decorators import task
    from airflow.operators.bash import BashOperator

__all__ = ["DAG", "Asset", "BashOperator", "task"]

# /pipeline is the same directory in the scheduler (read-only), the Spark master
# and the worker (read-write), so they all see the same files.
MOBILITY_HOME = Path(os.environ.get("MOBILITY_HOME", "/pipeline/gcp-mobility-data-platform"))
DATA_DIR = MOBILITY_HOME / "data"
DBT_DIR = MOBILITY_HOME / "dbt"
DBT_EXECUTABLE = MOBILITY_HOME / ".venvs" / "dbt" / "bin" / "dbt"
SPARK_CONTAINER = os.environ.get("MOBILITY_SPARK_CONTAINER", "da-spark-master")
SPARK_MASTER_URL = "spark://spark-master:7077"
SPARK_PYTHON = "/usr/local/bin/python3.11"

CONFIG = "var.json.mobility_config"
GCP_PROJECT = "{{ " + CONFIG + ".gcp_project }}"
GCS_BUCKET = "{{ " + CONFIG + ".gcs_bucket }}"
GCP_CONN_ID = os.environ.get("MOBILITY_GCP_CONN_ID", "google_cloud_default")

# Logical assets: link the producer DAG to its consumers (data-aware scheduling)
BRONZE_YELLOW_TRIPS = Asset("mobility://bronze/yellow_trips")
GOLD_FCT_TRIPS = Asset("mobility://gold/fct_trips")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def in_spark_container(*args: str) -> str:
    """Bash command that runs `args` in the Spark master container."""
    return "docker exec " + shlex.join([SPARK_CONTAINER, *args])


def spark_submit_command(
    application: Path, app_args: list[str], name: str, conf: dict[str, str]
) -> str:
    conf_args = [item for key, value in conf.items() for item in ("--conf", f"{key}={value}")]
    return in_spark_container(
        "/opt/spark/bin/spark-submit",
        "--master",
        SPARK_MASTER_URL,
        "--name",
        name,
        *conf_args,
        str(application),
        *app_args,
    )


def dbt_env() -> dict[str, str]:
    """dbt environment in prod. Templated values: resolved at run time.

    The project is read-only in the scheduler: target and logs go to /tmp,
    and packages (dbt_packages) are installed at deploy time, not at run time.
    """
    return {
        "DBT_TARGET": "prod",
        "DBT_PROFILES_DIR": str(DBT_DIR),
        "DBT_TARGET_PATH": "/tmp/mobility-dbt/target",
        "DBT_LOG_PATH": "/tmp/mobility-dbt/logs",
        "GCP_PROJECT_ID": GCP_PROJECT,
        "GCP_REGION": "us-central1",
        "GOOGLE_APPLICATION_CREDENTIALS": "{{ " + CONFIG + ".gcp_keyfile }}",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
    }
