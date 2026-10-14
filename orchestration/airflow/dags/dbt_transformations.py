"""Airflow DAGs for the dbt transformations: batch (data-triggered) and streaming.

(Airflow's safe mode only parses files that mention "airflow" and "dag"; this
file imports everything from mobility_common, hence the explicit mention.)

dbt_batch has no schedule: it runs when tlc_monthly_ingestion updates bronze
(asset BRONZE_YELLOW_TRIPS). dbt_streaming runs hourly over what Beam loaded.

With astronomer-cosmos installed, each model becomes a task (per-model retries
and observability). Without it, the DAG falls back to a single `dbt build`.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from mobility_common import (
    BRONZE_YELLOW_TRIPS,
    DAG,
    DBT_DIR,
    DBT_EXECUTABLE,
    DEFAULT_ARGS,
    GOLD_FCT_TRIPS,
    BashOperator,
    dbt_env,
)

try:
    from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
    from cosmos.constants import TestBehavior

    HAS_COSMOS = DBT_EXECUTABLE.exists()
except ImportError:
    HAS_COSMOS = False


def dbt_steps(selector: str, freshness_selector: str, outlets: list | None = None) -> None:
    """Source freshness, then build (seed + snapshot + run + test) for the selector."""
    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT_EXECUTABLE} source freshness --select {freshness_selector}",
        cwd=str(DBT_DIR),
        env=dbt_env(),
    )

    if HAS_COSMOS:
        build = DbtTaskGroup(
            group_id="dbt",
            project_config=ProjectConfig(DBT_DIR, env_vars=dbt_env()),
            profile_config=ProfileConfig(
                profile_name="mobility",
                target_name="prod",
                profiles_yml_filepath=DBT_DIR / "profiles.yml",
            ),
            execution_config=ExecutionConfig(dbt_executable_path=str(DBT_EXECUTABLE)),
            render_config=RenderConfig(select=[selector], test_behavior=TestBehavior.AFTER_EACH),
            operator_args={"install_deps": False},
        )
    else:
        build = BashOperator(
            task_id="dbt_build",
            # dbt_packages comes from the deploy (make vps-setup): the project is read-only here
            bash_command=f"{DBT_EXECUTABLE} build --select {selector}",
            cwd=str(DBT_DIR),
            env=dbt_env(),
            outlets=outlets or [],
        )

    freshness >> build


with DAG(
    dag_id="dbt_batch",
    description="silver/gold for TLC history; triggered by the bronze load",
    schedule=[BRONZE_YELLOW_TRIPS],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=DEFAULT_ARGS,
    tags=["mobility", "dbt", "batch"],
):
    dbt_steps("tag:batch", "source:tlc", outlets=[GOLD_FCT_TRIPS])


with DAG(
    dag_id="dbt_streaming",
    description="Real-time rides: lifecycle, fct_rides and KPIs",
    schedule="15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["mobility", "dbt", "streaming"],
):
    dbt_steps("tag:streaming", "source:rides")
