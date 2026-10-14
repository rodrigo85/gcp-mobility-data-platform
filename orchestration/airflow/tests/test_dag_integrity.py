"""Integrity tests: every DAG imports cleanly and follows the conventions.

Runs on the same Airflow as the VPS (2.10) and in CI:
    pytest orchestration/airflow/tests
"""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED_DAGS = {"tlc_monthly_ingestion", "dbt_batch", "dbt_streaming"}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_are_present(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED_DAGS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_conventions(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert "mobility" in dag.tags
    assert dag.dagrun_timeout is not None, "every DAG needs a timeout"
    assert dag.max_active_runs is not None
    # The VPS is shared: nothing starts running just because a file was deployed
    assert dag.is_paused_upon_creation is True
    for t in dag.tasks:
        assert t.owner == "data-platform"
        assert t.retries >= 1, f"{t.task_id} has no retries"


def test_ingestion_is_a_linear_chain_ending_in_reconciliation(dagbag):
    dag = dagbag.get_dag("tlc_monthly_ingestion")
    order = [t.task_id for t in dag.topological_sort()]
    assert order[0] == "wait_for_tlc_file"
    assert order[-1] == "reconcile_counts"
    assert order.index("clear_gcs_month_prefix") < order.index("upload_to_gcs")


def test_dbt_batch_is_triggered_by_bronze_asset(dagbag):
    dag = dagbag.get_dag("dbt_batch")
    assert dag.timetable.summary.lower() in {"dataset", "asset", "triggered by assets"}
