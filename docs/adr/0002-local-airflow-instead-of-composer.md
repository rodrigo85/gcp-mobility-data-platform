# ADR 0002: Airflow on a self-hosted VPS instead of Cloud Composer

**Status:** accepted · **Date:** 2026-10-14

## Context

Cloud Composer is GCP's managed Airflow. The smallest environment costs hundreds of dollars per month, even with no DAGs running. A VPS with Airflow 2.10 and a Spark 3.5 cluster already exists and is used by other projects.

## Decision

Orchestrate with the VPS Airflow. The repository is deployed to `/opt/spark-cluster/projects/gcp-mobility-data-platform`, visible as `/pipeline/gcp-mobility-data-platform` in the scheduler, the Spark master and the worker. Details are in [deploy-vps.md](../deploy-vps.md).

The scheduler mounts the project read-only. Anything that writes to disk (TLC downloads, `spark-submit`) runs inside the Spark master container via `docker exec`; the scheduler only reads the results.

The DAGs use a compatibility layer (`mobility_common.py`) that resolves Airflow 2 and Airflow 3 imports, so migrating to Airflow 3 requires no rewrite.

## Consequences

- Zero orchestration cost.
- No high availability: if the VPS goes down, runs are delayed. Since every DAG is idempotent and catchup is enabled where it matters, recovery is just bringing it back up.
- The VPS is shared, so everything in this project is isolated: virtual environments live inside the project folder, and the Airflow and Spark Python environments are never modified. DAGs are created paused.
- GCP access from the VPS uses the `mobility-orchestrator` service account key, stored in `.secrets/` (outside git, mode 600).

## When to revisit

If the project needs an orchestration SLA, move to Composer or to Airflow 3 on Kubernetes. The DAGs do not change.
