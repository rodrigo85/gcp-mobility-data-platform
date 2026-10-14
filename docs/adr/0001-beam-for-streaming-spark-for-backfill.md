# ADR 0001: Beam for streaming, Spark for backfill

**Status:** accepted · **Date:** 2026-10-14

## Context

There are two kinds of workload: continuous events (real-time rides) and large monthly files (TLC history, ~3.5 million rows per month and years of backfill).

## Decision

- **Streaming on Apache Beam.** The same code runs on DirectRunner (development, no cost) and on Dataflow (production). Beam's event-time model (watermarks, triggers, allowed lateness, accumulation modes) is the most complete available, and it is what Dataflow runs natively.
- **Backfill on PySpark**, on the Spark 3.5 cluster that already runs on the VPS. Marginal cost is zero, and the volume (GBs per month) does not justify Dataflow or Dataproc.

## Consequences

- Two processing technologies to maintain. Each covers the case where it is strongest, and the contract between them is the bronze schema defined in Terraform.
- The Spark job must stay compatible with the cluster's Spark 3.5. CI pins `pyspark==3.5.5`.
- Running the Beam pipeline on Dataflow costs money: only for demos, with `max_num_workers=1`.

## Alternatives rejected

- **Spark Structured Streaming for everything:** does not run on Dataflow, and its late-data support is less expressive.
- **Beam for everything:** backfill on Dataflow would cost money, and the VPS would sit idle.
