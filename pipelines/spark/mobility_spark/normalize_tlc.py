"""Normalize one month of TLC trips to the canonical bronze schema.

The TLC schema changes over the years: types (passenger_count double → int64),
name casing (airport_fee → Airport_fee) and new columns (cbd_congestion_fee in
2025). This job absorbs that drift and always delivers the same contract:

  {output}/valid/batch_month=YYYY-MM/       accepted rows, deduplicated
  {output}/quarantine/batch_month=YYYY-MM/  rejected rows, with the reasons
  {output}/_metrics/batch_month=YYYY-MM/    batch counts, as JSON

Rerunning the same month overwrites all three outputs, so the job is idempotent.
Compatible with Spark 3.5 (the VPS cluster) and 4.x.

    spark-submit normalize_tlc.py --month 2025-01 \\
        --input data/landing/yellow/yellow_tripdata_2025-01.parquet \\
        --output data/normalized/yellow_trips
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampNTZType,
)

log = logging.getLogger("normalize_tlc")

MONEY = DecimalType(12, 2)
MIN_ZONE_ID, MAX_ZONE_ID = 1, 265


@dataclass(frozen=True)
class CanonicalColumn:
    name: str
    dtype: DataType
    sources: tuple[str, ...]  # accepted source names, compared case-insensitively


CANONICAL_COLUMNS: tuple[CanonicalColumn, ...] = (
    CanonicalColumn("vendor_id", IntegerType(), ("VendorID",)),
    # NYC local time, no time zone: becomes DATETIME in BigQuery
    CanonicalColumn("pickup_datetime", TimestampNTZType(), ("tpep_pickup_datetime",)),
    CanonicalColumn("dropoff_datetime", TimestampNTZType(), ("tpep_dropoff_datetime",)),
    CanonicalColumn("passenger_count", IntegerType(), ("passenger_count",)),
    CanonicalColumn("trip_distance", DoubleType(), ("trip_distance",)),
    CanonicalColumn("rate_code_id", IntegerType(), ("RatecodeID",)),
    CanonicalColumn("store_and_fwd_flag", StringType(), ("store_and_fwd_flag",)),
    CanonicalColumn("pickup_zone_id", IntegerType(), ("PULocationID",)),
    CanonicalColumn("dropoff_zone_id", IntegerType(), ("DOLocationID",)),
    CanonicalColumn("payment_type", IntegerType(), ("payment_type",)),
    CanonicalColumn("fare_amount", MONEY, ("fare_amount",)),
    CanonicalColumn("extra", MONEY, ("extra",)),
    CanonicalColumn("mta_tax", MONEY, ("mta_tax",)),
    CanonicalColumn("tip_amount", MONEY, ("tip_amount",)),
    CanonicalColumn("tolls_amount", MONEY, ("tolls_amount",)),
    CanonicalColumn("improvement_surcharge", MONEY, ("improvement_surcharge",)),
    CanonicalColumn("total_amount", MONEY, ("total_amount",)),
    CanonicalColumn("congestion_surcharge", MONEY, ("congestion_surcharge",)),
    CanonicalColumn("airport_fee", MONEY, ("airport_fee",)),
    CanonicalColumn("cbd_congestion_fee", MONEY, ("cbd_congestion_fee",)),
)
CANONICAL_NAMES = tuple(c.name for c in CANONICAL_COLUMNS)


def normalize(df: DataFrame) -> DataFrame:
    """Rename, cast types and fill missing columns with null."""
    by_lower = {name.lower(): name for name in df.columns}
    selected: list[Column] = []
    for col in CANONICAL_COLUMNS:
        source = next((by_lower[s.lower()] for s in col.sources if s.lower() in by_lower), None)
        expr = F.col(f"`{source}`") if source else F.lit(None)
        selected.append(expr.cast(col.dtype).alias(col.name))

    source_file = F.col("_source_file") if "_source_file" in df.columns else F.lit(None)
    selected.append(source_file.cast("string").alias("_source_file"))
    return df.select(*selected)


def with_trip_id(df: DataFrame) -> DataFrame:
    """Deterministic ID: the same record yields the same ID on every rerun."""
    fields = [F.coalesce(F.col(n).cast("string"), F.lit("∅")) for n in CANONICAL_NAMES]
    return df.withColumn("trip_id", F.sha2(F.concat_ws("|", *fields), 256))


def quality_reasons(month: str) -> Column:
    """List of reasons a row is rejected (empty if the row is valid)."""

    def zone_ok(c: str) -> Column:
        return F.col(c).between(MIN_ZONE_ID, MAX_ZONE_ID)

    rules = {
        # TLC monthly files contain trips from other months, sometimes years earlier
        "pickup_outside_batch_month": F.date_format("pickup_datetime", "yyyy-MM") != month,
        "missing_pickup": F.col("pickup_datetime").isNull(),
        "dropoff_before_pickup": F.col("dropoff_datetime") < F.col("pickup_datetime"),
        "negative_distance": F.col("trip_distance") < 0,
        "invalid_pickup_zone": ~zone_ok("pickup_zone_id") | F.col("pickup_zone_id").isNull(),
        "invalid_dropoff_zone": ~zone_ok("dropoff_zone_id") | F.col("dropoff_zone_id").isNull(),
        "missing_total_amount": F.col("total_amount").isNull(),
    }
    flags = [F.when(cond, F.lit(name)) for name, cond in rules.items()]
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


def split_by_quality(df: DataFrame, month: str) -> tuple[DataFrame, DataFrame]:
    checked = df.withColumn("_dq_reasons", quality_reasons(month))
    valid = checked.filter(F.size("_dq_reasons") == 0).drop("_dq_reasons")
    quarantine = checked.filter(F.size("_dq_reasons") > 0)
    return valid, quarantine


def transform(raw: DataFrame, month: str) -> tuple[DataFrame, DataFrame]:
    """Pipeline completo sem I/O: normaliza, identifica, valida e deduplica."""
    stamped = (
        with_trip_id(normalize(raw))
        .withColumn("_batch_month", F.lit(month))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    valid, quarantine = split_by_quality(stamped, month)
    return valid.dropDuplicates(["trip_id"]), quarantine


def run(spark: SparkSession, input_path: str, output_path: str, month: str) -> dict:
    # _metadata is a hidden column of file sources (Spark >= 3.3)
    raw = spark.read.parquet(input_path).withColumn("_source_file", F.col("_metadata.file_path"))
    valid, quarantine = transform(raw, month)
    valid.cache()

    partition = f"batch_month={month}"
    valid.write.mode("overwrite").parquet(f"{output_path}/valid/{partition}")
    quarantine.write.mode("overwrite").parquet(f"{output_path}/quarantine/{partition}")

    raw_rows = raw.count()
    valid_rows = valid.count()
    quarantine_rows = quarantine.count()
    metrics = {
        "batch_month": month,
        "raw_rows": raw_rows,
        "valid_rows": valid_rows,
        "quarantine_rows": quarantine_rows,
        "duplicate_rows": raw_rows - valid_rows - quarantine_rows,
        "quarantine_by_reason": {
            row["reason"]: row["count"]
            for row in quarantine.select(F.explode("_dq_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        },
    }
    spark.createDataFrame([(json.dumps(metrics),)], ["value"]).coalesce(1).write.mode(
        "overwrite"
    ).text(f"{output_path}/_metrics/{partition}")
    log.info("batch metrics: %s", json.dumps(metrics))
    return metrics


def build_spark(app_name: str = "normalize_tlc") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # INT64 microseconds: BigQuery reads it unambiguously (Spark's default is INT96)
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Overwrite only the month's partition, never the whole table
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Normalize one TLC month")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = build_spark()
    try:
        run(spark, args.input, args.output, args.month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
