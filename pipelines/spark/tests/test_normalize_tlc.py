from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import TimestampNTZType

from mobility_spark.normalize_tlc import CANONICAL_NAMES, normalize, transform


def trip(**overrides):
    row = {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime(2025, 1, 15, 8, 30),
        "tpep_dropoff_datetime": datetime(2025, 1, 15, 8, 50),
        "passenger_count": 1,
        "trip_distance": 3.2,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 132,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 18.4,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 26.4,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
    }
    row.update(overrides)
    return row


def test_schema_drift_between_years_is_absorbed(spark):
    # 2023: passenger_count is double and airport_fee is lowercase; no cbd_congestion_fee
    old = spark.createDataFrame(
        [
            {
                **{k: v for k, v in trip().items() if k != "Airport_fee"},
                "passenger_count": 2.0,
                "airport_fee": 1.75,
            }
        ]
    )
    # 2025: Airport_fee is capitalized and cbd_congestion_fee is a new column
    new = spark.createDataFrame([trip(cbd_congestion_fee=0.75)])

    a, b = normalize(old), normalize(new)

    assert a.schema == b.schema
    assert tuple(a.columns[: len(CANONICAL_NAMES)]) == CANONICAL_NAMES
    assert a.schema["pickup_datetime"].dataType == TimestampNTZType()
    row_old, row_new = a.first(), b.first()
    assert row_old.passenger_count == 2
    assert row_old.airport_fee == Decimal("1.75")
    assert row_old.cbd_congestion_fee is None
    assert row_new.cbd_congestion_fee == Decimal("0.75")


def test_quality_rules_quarantine_with_reasons(spark):
    raw = spark.createDataFrame(
        [
            trip(),
            trip(tpep_pickup_datetime=datetime(2009, 1, 1, 0, 5)),
            trip(tpep_dropoff_datetime=datetime(2025, 1, 15, 8, 0)),
            trip(PULocationID=999, trip_distance=-1.0),
        ]
    )

    valid, quarantine = transform(raw, "2025-01")

    assert valid.count() == 1
    reasons = {tuple(r._dq_reasons) for r in quarantine.collect()}
    assert reasons == {
        ("pickup_outside_batch_month",),
        ("dropoff_before_pickup",),
        ("negative_distance", "invalid_pickup_zone"),
    }


def test_exact_duplicates_are_removed_and_trip_id_is_stable(spark):
    raw = spark.createDataFrame([trip(), trip(), trip(tip_amount=5.0)])

    valid, _ = transform(raw, "2025-01")
    again, _ = transform(raw, "2025-01")

    assert valid.count() == 2
    ids = sorted(r.trip_id for r in valid.collect())
    assert ids == sorted(r.trip_id for r in again.collect())
    assert all(len(i) == 64 for i in ids)
