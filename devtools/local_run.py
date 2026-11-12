"""Runs the Spark side of Seatline Lakehouse on a file of captured changes, without
Kafka, Iceberg or Airflow. Same functions as the production jobs; silver is
computed by folding the changes instead of by MERGE.

    python -m devtools.local_run --events events.jsonl --out dashboard/data.json
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import cdc, export, marts, quality, scd2, silver
from lakehouse.contracts import CONTRACTS

RECORD = "topic STRING, partition INT, offset BIGINT, timestamp BIGINT, key STRING, value STRING"


def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-local")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--events", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--parquet", type=Path, help="also write bronze/silver/history/gold here")
    args = p.parse_args()

    s = spark()
    started = time.monotonic()
    records = s.read.schema(RECORD).json(str(args.events)).withColumn("timestamp", F.timestamp_millis("timestamp"))
    bronze = cdc.to_bronze(records).cache()
    changes = {t: cdc.change_rows(bronze, c).cache() for t, c in CONTRACTS.items()}
    current = {t: silver.fold(changes[t], c).cache() for t, c in CONTRACTS.items()}
    history = {t: scd2.history(changes[t], c).cache() for t, c in CONTRACTS.items()}

    # Captured data is replayed long after it happened, so freshness would
    # always fail here; it is checked on the live pipeline only.
    checks = quality.run_all(bronze, current, CONTRACTS, max_lag_minutes=None)
    gold = marts.build(current, history, changes)

    export.write(
        args.out,
        marts=gold,
        checks=checks,
        extra={
            "summary": export.summary(bronze, changes, history),
            "fare_story": export.fare_story(history["fares"], current["flights"]),
        },
    )
    if args.parquet:
        bronze.write.mode("overwrite").parquet(str(args.parquet / "bronze"))
        for t in CONTRACTS:
            current[t].write.mode("overwrite").parquet(str(args.parquet / "silver" / t))
            history[t].write.mode("overwrite").parquet(str(args.parquet / "history" / t))
        for name, df in gold.items():
            df.write.mode("overwrite").parquet(str(args.parquet / "gold" / name))

    for c in checks:
        print(f"{c.status:4}  {c.check:22} {c.table:9} {c.detail}")
    print(f"wrote {args.out} in {time.monotonic() - started:.1f}s")


if __name__ == "__main__":
    main()
