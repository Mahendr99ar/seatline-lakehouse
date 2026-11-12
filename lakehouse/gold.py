"""Batch steps on the lake, one per Airflow task.

    python -m lakehouse.gold quality | history | marts | export | maintain

Each step reads what the previous one wrote from Iceberg, so a failed task
can be retried on its own.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import cdc, export, lake, marts, quality, scd2
from .contracts import CONTRACTS

EXPORT_PATH = Path(os.environ.get("DASHBOARD_DATA", "/dashboard/data.json"))
MAX_LAG_MINUTES = int(os.environ.get("MAX_LAG_MINUTES", "30"))


def _silver(spark: SparkSession) -> dict:
    return {t: spark.table(f"{lake.CATALOG}.silver.{t}") for t in CONTRACTS}


def _history(spark: SparkSession) -> dict:
    return {t: spark.table(lake.history_table(t)) for t in CONTRACTS}


def _changes(spark: SparkSession) -> dict:
    bronze = spark.table(lake.BRONZE)
    return {t: cdc.change_rows(bronze, c) for t, c in CONTRACTS.items()}


def step_quality(spark: SparkSession) -> int:
    results = quality.run_all(spark.table(lake.BRONZE), _silver(spark), CONTRACTS, MAX_LAG_MINUTES)
    rows = [(r.check, r.table, r.status, r.bad_rows, r.detail) for r in results]
    schema = "check_name STRING, table_name STRING, status STRING, bad_rows BIGINT, detail STRING"
    (
        spark.createDataFrame(rows, schema)
        .withColumn("run_at", F.current_timestamp())
        .select("run_at", "check_name", "table_name", "status", "bad_rows", "detail")
        .writeTo(lake.DQ_RESULTS)
        .append()
    )
    for r in results:
        print(f"{r.status:4}  {r.check:22} {r.table:9} {r.detail}")
    bad = quality.failed(results)
    if bad:
        print(f"{len(bad)} check(s) failed; gold is left as it was", file=sys.stderr)
        return 1
    return 0


def step_history(spark: SparkSession) -> int:
    for t, df in _changes(spark).items():
        scd2.history(df, CONTRACTS[t]).writeTo(lake.history_table(t)).using("iceberg").createOrReplace()
    return 0


def step_marts(spark: SparkSession) -> int:
    for name, df in marts.build(_silver(spark), _history(spark), _changes(spark)).items():
        df.writeTo(lake.gold_table(name)).using("iceberg").createOrReplace()
    return 0


def step_export(spark: SparkSession) -> int:
    names = ["route_daily", "booking_funnel", "fare_curve", "load_factor", "delays", "cdc_volume"]
    gold = {n: spark.table(lake.gold_table(n)) for n in names}
    latest_run = spark.table(lake.DQ_RESULTS).agg(F.max("run_at")).first()[0]
    checks = [
        quality.Result(r.check_name, r.table_name, r.status, r.bad_rows, r.detail)
        for r in spark.table(lake.DQ_RESULTS).where(F.col("run_at") == F.lit(latest_run)).collect()
    ]
    history = _history(spark)
    export.write(
        EXPORT_PATH,
        marts=gold,
        checks=checks,
        extra={
            "summary": export.summary(spark.table(lake.BRONZE), _changes(spark), history),
            "fare_story": export.fare_story(history["fares"], _silver(spark)["flights"]),
        },
    )
    print(f"wrote {EXPORT_PATH}")
    return 0


def step_maintain(spark: SparkSession) -> int:
    """Daily housekeeping. A streaming job commits every 30 s, which leaves
    many small files and thousands of snapshots; both slow every reader."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    tables = [lake.BRONZE, *[f"{lake.CATALOG}.silver.{t}" for t in CONTRACTS]]
    for t in tables:
        name = t.split(".", 1)[1]
        spark.sql(f"CALL {lake.CATALOG}.system.rewrite_data_files(table => '{name}')").show()
        # Silver is merge-on-read, so it also collects delete files.
        if ".silver." in t:
            spark.sql(f"CALL {lake.CATALOG}.system.rewrite_position_delete_files(table => '{name}')").show()
        spark.sql(
            f"CALL {lake.CATALOG}.system.expire_snapshots(table => '{name}', "
            f"older_than => TIMESTAMP '{cutoff}', retain_last => 50)"
        ).show()
    return 0


STEPS = {
    "quality": step_quality,
    "history": step_history,
    "marts": step_marts,
    "export": step_export,
    "maintain": step_maintain,
}


def main() -> None:
    p = argparse.ArgumentParser(prog="lakehouse.gold")
    p.add_argument("step", choices=STEPS)
    args = p.parse_args()
    spark = lake.session(f"lakehouse-{args.step}")
    try:
        sys.exit(STEPS[args.step](spark))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
