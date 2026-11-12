"""Iceberg table layout and the Spark session that talks to it.

    lake.bronze.changes        every change event, append-only
    lake.silver.<table>        current state, one row per key, MERGEd
    lake.history.<table>       SCD2 versions, rebuilt from bronze
    lake.gold.<mart>           tables the dashboard reads
    lake.ops.dq_results        one row per check per run

Catalog, warehouse and S3 settings come from conf/spark-defaults.conf in
the image, not from code, so the same jobs run against MinIO on the box or
against S3 by changing that one file.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lake")
BRONZE = f"{CATALOG}.bronze.changes"
DQ_RESULTS = f"{CATALOG}.ops.dq_results"


def session(app: str) -> SparkSession:
    builder = SparkSession.builder.appName(app)
    # The catalog password is the one setting spark-defaults.conf cannot hold,
    # since that file is baked into the image. S3 credentials need nothing
    # here: S3FileIO reads AWS_ACCESS_KEY_ID and friends from the environment.
    password = os.environ.get("CATALOG_PASSWORD")
    if password:
        builder = builder.config(f"spark.sql.catalog.{CATALOG}.jdbc.password", password)
    return builder.getOrCreate()


def history_table(table: str) -> str:
    return f"{CATALOG}.history.{table}"


def gold_table(name: str) -> str:
    return f"{CATALOG}.gold.{name}"


def create_all(spark: SparkSession) -> None:
    for ns in ("bronze", "silver", "history", "gold", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    # Partitioned by table and day of the source commit: the SCD2 rebuild
    # reads one table at a time, and maintenance compacts by day.
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
  source_table STRING, op STRING, lsn BIGINT, tx_id BIGINT, source_ts TIMESTAMP,
  before STRING, after STRING, key STRING,
  topic STRING, partition INT, offset BIGINT, kafka_ts TIMESTAMP, ingested_at TIMESTAMP
) USING iceberg
PARTITIONED BY (source_table, days(source_ts))
TBLPROPERTIES ('format-version' = '2')
""".strip()
    )
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {DQ_RESULTS} (
  run_at TIMESTAMP, check_name STRING, table_name STRING, status STRING, bad_rows BIGINT, detail STRING
) USING iceberg
""".strip()
    )
