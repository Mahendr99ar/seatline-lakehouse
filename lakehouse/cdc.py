"""From Kafka records to typed change rows.

Debezium's JSON envelope (schemas disabled) looks like

    {"before": {...} | null, "after": {...} | null,
     "source": {"table": "bookings", "lsn": 2468, "txId": 771, "ts_ms": ...},
     "op": "c" | "u" | "d" | "r", "ts_ms": ...}

Bronze keeps that envelope almost as it came, with the row images left as
JSON text. Parsing them against a contract happens one step later, so a
column the source adds tomorrow is still in bronze when someone wants it.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .contracts import Contract

# before/after as STRING keeps the row image as raw JSON text.
ENVELOPE = (
    "before STRING, after STRING, op STRING, ts_ms BIGINT, "
    "source STRUCT<table: STRING, lsn: BIGINT, txId: BIGINT, ts_ms: BIGINT>"
)

TOPIC_PATTERN = r"src\.public\..*"


def to_bronze(records: DataFrame) -> DataFrame:
    """Kafka source rows (topic, partition, offset, timestamp, key, value) to
    bronze rows. Tombstones and anything that is not a table topic (the
    Debezium heartbeat topic, for one) are dropped here."""
    env = F.from_json(F.col("value").cast("string"), ENVELOPE)
    return (
        records.where(F.col("value").isNotNull() & F.col("topic").rlike(f"^{TOPIC_PATTERN}$"))
        .select(
            F.col("topic"),
            F.col("partition").cast("int").alias("partition"),
            F.col("offset").cast("bigint").alias("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("key"),
            env.alias("e"),
        )
        .select(
            F.col("e.source.table").alias("source_table"),
            F.col("e.op").alias("op"),
            F.col("e.source.lsn").alias("lsn"),
            F.col("e.source.txId").alias("tx_id"),
            F.timestamp_millis(F.col("e.source.ts_ms")).alias("source_ts"),
            F.col("e.before").alias("before"),
            F.col("e.after").alias("after"),
            "key",
            "topic",
            "partition",
            "offset",
            "kafka_ts",
            F.current_timestamp().alias("ingested_at"),
        )
    )


def _typed(json_col: Column, contract: Contract) -> list[Column]:
    # Timestamps are read as text and cast, because Debezium writes them as
    # ISO-8601 with up to six fractional digits, which the JSON reader's
    # default timestamp pattern does not accept.
    as_text = ", ".join(f"{n} STRING" if t == "TIMESTAMP" else f"{n} {t}" for n, t in contract.columns.items())
    parsed = F.from_json(json_col, as_text)
    return [
        (F.to_timestamp(parsed[n]) if t == "TIMESTAMP" else parsed[n]).alias(n) for n, t in contract.columns.items()
    ]


def change_rows(bronze: DataFrame, contract: Contract) -> DataFrame:
    """One typed row per change to `contract.table`. A delete carries the
    row as it was before the delete, flagged `_deleted`."""
    image = F.coalesce(F.col("after"), F.col("before"))
    known = F.array(*[F.lit(c) for c in contract.columns])
    everything = F.from_json(image, "MAP<STRING, STRING>")
    extra = F.map_filter(everything, lambda k, v: ~F.array_contains(known, k))
    return bronze.where(F.col("source_table") == contract.table).select(
        *_typed(image, contract),
        F.col("lsn").alias("_lsn"),
        F.col("op").alias("_op"),
        (F.col("op") == "d").alias("_deleted"),
        F.col("source_ts").alias("_source_ts"),
        F.col("ingested_at").alias("_ingested_at"),
        F.when(F.size(extra) > 0, extra).alias("_extra"),
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
    )


def latest_per_key(changes: DataFrame, contract: Contract) -> DataFrame:
    """Keeps the last change for each key. Ordering is by LSN, the position in
    the source's write-ahead log, which is the order the database applied
    them. Kafka offset breaks ties (Debezium snapshot rows share one LSN)."""
    w = Window.partitionBy(*contract.key).orderBy(F.col("_lsn").desc(), F.col("_offset").desc())
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_partition", "_offset")
