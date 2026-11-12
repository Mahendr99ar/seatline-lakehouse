"""The streaming job: Kafka -> bronze -> silver, one micro-batch at a time.

    spark-submit bin/stream.py   (the spark-stream service in docker-compose.yml)

Exactly-once into the lake rests on two things. Spark's checkpoint records
which Kafka offsets each batch covered, so a restarted job asks Kafka for the
same batch again. If the crash came after the bronze commit but before the
checkpoint was written, the replayed batch would be appended twice; each
bronze commit therefore carries its batch id as a snapshot property, and a
batch id that is already there is skipped. The silver MERGE needs no such
guard: it only applies changes newer than the row's LSN, so running it twice
leaves the same rows.
"""

from __future__ import annotations

import json
import os
import time
from functools import cache
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from . import cdc, lake, silver
from .contracts import CONTRACTS

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "/checkpoints/cdc")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
MAX_OFFSETS = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000")
BATCH_PROPERTY = "lakehouse.batch-id"


def already_committed(spark: SparkSession, batch_key: str) -> bool:
    found = spark.sql(
        f"SELECT 1 FROM {lake.BRONZE}.snapshots WHERE summary['{BATCH_PROPERTY}'] = '{batch_key}' LIMIT 1"
    )
    return found.count() > 0


@cache
def query_id() -> str:
    """The streaming query's id, which Spark writes to the checkpoint before
    the first batch and keeps across restarts. Deleting the checkpoint gives
    a new id, so batch 0 of a fresh start is not mistaken for the old one."""
    return json.loads(Path(CHECKPOINT, "metadata").read_text())["id"]


def process(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    started = time.monotonic()
    bronze = cdc.to_bronze(batch).persist()
    try:
        key = f"{query_id()}:{batch_id}"
        if not already_committed(spark, key):
            bronze.writeTo(lake.BRONZE).option(f"snapshot-property.{BATCH_PROPERTY}", key).append()

        for table, contract in CONTRACTS.items():
            changes = cdc.change_rows(bronze, contract)
            latest = cdc.latest_per_key(changes, contract)
            view = f"incoming_{table}"
            latest.createOrReplaceTempView(view)
            spark.sql(silver.merge_sql(contract, view, lake.CATALOG))
        # One line per batch, so throughput can be read off the container log.
        print(f"batch {batch_id}: {bronze.count()} changes in {time.monotonic() - started:.1f}s", flush=True)
    finally:
        bronze.unpersist()


def main() -> None:
    spark = lake.session("lakehouse-stream")
    lake.create_all(spark)
    for contract in CONTRACTS.values():
        spark.sql(silver.create_sql(contract, lake.CATALOG))

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", cdc.TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        # Retention deleting offsets the checkpoint still points at would
        # mean lost changes; better to stop and say so.
        .option("failOnDataLoss", "true")
        .load()
    )

    (
        source.writeStream.queryName("cdc")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .foreachBatch(process)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
