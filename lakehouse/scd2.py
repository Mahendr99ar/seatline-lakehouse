"""SCD Type 2 history from the change log.

Every change in bronze is a candidate version. A new version starts only
when a tracked column changes (a fare's price, a flight's status or times);
changes to other columns, like seats_sold moving with every booking, update
the version that is already open. A delete closes the open version.

The history is rebuilt from bronze on every run instead of being patched
incrementally. Bronze is the complete change log, so a rebuild is always
correct, including after a late or replayed event, and at this volume it
takes seconds. Patching in place would need its own logic for every one of
those cases.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .contracts import Contract

FAR_FUTURE = "9999-12-31 00:00:00"


def history(changes: DataFrame, contract: Contract) -> DataFrame:
    """One row per version: key, tracked columns, the latest value of every
    other column within the version, valid_from, valid_to and is_current.

    valid_from/valid_to are business time (`contract.event_time`), so a
    question like "what did this fare cost at 10:00 on 3 November" is answered
    by `valid_from <= t AND t < valid_to`."""
    key = list(contract.key)
    order = [F.col(contract.event_time), F.col("_lsn"), F.col("_offset")]
    w = Window.partitionBy(*key).orderBy(*order)

    tracked = F.struct(*[F.col(c) for c in contract.tracked])
    flagged = changes.withColumn("_t", tracked).withColumn(
        "_new_version",
        F.when(F.lag("_t").over(w).isNull(), True)  # first change for the key
        .when(F.col("_deleted"), True)  # a delete closes what was open
        .when(F.lag("_deleted").over(w), True)  # a row coming back after a delete
        .otherwise(~F.lag("_t").over(w).eqNullSafe(F.col("_t"))),
    )
    numbered = flagged.withColumn(
        "_version", F.sum(F.col("_new_version").cast("int")).over(w.rowsBetween(Window.unboundedPreceding, 0))
    )

    latest = F.struct(*order)
    untracked = [c for c in contract.columns if c not in contract.key and c not in contract.tracked]
    versions = numbered.groupBy(*key, "_version").agg(
        F.min(contract.event_time).alias("valid_from"),
        *[F.first(c).alias(c) for c in contract.tracked],
        *[F.max_by(c, latest).alias(c) for c in untracked if c != contract.event_time],
        F.max(contract.event_time).alias("last_changed_at"),
        F.max_by("_deleted", latest).alias("_is_delete"),
        F.count("*").alias("changes"),
    )

    by_version = Window.partitionBy(*key).orderBy("_version")
    return (
        versions.withColumn("valid_to", F.lead("valid_from").over(by_version))
        .where(~F.col("_is_delete"))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("valid_to", F.coalesce("valid_to", F.lit(FAR_FUTURE).cast("timestamp")))
        .drop("_version", "_is_delete")
    )


def as_of(versions: DataFrame, ts: str) -> DataFrame:
    """The version of every key that was valid at business time `ts`."""
    t = F.lit(ts).cast("timestamp")
    return versions.where((F.col("valid_from") <= t) & (t < F.col("valid_to")))
