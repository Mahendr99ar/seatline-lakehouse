"""Data quality checks that run before the gold tables are rebuilt.

Each check returns a Result. `fail` stops the gold run (the dashboards keep
the last good numbers instead of showing wrong ones); `warn` is reported and
the run goes on. Which is which is a judgement per check, written next to it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .contracts import Contract

PASS, WARN, FAIL = "pass", "warn", "fail"


@dataclass
class Result:
    check: str
    table: str
    status: str
    bad_rows: int
    detail: str

    def as_dict(self) -> dict:
        return asdict(self)


def _result(check: str, table: str, bad: int, detail: str, on_bad: str = FAIL) -> Result:
    return Result(check, table, PASS if bad == 0 else on_bad, bad, detail)


def live(df: DataFrame) -> DataFrame:
    return df.where(~F.col("_deleted"))


def duplicate_keys(silver: DataFrame, contract: Contract) -> Result:
    # Should be impossible after a MERGE on the key. If it happens the merge
    # condition is wrong, and every number built on the table is wrong too.
    bad = silver.groupBy(*contract.key).count().where("count > 1").count()
    key = ", ".join(contract.key)
    return _result("duplicate_keys", contract.table, bad, f"keys appearing more than once on ({key})")


def bookings_valid(bookings: DataFrame) -> Result:
    b = live(bookings)
    bad = b.where(
        (F.col("seats") < 1) | (F.col("seats") > 9) | (F.col("total_paise") <= 0) | F.col("total_paise").isNull()
    ).count()
    return _result("bookings_valid", "bookings", bad, "seats outside 1..9 or a non-positive total")


def fares_within_capacity(fares: DataFrame) -> Result:
    bad = live(fares).where((F.col("seats_sold") < 0) | (F.col("seats_sold") > F.col("seats_total"))).count()
    return _result("fares_within_capacity", "fares", bad, "seats_sold below 0 or above seats_total")


def flights_arrive_after_departing(flights: DataFrame) -> Result:
    bad = live(flights).where(F.col("arrives_at") <= F.col("departs_at")).count()
    return _result("arrive_after_depart", "flights", bad, "arrives_at not after departs_at")


def orphan_bookings(bookings: DataFrame, flights: DataFrame, grace_minutes: int = 10) -> Result:
    # A booking can land in a micro-batch before its flight does (different
    # topics, different partitions), so only bookings older than the grace
    # period count. A warning, not a failure: the flight usually turns up in
    # the next batch, and the revenue mart joins on flights anyway.
    newest = bookings.agg(F.max("updated_at").alias("t"))
    cutoff = newest.select((F.col("t") - F.expr(f"INTERVAL {grace_minutes} MINUTES")).alias("cutoff"))
    bad = (
        live(bookings)
        .crossJoin(cutoff)
        .where(F.col("created_at") < F.col("cutoff"))
        .join(live(flights).select(F.col("id").alias("flight_id")), "flight_id", "left_anti")
        .count()
    )
    return _result("orphan_bookings", "bookings", bad, f"bookings with no flight after {grace_minutes} min", WARN)


def schema_drift(silver: DataFrame, contract: Contract) -> Result:
    # Columns the source added that the contract does not know. Their values
    # are kept in _extra, so nothing is lost; the warning is the prompt to add
    # them to the contract.
    seen = silver.where(F.col("_extra").isNotNull()).select(F.explode(F.map_keys("_extra")).alias("k")).distinct()
    names = sorted(r.k for r in seen.collect())
    rows = silver.where(F.col("_extra").isNotNull()).count() if names else 0
    detail = f"unknown columns: {', '.join(names)}" if names else "no unknown columns"
    return _result("schema_drift", contract.table, rows, detail, WARN)


def freshness(bronze: DataFrame, max_lag_minutes: int) -> Result:
    # How far behind the newest change in bronze is, compared with the clock
    # of whoever runs the check. Stale bronze means Debezium or the stream
    # job has stopped; the gold tables would quietly stop moving.
    seconds = bronze.agg(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("source_ts"))).alias("s")
    ).first()["s"]
    if seconds is None:
        return Result("freshness", "bronze", FAIL, 1, "bronze is empty")
    minutes = int(seconds // 60)
    bad = 1 if minutes > max_lag_minutes else 0
    return _result("freshness", "bronze", bad, f"newest change is {minutes} min old (limit {max_lag_minutes})")


def run_all(
    bronze: DataFrame, silver: dict[str, DataFrame], contracts: dict[str, Contract], max_lag_minutes: int | None = 30
) -> list[Result]:
    results = [duplicate_keys(silver[t], c) for t, c in contracts.items()]
    results += [
        bookings_valid(silver["bookings"]),
        fares_within_capacity(silver["fares"]),
        flights_arrive_after_departing(silver["flights"]),
        orphan_bookings(silver["bookings"], silver["flights"]),
    ]
    results += [schema_drift(silver[t], c) for t, c in contracts.items()]
    if max_lag_minutes is not None:
        results.append(freshness(bronze, max_lag_minutes))
    return results


def failed(results: list[Result]) -> list[Result]:
    return [r for r in results if r.status == FAIL]
