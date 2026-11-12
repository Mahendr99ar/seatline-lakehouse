"""Gold tables. Each one answers a question the operational database cannot,
because the answer needs history the database has already overwritten.

Business dates are in IST, the way the airline reports them. Times inside
the lake stay in UTC.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .contracts import CONTRACTS

IST = "Asia/Kolkata"


def _ist_date(col: str) -> F.Column:
    return F.to_date(F.from_utc_timestamp(F.col(col), IST))


def _route(flights: DataFrame) -> DataFrame:
    return flights.select(
        F.col("id").alias("flight_id"),
        F.concat_ws("-", "origin", "destination").alias("route"),
        "number",
    )


def route_daily(bookings: DataFrame, flights: DataFrame) -> DataFrame:
    """Confirmed revenue and seats per route per booking day."""
    confirmed = bookings.where(~F.col("_deleted") & (F.col("status") == "CONFIRMED"))
    return (
        confirmed.join(_route(flights), "flight_id")
        .groupBy(_ist_date("created_at").alias("day"), "route")
        .agg(
            F.count("*").alias("bookings"),
            F.sum("seats").alias("seats"),
            F.sum("total_paise").alias("revenue_paise"),
        )
    )


def booking_funnel(booking_history: DataFrame) -> DataFrame:
    """How holds ended, per day the hold was made. Uses SCD2 history rather
    than current state: an expired hold is purged from the source after a
    day, so current state alone would undercount expiries."""
    outcome = booking_history.groupBy("id").agg(
        F.min("created_at").alias("created_at"),
        F.max(F.when(F.col("status") == "CONFIRMED", 1).otherwise(0)).alias("was_confirmed"),
        F.max_by("status", "valid_from").alias("final_status"),
    )
    return (
        outcome.groupBy(_ist_date("created_at").alias("day"))
        .agg(
            F.count("*").alias("held"),
            F.sum("was_confirmed").alias("confirmed"),
            F.sum((F.col("final_status") == "EXPIRED").cast("int")).alias("expired"),
            F.sum(((F.col("final_status") == "CANCELLED") & (F.col("was_confirmed") == 1)).cast("int")).alias(
                "cancelled_after_confirm"
            ),
        )
        .orderBy("day")
    )


def fare_curve(fare_history: DataFrame, flights: DataFrame) -> DataFrame:
    """Average price, as a multiple of the flight's opening price, by hours
    left before departure. Each fare version counts for every hour it was
    on sale, so a price that held for a day weighs more than one that lasted
    five minutes."""
    first = fare_history.groupBy("flight_id", "cabin").agg(F.min_by("price_paise", "valid_from").alias("opening"))
    dep = flights.select(F.col("id").alias("flight_id"), "departs_at")
    hours = (
        fare_history.join(first, ["flight_id", "cabin"])
        .join(dep, "flight_id")
        .withColumn("end", F.least("valid_to", "departs_at"))
        .where(F.col("valid_from") < F.col("end"))
        .withColumn(
            "h",
            F.explode(
                F.sequence(
                    F.ceil((F.unix_timestamp("departs_at") - F.unix_timestamp("end")) / 3600).cast("int"),
                    F.floor((F.unix_timestamp("departs_at") - F.unix_timestamp("valid_from")) / 3600).cast("int"),
                )
            ),
        )
    )
    return (
        hours.where(F.col("h").between(0, 167))
        .groupBy("cabin", F.col("h").alias("hours_before"))
        .agg(F.avg(F.col("price_paise") / F.col("opening")).alias("price_index"), F.count("*").alias("samples"))
        .orderBy("cabin", F.col("hours_before").desc())
    )


def load_factor(fare_history: DataFrame, flights: DataFrame) -> DataFrame:
    """Share of seats sold 72 h, 24 h and 0 h before each departed flight left,
    per route. Silver only knows the final number; the earlier ones come from
    the fare version that was valid at each moment."""
    departed = flights.where(~F.col("_deleted") & (F.col("status") == "departed")).select(
        F.col("id").alias("flight_id"), "departs_at"
    )
    marks = departed.crossJoin(
        fare_history.sparkSession.createDataFrame([(72,), (24,), (0,)], "hours_before INT")
    ).withColumn("at", F.col("departs_at") - F.make_interval(hours=F.col("hours_before")))
    at_mark = (
        fare_history.join(marks, "flight_id")
        .where((F.col("valid_from") <= F.col("at")) & (F.col("at") < F.col("valid_to")))
        .groupBy("flight_id", "hours_before")
        .agg(F.sum("seats_sold").alias("sold"), F.sum("seats_total").alias("total"))
    )
    # Flights that left in the first three days have no 72 h reading; leaving
    # them in would compare different sets of flights at each mark.
    complete = at_mark.groupBy("flight_id").count().where("count = 3").select("flight_id")
    return (
        at_mark.join(complete, "flight_id")
        .join(_route(flights), "flight_id")
        .groupBy("route", "hours_before")
        .agg(F.countDistinct("flight_id").alias("flights"), (F.sum("sold") / F.sum("total")).alias("load_factor"))
        .orderBy("route", F.col("hours_before").desc())
    )


def delays(flight_history: DataFrame) -> DataFrame:
    """Delay per flight against the schedule it was first published with.
    Silver only has the latest departure time; the original is in the first
    SCD2 version."""
    per_flight = flight_history.groupBy("id", "origin", "destination").agg(
        F.min_by("departs_at", "valid_from").alias("scheduled"),
        F.max_by("departs_at", "valid_from").alias("actual"),
        F.max_by("status", "valid_from").alias("status"),
        F.count("*").alias("versions"),
    )
    return (
        per_flight.where(F.col("status") != "cancelled")
        .withColumn("delay_min", (F.unix_timestamp("actual") - F.unix_timestamp("scheduled")) / 60)
        .groupBy(F.concat_ws("-", "origin", "destination").alias("route"))
        .agg(
            F.count("*").alias("flights"),
            F.sum((F.col("delay_min") > 0).cast("int")).alias("delayed"),
            F.avg(F.when(F.col("delay_min") > 0, F.col("delay_min"))).alias("avg_delay_min"),
        )
        .orderBy("route")
    )


def cdc_volume(changes: dict[str, DataFrame]) -> DataFrame:
    """Changes per hour of business time, by table and operation."""
    parts = [
        df.select(
            F.lit(t).alias("table"),
            F.date_trunc("hour", F.from_utc_timestamp(F.col(CONTRACTS[t].event_time), IST)).alias("hour"),
            "_op",
        )
        for t, df in changes.items()
    ]
    union = parts[0]
    for p in parts[1:]:
        union = union.unionByName(p)
    return union.groupBy("hour", "table", F.col("_op").alias("op")).count().orderBy("hour", "table", "op")


def build(silver: dict, history: dict, changes: dict) -> dict[str, DataFrame]:
    """All marts, from silver (current state), SCD2 history and the typed
    change rows, each a dict keyed by source table."""
    return {
        "route_daily": route_daily(silver["bookings"], silver["flights"]),
        "booking_funnel": booking_funnel(history["bookings"]),
        "fare_curve": fare_curve(history["fares"], silver["flights"]),
        "load_factor": load_factor(history["fares"], silver["flights"]),
        "delays": delays(history["flights"]),
        "cdc_volume": cdc_volume(changes),
    }
