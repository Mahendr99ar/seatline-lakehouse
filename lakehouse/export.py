"""Writes the gold tables as one JSON file for the static dashboard.

The dashboard is a plain HTML page with no server behind it, so the export
is the whole contract between the lake and the page: change a field name
here and the page has to change with it.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .quality import Result


def _plain(v):
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, float):
        return round(v, 4)
    return v


def rows(df: DataFrame) -> list[dict]:
    return [{k: _plain(v) for k, v in r.asDict().items()} for r in df.collect()]


def fare_story(fare_history: DataFrame, flights: DataFrame) -> dict:
    """Every price a single economy fare went through, for the timeline on the
    dashboard. Picks the departed flight whose fare changed most often."""
    departed = flights.where(F.col("status") == "departed").select(
        F.col("id").alias("flight_id"), "number", "origin", "destination", "departs_at"
    )
    eco = fare_history.where(F.col("cabin") == "economy").join(departed, "flight_id")
    busiest = eco.groupBy("flight_id").count().orderBy(F.col("count").desc(), "flight_id").first()
    if busiest is None:
        return {}
    story = eco.where(F.col("flight_id") == busiest.flight_id).orderBy("valid_from")
    versions = rows(story.select("valid_from", "valid_to", "price_paise", "seats_sold", "seats_total"))
    head = story.first()
    return {
        "flight": head.number,
        "route": f"{head.origin}-{head.destination}",
        "departs_at": _plain(head.departs_at),
        "versions": versions,
    }


def summary(bronze: DataFrame, changes: dict[str, DataFrame], history: dict[str, DataFrame]) -> dict:
    ops = {r.op: r["count"] for r in bronze.groupBy("op").count().collect()}
    spans = [df.agg(F.min("updated_at").alias("a"), F.max("updated_at").alias("b")).first() for df in changes.values()]
    return {
        "changes": sum(ops.values()),
        "by_op": ops,
        "versions": {t: h.count() for t, h in history.items()},
        "business_from": _plain(min(s.a for s in spans)),
        "business_to": _plain(max(s.b for s in spans)),
    }


def write(path: Path, *, marts: dict[str, DataFrame], checks: list[Result], extra: dict) -> None:
    doc = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        **extra,
        "checks": [c.as_dict() for c in checks],
        **{name: rows(df) for name, df in marts.items()},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, separators=(",", ":")))
