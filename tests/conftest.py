import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RECORD = "topic STRING, partition INT, offset BIGINT, timestamp BIGINT, key STRING, value STRING"


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


class Events:
    """Builds Kafka records shaped like Debezium's, in the order given.
    LSNs and offsets increase with each call unless set explicitly."""

    def __init__(self):
        self.records = []
        self.lsn = 1000
        self.offset = 0

    def add(self, table, op, before=None, after=None, lsn=None, topic=None, partition=0):
        self.lsn += 8
        self.offset += 1
        row = after or before
        key = {k: row[k] for k in (("flight_id", "cabin") if table == "fares" else ("id",))}
        value = {
            "before": before,
            "after": after,
            "source": {"table": table, "lsn": lsn if lsn is not None else self.lsn, "txId": 1, "ts_ms": 0},
            "op": op,
            "ts_ms": 0,
        }
        self.records.append(
            (topic or f"src.public.{table}", partition, self.offset, 0, json.dumps(key), json.dumps(value))
        )
        return self

    def tombstone(self, table):
        self.offset += 1
        self.records.append((f"src.public.{table}", 0, self.offset, 0, "{}", None))
        return self

    def frame(self, spark):
        return spark.createDataFrame(self.records, RECORD).withColumn("timestamp", F.timestamp_millis("timestamp"))


@pytest.fixture
def events():
    return Events()


def fare(price, sold=0, at="2026-11-01T00:00:00Z", flight="f1", cabin="economy", total=108):
    return {
        "flight_id": flight,
        "cabin": cabin,
        "price_paise": price,
        "seats_total": total,
        "seats_sold": sold,
        "updated_at": at,
    }


def booking(bid, status, at, flight="f1", seats=1, total=500000, created="2026-11-01T00:00:00Z", **extra):
    return {
        "id": bid,
        "flight_id": flight,
        "cabin": "economy",
        "seats": seats,
        "total_paise": total,
        "status": status,
        "created_at": created,
        "updated_at": at,
        **extra,
    }


def flight(
    fid="f1",
    departs="2026-11-03T00:40:00Z",
    arrives="2026-11-03T02:50:00Z",
    status="scheduled",
    at="2026-11-01T00:00:00Z",
):
    return {
        "id": fid,
        "number": "SL 100",
        "origin": "DEL",
        "destination": "BOM",
        "departs_at": departs,
        "arrives_at": arrives,
        "status": status,
        "updated_at": at,
    }
