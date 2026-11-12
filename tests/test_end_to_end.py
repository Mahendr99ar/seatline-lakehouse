"""Runs the simulator against a real Postgres, captures the WAL, and checks
that the lake's current state equals the source tables row for row.

Needs a Postgres with wal_level=logical. Skipped unless TEST_DSN is set:

    TEST_DSN="postgresql://postgres@/lakehouse_test?host=/tmp" pytest tests/test_end_to_end.py
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import psycopg
import pytest

from devtools.wal_capture import Capture
from lakehouse import cdc, scd2, silver
from lakehouse.contracts import CONTRACTS
from traffic.sim import Config, Simulator

DSN = os.environ.get("TEST_DSN")
SCHEMA = Path(__file__).parent.parent / "source" / "schema.sql"
SLOT = "lakehouse_e2e"

pytestmark = pytest.mark.skipif(not DSN, reason="TEST_DSN not set")


@pytest.fixture(scope="module")
def captured(tmp_path_factory):
    out = tmp_path_factory.mktemp("e2e") / "events.jsonl"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PUBLICATION IF EXISTS lakehouse")
        conn.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", [SLOT]
        )
        conn.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", [SLOT])
        conn.execute(SCHEMA.read_text())
        # Schema change early, so the run covers rows before and after it.
        sim = Simulator(conn, Config(seed=11, schema_change_tick=60))
        sim.setup()
        for _ in range(450):
            sim.tick()
        Capture(DSN, SLOT, out).poll()
        conn.execute("SELECT pg_drop_replication_slot(%s)", [SLOT])
        source = {
            t: {
                tuple(str(v) for v in r[: len(c.key)]): r[len(c.key) :]
                for r in conn.execute(
                    f"SELECT {', '.join(c.key)}, {', '.join(x for x in c.columns if x not in c.key)} FROM {t}"
                ).fetchall()
            }
            for t, c in CONTRACTS.items()
        }
    return out, source


def _lake(spark, path):
    from conftest import RECORD
    from pyspark.sql import functions as F

    records = spark.read.schema(RECORD).json(str(path)).withColumn("timestamp", F.timestamp_millis("timestamp"))
    bronze = cdc.to_bronze(records)
    changes = {t: cdc.change_rows(bronze, c) for t, c in CONTRACTS.items()}
    return bronze, changes


def _plain(v):
    # psycopg returns aware datetimes and UUID objects; Spark gives naive UTC
    # datetimes and strings.
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(v, UUID):
        return str(v)
    return v


def test_silver_matches_the_source(spark, captured):
    path, source = captured
    _, changes = _lake(spark, path)
    for t, c in CONTRACTS.items():
        lake = {}
        for r in silver.fold(changes[t], c).where("NOT _deleted").collect():
            key = tuple(str(r[k]) for k in c.key)
            lake[key] = tuple(r[x] for x in c.columns if x not in c.key)
        expected = {k: tuple(_plain(v) for v in row) for k, row in source[t].items()}
        assert lake == expected, t


def test_deletes_and_drift_were_exercised(spark, captured):
    path, _ = captured
    bronze, changes = _lake(spark, path)
    ops = {r.op for r in bronze.select("op").distinct().collect()}
    assert ops == {"c", "u", "d"}
    with_channel = changes["bookings"].where("_extra['channel'] IS NOT NULL").count()
    assert with_channel > 0


def test_current_history_versions_agree_with_silver(spark, captured):
    path, _ = captured
    _, changes = _lake(spark, path)
    for t, c in CONTRACTS.items():
        current = scd2.history(changes[t], c).where("is_current")
        live = silver.fold(changes[t], c).where("NOT _deleted")
        assert current.count() == live.count(), t
        tracked = [*c.key, *c.tracked]
        assert current.select(*tracked).exceptAll(live.select(*tracked)).count() == 0, t
