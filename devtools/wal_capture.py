"""Development-only change capture, for machines without Kafka or Debezium.

Reads the Postgres write-ahead log through a logical replication slot that
uses the built-in `test_decoding` plugin, and writes each row change as the
record Debezium would put on Kafka: same topic names, same key, same
before/after/source/op envelope (JSON converter, schemas disabled). The Spark
code cannot tell the two apart, which is the point: the tests and the sample
dashboard run on real WAL changes without the production stack.

In production Debezium does this job. Differences that matter:
- there is no initial snapshot here (`op = "r"`); create the slot before
  loading data so every row arrives as an insert;
- offsets are counters in a local file, not Kafka offsets.

    python -m devtools.wal_capture --dsn ... --slot dev --out events.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

KEYS = {"flights": ["id"], "fares": ["flight_id", "cabin"], "bookings": ["id"]}
PARTITIONS = 3

LINE = re.compile(r"^table public\.(\w+): (INSERT|UPDATE|DELETE): (.*)$")
FIELD = re.compile(r"(\w+)\[([^\]]+)\]:('(?:[^']|'')*'|null|\S+)")
COMMIT = re.compile(r"^COMMIT (\d+) \(at (.+)\)$")
INTS = {"integer", "bigint", "smallint"}


def parse_tuple(text: str) -> dict:
    row = {}
    for name, typ, raw in FIELD.findall(text):
        if raw == "null":
            row[name] = None
        elif raw.startswith("'"):
            value = raw[1:-1].replace("''", "'")
            row[name] = iso_timestamp(value) if typ == "timestamp with time zone" else value
        elif typ in INTS:
            row[name] = int(raw)
        elif typ == "boolean":
            row[name] = raw == "true"
        else:
            row[name] = raw
    return row


PG_TS = re.compile(r"(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(?:\.(\d+))?([+-])(\d{2})(?::?(\d{2}))?")


def iso_timestamp(pg: str) -> str:
    """'2026-11-01 00:05:00+00' -> '2026-11-01T00:05:00Z', the form Debezium
    uses for timestamptz (io.debezium.time.ZonedTimestamp)."""
    stamp = pg.replace(" ", "T", 1)
    return stamp[:-3] + "Z" if stamp.endswith("+00") else stamp


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def parse_change(data: str) -> tuple[str, str, dict | None, dict | None] | None:
    m = LINE.match(data)
    if not m or m.group(1) not in KEYS:
        return None
    table, kind, rest = m.groups()
    if kind == "INSERT":
        return table, "c", None, parse_tuple(rest)
    if kind == "DELETE":
        return table, "d", parse_tuple(rest), None
    old, _, new = rest.partition(" new-tuple: ")
    before = parse_tuple(old.removeprefix("old-key: ")) if new else None
    return table, "u", before, parse_tuple(new or rest)


class Capture:
    def __init__(self, dsn: str, slot: str, out: Path):
        self.dsn, self.slot, self.out = dsn, slot, out
        self.state_path = out.with_suffix(".offsets.json")
        self.offsets = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}

    def poll(self) -> int:
        """Consumes everything the slot has and appends it to the output file."""
        with psycopg.connect(self.dsn) as conn:
            rows = conn.execute(
                "SELECT lsn::text, xid::text, data FROM pg_logical_slot_get_changes"
                "(%s, NULL, NULL, 'include-xids', '1', 'include-timestamp', '1')",
                [self.slot],
            ).fetchall()
            db = conn.info.dbname
        pending: list[tuple[int, int, tuple]] = []
        written = 0
        with self.out.open("a") as f:
            for lsn, xid, data in rows:
                commit = COMMIT.match(data)
                if commit:
                    ts_ms = _ms(commit.group(2))
                    for change_lsn, change_xid, (table, op, before, after) in pending:
                        record = self._record(db, table, op, before, after, change_lsn, change_xid, ts_ms)
                        f.write(json.dumps(record) + "\n")
                        written += 1
                    pending = []
                    continue
                change = parse_change(data)
                if change:
                    pending.append((lsn_to_int(lsn), int(xid), change))
        self.state_path.write_text(json.dumps(self.offsets))
        return written

    def _record(self, db, table, op, before, after, lsn, xid, ts_ms) -> dict:
        row = after or before
        key = {k: row[k] for k in KEYS[table]}
        key_json = json.dumps(key, separators=(",", ":"))
        topic = f"src.public.{table}"
        partition = zlib.crc32(key_json.encode()) % PARTITIONS
        slot = f"{topic}:{partition}"
        offset = self.offsets.get(slot, 0)
        self.offsets[slot] = offset + 1
        value = {
            "before": before,
            "after": after,
            "source": {
                "version": "dev-capture",
                "connector": "postgresql",
                "name": "src",
                "ts_ms": ts_ms,
                "db": db,
                "schema": "public",
                "table": table,
                "txId": xid,
                "lsn": lsn,
            },
            "op": op,
            "ts_ms": int(time.time() * 1000),
        }
        return {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "timestamp": value["ts_ms"],
            "key": key_json,
            "value": json.dumps(value, separators=(",", ":")),
        }


def _ms(pg_ts: str) -> int:
    """Commit time from test_decoding ('2026-11-12 11:39:42.776271+00') in
    epoch milliseconds. Parsed by hand: before Python 3.11, fromisoformat
    rejects both a '+00' offset and fractions that are not 3 or 6 digits."""
    m = PG_TS.fullmatch(pg_ts)
    if m is None:
        raise ValueError(f"unexpected commit timestamp {pg_ts!r}")
    date, time_, frac, sign, hh, mm = m.groups()
    base = datetime.strptime(f"{date} {time_}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    offset = timedelta(hours=int(hh), minutes=int(mm or 0)) * (1 if sign == "+" else -1)
    micros = int((frac or "0").ljust(6, "0")[:6])
    return int((base - offset).timestamp() * 1000) + micros // 1000


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", required=True)
    p.add_argument("--slot", default="dev")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    print(f"captured {Capture(args.dsn, args.slot, args.out).poll()} changes")


if __name__ == "__main__":
    main()
