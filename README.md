# Seatline Lakehouse

*Change data capture from a Postgres booking database into an Iceberg lakehouse that remembers every value the source overwrote.*

![ci](https://github.com/Mahendr99ar/seatline-lakehouse/actions/workflows/ci.yml/badge.svg)
![python](https://img.shields.io/badge/python-3.10-blue)
![spark](https://img.shields.io/badge/spark-3.5.3-orange)
![iceberg](https://img.shields.io/badge/iceberg-1.6.1-teal)
![license](https://img.shields.io/badge/license-MIT-green)

> The bookings come from a deterministic simulator, not a real airline. Every number below was measured on that data.

[Dashboard](https://lake-mahendra.duckdns.org) (live once the EC2 run is done) | [Design notes](docs/decisions.md) | [EC2 runbook](deploy/ec2.md)

---

## What this is

An airline's booking database keeps one row per fare, flight and booking. When a fare goes from ₹6,400 to ₹7,680, the old price is gone. When an expired hold is purged, the booking is gone. Questions like "how full was this flight a day before it left" or "how many holds expired last Tuesday" cannot be answered from it.

Seatline Lakehouse reads the database's write-ahead log and keeps all of it.

| Layer | What it holds | Written by | Code |
|---|---|---|---|
| Bronze | every change event, as it arrived | Spark Structured Streaming, append | `lakehouse/stream.py`, `lakehouse/cdc.py` |
| Silver | current state, one row per key | Spark, `MERGE` with an LSN guard | `lakehouse/silver.py` |
| History | SCD Type 2 versions per key | Spark batch, rebuilt from bronze | `lakehouse/scd2.py` |
| Gold | six tables the dashboard reads | Spark batch, run by Airflow | `lakehouse/marts.py` |

All four layers are Iceberg tables in MinIO (S3 API) with a JDBC catalog in Postgres.

## Why not just snapshot the tables every night?

- A nightly copy sees one value per day. The fare at the top of the dashboard went through 7 prices, 5 of them in its last 4 hours before departure; a nightly snapshot would have caught at most one of those 5.
- Deletes disappear between snapshots. 2,207 of the captured events are deletes, mostly purged holds. The booking funnel counts them; a snapshot-based funnel would not know they existed.
- Reading the WAL puts no query load on the source. Debezium reads the replication stream instead of querying the tables.

## How it works

### 1. Capture

Postgres runs with `wal_level=logical`, `REPLICA IDENTITY FULL` on the three tables (so updates and deletes carry the whole old row) and a publication Debezium is not allowed to change. Debezium's Postgres connector reads it with `pgoutput` and writes one Kafka topic per table, keyed by primary key. The connector's password comes from an environment variable through Kafka's `EnvVarConfigProvider`, so it is never stored in Kafka.

### 2. Bronze, exactly once

A Spark stream reads `src.public.*` every 30 seconds. Each micro-batch is appended to `lake.bronze.changes` with its batch id written into the Iceberg snapshot summary. If the job dies after the append but before Spark records the batch as done, the restarted job sees that batch id already in the table's snapshots and skips the append.

### 3. Silver, safe against replays and reordering

The same batch is folded to the newest change per key (ordered by LSN, the position in the source's log) and `MERGE`d into silver with `WHEN MATCHED AND s._lsn > t._lsn`. A replayed or late event is older than what silver has, so it changes nothing. Deletes are soft (`_deleted = true`), so a stale update arriving after a delete cannot bring the row back.

Columns the source adds later are not dropped. They are kept in an `_extra` map and the quality checks report them.

### 4. History and gold

Every 15 minutes Airflow runs four steps:

```
quality ──► scd2_history ──► gold_marts ──► export_dashboard
```

`scd2_history` rebuilds the version tables from bronze. A new version starts only when a tracked column changes (a fare's price; a flight's times or status; a booking's status). Other columns, like `seats_sold` moving with every booking, update the open version instead of creating noise. A delete closes the open version. Versions are ordered by business time and then LSN, so the history comes out the same whatever order the events arrived in; there is a test for exactly that.

If `quality` fails, the run stops and gold keeps the last numbers that passed.

## Architecture

```
 ┌───────────────┐  WAL   ┌──────────┐        ┌───────────┐
 │ Postgres      │──────► │ Debezium │──────► │ Kafka     │  3 topics, 3 partitions
 │ flights       │pgoutput│ Connect  │  JSON  │ (KRaft)   │
 │ fares         │        └──────────┘        └─────┬─────┘
 │ bookings      │◄── traffic simulator             │
 └───────────────┘                                  ▼ every 30 s
                                         ┌─────────────────────┐
                                         │ Spark streaming     │
                                         │ bronze append       │
                                         │ silver MERGE        │
                                         └─────────┬───────────┘
                                                   ▼
          ┌──────────────── Iceberg on MinIO, JDBC catalog ───────────────┐
          │  bronze.changes   silver.*   history.*   gold.*   ops.dq_*    │
          └──────────────────────────────▲────────────────────────────────┘
                                         │ every 15 min
                                  ┌──────┴──────┐        ┌───────────────┐
                                  │ Airflow     │──────► │ data.json     │──► static dashboard
                                  │ Spark local │        └───────────────┘    (Caddy)
                                  └─────────────┘
```

| Concern | How |
|---|---|
| Ordering | LSN from the source log, Kafka offset as tie-break |
| Duplicates | batch id in Iceberg snapshot summary; LSN-guarded `MERGE` |
| Deletes | soft in silver, version-closing in history |
| Schema drift | unknown columns kept in `_extra`, reported as a warning |
| Small files | daily `rewrite_data_files`, `rewrite_position_delete_files`, `expire_snapshots` |
| WAL growth if Debezium stops | `max_slot_wal_keep_size=2GB`, heartbeat query keeps the slot moving |

## Results

Measured on one run of the simulator: 2,016 ticks of 5 simulated minutes (1 to 8 November 2026), captured from the WAL.

| Measure | Value |
|---|---|
| Change events | 45,676 (14,070 inserts, 29,399 updates, 2,207 deletes) |
| SCD2 versions | 28,201 (flights 1,067, fares 2,374, bookings 24,760) |
| Confirmed revenue in `route_daily` | ₹12,15,62,183 |
| Bronze to gold, local mode, 2 cores | 30.6 s and 32.4 s on two runs |
| Lake vs source after 450 ticks | identical, row for row, all three tables (`tests/test_end_to_end.py`) |
| Tests | 24 passing |

The local figures come from `devtools/local_run.py`, which runs the same Spark functions on changes captured by `devtools/wal_capture.py` instead of Debezium. Throughput and memory on the full Kafka, Debezium and Iceberg stack have not been measured yet; [deploy/ec2.md](deploy/ec2.md) says how, and this table will get those rows when they exist.

## How it compares

| | Nightly `pg_dump` | Query-based CDC (`updated_at > last_run`) | Seatline Lakehouse |
|---|---|---|---|
| Sees every change, not just the last one | ✗ | ✗ | ✓ |
| Sees deletes | ✗ | ✗ | ✓ |
| Load on the source | full table read | indexed scan per run | replication stream only |
| Point-in-time questions | per day | per run | per change |
| Moving parts | 1 | 2 | 8 services |

The last row is the honest cost.

## Run it

Full stack (needs Docker and about 4 GB of RAM plus swap):

```
cp .env.example .env        # set every password
docker compose up -d --build
./scripts/check.sh          # source rows vs lake rows
```

Without Docker, the Spark logic on real WAL changes (needs Java 17 and a Postgres with `wal_level=logical`):

```
pip install -e ".[dev]"
createdb airline && psql -d airline -c "SELECT pg_create_logical_replication_slot('dev', 'test_decoding')"
psql -d airline -f source/schema.sql
python -m traffic run --dsn postgresql:///airline --ticks 2016 --sleep 0
python -m devtools.wal_capture --dsn postgresql:///airline --slot dev --out events.jsonl
python -m devtools.local_run --events events.jsonl --out dashboard/data.json
cd dashboard && python -m http.server
```

Tests:

```
pytest                                                    # unit tests
TEST_DSN=postgresql:///lakehouse_test pytest              # plus the end-to-end test
```

## The dashboard

- The top chart is one fare's life. Move along it (or focus it and use the arrow keys) to see the price, seats sold and version number at any moment. The shaded part is the only thing the source table still has.
- Each section's coloured band names the lake layer its numbers come from: bronze, history, gold, or the quality checks.
- Charts with more than one colour have a "Show as a table" option.
- Light and dark themes; the choice is remembered in the browser.

## Gold tables

| Table | Question | Needs history because |
|---|---|---|
| `route_daily` | confirmed revenue per route per day | it does not; it is the baseline |
| `booking_funnel` | how did each day's holds end | expired holds are deleted from the source |
| `fare_curve` | how prices move as departure nears | the source has only the current price |
| `load_factor` | seats sold 72 h, 24 h and 0 h before departure | the source has only the final count |
| `delays` | delay against the first published time | the source has only the latest time |
| `cdc_volume` | change events per hour by table and operation | built from bronze directly |

## Repository structure

```
seatline-lakehouse/
├── source/            schema, Debezium role, catalog init
├── traffic/           booking simulator (deterministic, resumable)
├── connect/           Debezium connector config and register script
├── lakehouse/
│   ├── contracts.py   expected columns per table, SCD2 tracked columns
│   ├── cdc.py         Kafka records to bronze rows to typed changes
│   ├── silver.py      MERGE statement and its non-Iceberg equivalent
│   ├── scd2.py        version building and as-of lookup
│   ├── quality.py     checks that gate the gold run
│   ├── marts.py       gold tables
│   ├── export.py      data.json for the dashboard
│   ├── stream.py      streaming job
│   ├── gold.py        batch steps for Airflow
│   └── lake.py        table names and layout
├── airflow/dags/      lakehouse_gold, lakehouse_maintenance
├── devtools/          WAL capture without Debezium, local runner
├── dashboard/         static page, no build step (data.json is a sample from the local run)
├── bin/stream.py      spark-submit entry point
├── scripts/check.sh   source rows vs lake rows
├── docker/            images for Spark, Airflow, the simulator
├── conf/              spark-defaults.conf (Iceberg catalog, S3)
├── deploy/ec2.md      runbook
├── docs/decisions.md  why it is built this way
└── tests/
```

## What Seatline Lakehouse will not do

- Serve queries in real time. Silver is at most one trigger interval behind; gold is up to 15 minutes behind.
- Replace the source's backups. Bronze keeps changes from the moment the connector started, not before.
- Handle more than one Postgres database or a primary key change without code changes.

## Limitations

- The full stack has run here only as configuration; the end-to-end run on Kafka, Debezium and Iceberg happens on the EC2 box (see the runbook). The Spark logic is tested on real WAL changes, and the `MERGE` is the one piece the local runner replaces with an equivalent fold.
- The SCD2 rebuild reads all of bronze every run. That is fine at this volume and would need to become incremental at a few hundred million events.
- Single-node Kafka with replication factor 1. Losing the disk loses unconsumed events.
- Spark runs in local mode inside the Airflow task to fit in 4 GB.

## Next

- [x] Bronze, silver, SCD2 history and gold on captured WAL changes
- [x] End-to-end test: lake equals source after deletes and a schema change
- [ ] Measure the full stack on EC2 and add the numbers above
- [ ] Incremental SCD2 for keys touched since the last run
- [ ] Terraform for S3 and Glue in place of MinIO and the JDBC catalog

## License

MIT

*Built by [Mahendr99ar](https://github.com/Mahendr99ar).*
