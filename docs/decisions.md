# Design notes

Why Seatline Lakehouse is built the way it is, and what each choice costs.

## Log-based CDC instead of polling `updated_at`

Polling misses every intermediate value between two polls and never sees a
delete. Reading the WAL sees both, and costs the source a replication
connection instead of repeated scans. The cost is operational: a replication
slot holds WAL until the consumer confirms it, so a stopped Debezium can fill
the source's disk. `max_slot_wal_keep_size=2GB` caps that; past the cap
Postgres drops the slot and the connector needs a fresh snapshot, which is
the better failure.

## `REPLICA IDENTITY FULL`

Without it, an update event's `before` and a delete event carry only the
primary key. The SCD2 code needs the whole old row of a delete to close the
version with the right values, and the drift check needs every column. The
cost is a larger WAL record per update on these tables.

## The publication is created by the schema, not by Debezium

`publication.autocreate.mode=disabled`, so the Debezium role needs
`REPLICATION` and `SELECT`, not table ownership. Adding a table to the
pipeline is a reviewed change to `source/schema.sql`, not something the
connector does on its own.

## Bronze keeps the row images as JSON text

Parsing happens when bronze is read, against a contract. A column the source
adds is still in bronze even though silver does not know it yet, and adding it
to the contract later backfills it from bronze by rerunning the history step.
The cost is parsing JSON on every rebuild.

## Exactly once: batch id in the snapshot summary

Spark's checkpoint alone gives at-least-once into an external sink: a crash
between the Iceberg commit and the checkpoint write replays the batch. Iceberg
lets a commit carry arbitrary summary properties, so each bronze append
records `<query id>:<batch id>`, and a replayed batch finds itself and skips
the append. The query id comes from the checkpoint, so deleting the
checkpoint (a deliberate full replay) does not collide with old batch ids.

Snapshot expiry removes old summaries. That is safe because only the most
recent batch can ever be replayed, and expiry keeps the last 50 snapshots.

## Silver: LSN guard and soft deletes

A `MERGE` that always overwrites would let a replayed or late event roll a
row back. Comparing LSNs makes the merge idempotent and order-independent.
Hard deletes would lose the LSN of the delete, and a late update for that key
would then be inserted as a new row; keeping the row with `_deleted = true`
prevents that. Readers filter on `NOT _deleted`.

Silver tables are merge-on-read (Iceberg v2 position deletes), because a
`MERGE` every 30 seconds rewriting whole data files would be far more
expensive than writing small delete files. The maintenance DAG compacts them.

## SCD2 is rebuilt, not patched

Patching history in place needs separate logic for a late event, a replayed
event, an event older than the current version, and a delete followed by a
reinsert. Rebuilding from bronze is one function and is correct in all of
those cases by construction. It reads all of bronze each run, which takes
seconds at this volume. The plan when it stops being cheap is to rebuild only
keys touched since the last run and `MERGE` their versions.

## Tracked columns

If every change made a version, `fares` would get a version per booking
(`seats_sold` changes each time) and the history would mostly be noise. Only
columns someone would ask "what was it at time T" about start a version; the
rest keep their latest value within the version. The choice is per table, in
`lakehouse/contracts.py`.

## Versions are ordered by business time, then LSN

`updated_at` is when the change happened in the business; the LSN is when
the database wrote it. For this source they agree. Ordering by business time
first means that if a system ever writes a correction with an earlier
`updated_at`, it lands in the right place in history.

## Quality checks gate gold

A dashboard that shows wrong numbers is worse than one that shows numbers 15
minutes old. Checks that indicate a broken pipeline (duplicate keys, sold
seats above capacity, stale bronze) fail the run. Checks that indicate
something to look at (orphan bookings within the grace period, new source
columns) warn and let it continue.

## Spark in local mode inside Airflow tasks

A Spark standalone master and worker would hold memory all day for jobs that
run a few minutes in every fifteen. On a 4 GB machine shared with Kafka and
Debezium, each Airflow task starts its own local-mode Spark and exits.
Airflow's parallelism is 1 so two of them never run at once. On a bigger box
or in the cloud, the same `lakehouse.gold` steps would be submitted to a
cluster instead.

## MinIO and a JDBC catalog

S3 semantics without an AWS bill, and a catalog that is just a Postgres
database. Moving to S3 and Glue is a change to `conf/spark-defaults.conf`;
no job code refers to either.

## A static dashboard

The export writes one JSON file and the page is plain HTML, CSS and SVG with
no build step and no chart library. It keeps working when the pipeline is
stopped, and any static server can host it.

## Local development without Kafka

`devtools/wal_capture.py` reads a `test_decoding` slot and writes records in
Debezium's envelope, so the Spark code runs and is tested on real WAL changes
on a laptop. The end-to-end test compares the result with the source tables
row by row. What it cannot cover is the Iceberg-specific part: the `MERGE`
statement, the snapshot-summary check and maintenance procedures. Those are
exercised by the compose stack.
