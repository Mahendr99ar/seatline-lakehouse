from conftest import booking, fare

from lakehouse import cdc, silver
from lakehouse.contracts import BOOKINGS, FARES


def test_bronze_drops_tombstones_and_other_topics(spark, events):
    events.add("fares", "c", after=fare(640000)).tombstone("fares")
    events.add("fares", "c", after=fare(640000), topic="__debezium-heartbeat.src")
    bronze = cdc.to_bronze(events.frame(spark)).collect()
    assert len(bronze) == 1
    assert bronze[0].source_table == "fares" and bronze[0].op == "c"


def test_change_rows_are_typed_and_deletes_keep_the_old_row(spark, events):
    events.add("bookings", "c", after=booking("b1", "HELD", "2026-11-01T00:05:00.123456Z"))
    events.add("bookings", "d", before=booking("b1", "EXPIRED", "2026-11-01T00:15:00Z"))
    rows = cdc.change_rows(cdc.to_bronze(events.frame(spark)), BOOKINGS).orderBy("_lsn").collect()
    assert [r._op for r in rows] == ["c", "d"]
    assert rows[0].updated_at.isoformat() == "2026-11-01T00:05:00.123456"
    assert rows[1]._deleted and rows[1].status == "EXPIRED"


def test_unknown_columns_go_to_extra(spark, events):
    events.add("bookings", "c", after=booking("b1", "HELD", "2026-11-01T00:05:00Z", channel="app"))
    events.add("bookings", "c", after=booking("b2", "HELD", "2026-11-01T00:05:00Z"))
    rows = {r.id: r for r in cdc.change_rows(cdc.to_bronze(events.frame(spark)), BOOKINGS).collect()}
    assert rows["b1"]._extra == {"channel": "app"}
    assert rows["b2"]._extra is None


def test_latest_wins_by_lsn_not_by_arrival(spark, events):
    # The update with the higher LSN arrives first; the stale one must lose.
    events.add("fares", "u", after=fare(768000, sold=60), lsn=2000)
    events.add("fares", "u", after=fare(640000, sold=50), lsn=1500)
    changes = cdc.change_rows(cdc.to_bronze(events.frame(spark)), FARES)
    [row] = cdc.latest_per_key(changes, FARES).collect()
    assert row.price_paise == 768000 and row._lsn == 2000


def test_a_late_update_does_not_bring_a_deleted_row_back(spark, events):
    events.add("bookings", "c", after=booking("b1", "HELD", "2026-11-01T00:05:00Z"), lsn=100)
    events.add("bookings", "d", before=booking("b1", "EXPIRED", "2026-11-02T00:00:00Z"), lsn=300)
    events.add("bookings", "u", after=booking("b1", "EXPIRED", "2026-11-01T00:15:00Z"), lsn=200)
    changes = cdc.change_rows(cdc.to_bronze(events.frame(spark)), BOOKINGS)
    [row] = silver.fold(changes, BOOKINGS).collect()
    assert row._deleted and row._lsn == 300


def test_merge_only_applies_newer_changes():
    sql = silver.merge_sql(FARES, "incoming")
    assert "ON t.flight_id = s.flight_id AND t.cabin = s.cabin" in sql
    assert "WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE" in sql
    assert "DELETE" not in sql  # deletes are soft
