from conftest import booking, fare, flight

from lakehouse import cdc, marts, quality, scd2, silver
from lakehouse.contracts import CONTRACTS, FARES


def _layers(spark, events):
    bronze = cdc.to_bronze(events.frame(spark))
    changes = {t: cdc.change_rows(bronze, c) for t, c in CONTRACTS.items()}
    current = {t: silver.fold(changes[t], c) for t, c in CONTRACTS.items()}
    history = {t: scd2.history(changes[t], c) for t, c in CONTRACTS.items()}
    return bronze, changes, current, history


def test_capacity_and_drift_checks(spark, events):
    events.add("flights", "c", after=flight())
    events.add("fares", "c", after=fare(640000, sold=109))
    events.add("bookings", "c", after=booking("b1", "HELD", "2026-11-01T00:05:00Z", channel="web"))
    bronze, _, current, _ = _layers(spark, events)
    results = {(r.check, r.table): r for r in quality.run_all(bronze, current, CONTRACTS, max_lag_minutes=None)}
    assert results[("fares_within_capacity", "fares")].status == quality.FAIL
    drift = results[("schema_drift", "bookings")]
    assert drift.status == quality.WARN and "channel" in drift.detail
    assert results[("duplicate_keys", "bookings")].status == quality.PASS


def test_orphans_only_count_after_the_grace_period(spark, events):
    events.add("flights", "c", after=flight("f1"))
    old = booking("b1", "HELD", "2026-11-01T00:05:00Z", flight="ghost", created="2026-11-01T00:05:00Z")
    new = booking("b2", "HELD", "2026-11-01T01:00:00Z", flight="ghost", created="2026-11-01T01:00:00Z")
    events.add("bookings", "c", after=old).add("bookings", "c", after=new)
    _, _, current, _ = _layers(spark, events)
    r = quality.orphan_bookings(current["bookings"], current["flights"], grace_minutes=10)
    assert r.status == quality.WARN and r.bad_rows == 1


def test_duplicate_keys_are_caught(spark):
    df = spark.createDataFrame([("f1", "economy"), ("f1", "economy")], "flight_id STRING, cabin STRING")
    assert quality.duplicate_keys(df, FARES).bad_rows == 1


def test_funnel_counts_holds_that_were_purged(spark, events):
    # b2 expired and was then deleted from the source. Current state has no
    # trace of it; the funnel still has to count it as an expiry.
    events.add("bookings", "c", after=booking("b1", "HELD", "2026-11-01T00:05:00Z"))
    events.add("bookings", "u", after=booking("b1", "CONFIRMED", "2026-11-01T00:15:00Z"))
    events.add("bookings", "c", after=booking("b2", "HELD", "2026-11-01T00:05:00Z"))
    events.add("bookings", "u", after=booking("b2", "EXPIRED", "2026-11-01T00:15:00Z"))
    events.add("bookings", "d", before=booking("b2", "EXPIRED", "2026-11-02T00:20:00Z"))
    events.add("bookings", "c", after=booking("b3", "HELD", "2026-11-01T00:05:00Z"))
    events.add("bookings", "u", after=booking("b3", "CONFIRMED", "2026-11-01T00:15:00Z"))
    events.add("bookings", "u", after=booking("b3", "CANCELLED", "2026-11-01T05:00:00Z"))
    _, _, _, history = _layers(spark, events)
    [day] = marts.booking_funnel(history["bookings"]).collect()
    assert (day.held, day.confirmed, day.expired, day.cancelled_after_confirm) == (3, 2, 1, 1)


def test_delays_measure_against_the_first_published_time(spark, events):
    events.add("flights", "c", after=flight("f1"))
    events.add(
        "flights",
        "u",
        after=flight("f1", "2026-11-03T01:40:00Z", "2026-11-03T03:50:00Z", "delayed", "2026-11-02T20:00:00Z"),
    )
    events.add(
        "flights",
        "u",
        after=flight("f1", "2026-11-03T01:40:00Z", "2026-11-03T03:50:00Z", "departed", "2026-11-03T01:40:00Z"),
    )
    _, _, _, history = _layers(spark, events)
    [row] = marts.delays(history["flights"]).collect()
    assert (row.flights, row.delayed, row.avg_delay_min) == (1, 1, 60.0)


def test_route_daily_counts_confirmed_bookings_only(spark, events):
    events.add("flights", "c", after=flight("f1"))
    events.add("bookings", "c", after=booking("b1", "CONFIRMED", "2026-11-01T00:15:00Z", seats=2, total=1280000))
    events.add("bookings", "c", after=booking("b2", "HELD", "2026-11-01T00:15:00Z"))
    _, _, current, _ = _layers(spark, events)
    [row] = marts.route_daily(current["bookings"], current["flights"]).collect()
    assert (row.route, row.bookings, row.seats, row.revenue_paise) == ("DEL-BOM", 1, 2, 1280000)
    assert str(row.day) == "2026-11-01"
