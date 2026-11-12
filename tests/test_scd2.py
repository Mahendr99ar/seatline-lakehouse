from conftest import booking, fare

from lakehouse import cdc, scd2
from lakehouse.contracts import BOOKINGS, FARES


def _history(spark, events, contract):
    changes = cdc.change_rows(cdc.to_bronze(events.frame(spark)), contract)
    return scd2.history(changes, contract)


def test_tracked_change_opens_a_version_untracked_does_not(spark, events):
    events.add("fares", "c", after=fare(640000, sold=0, at="2026-11-01T00:00:00Z"))
    events.add("fares", "u", after=fare(640000, sold=4, at="2026-11-01T01:00:00Z"))
    events.add("fares", "u", after=fare(736000, sold=60, at="2026-11-01T02:00:00Z"))
    events.add("fares", "u", after=fare(736000, sold=61, at="2026-11-01T03:00:00Z"))
    v = _history(spark, events, FARES).orderBy("valid_from").collect()
    assert [(r.price_paise, r.seats_sold, r.changes) for r in v] == [(640000, 4, 2), (736000, 61, 2)]
    assert v[0].valid_to == v[1].valid_from
    assert [r.is_current for r in v] == [False, True]


def test_delete_closes_the_open_version(spark, events):
    events.add("bookings", "c", after=booking("b1", "HELD", "2026-11-01T00:05:00Z"))
    events.add("bookings", "u", after=booking("b1", "EXPIRED", "2026-11-01T00:15:00Z"))
    events.add("bookings", "d", before=booking("b1", "EXPIRED", "2026-11-02T00:15:00Z"))
    v = _history(spark, events, BOOKINGS).orderBy("valid_from").collect()
    assert [r.status for r in v] == ["HELD", "EXPIRED"]
    assert v[1].valid_to.isoformat() == "2026-11-02T00:15:00"
    assert not any(r.is_current for r in v)


def test_history_does_not_depend_on_arrival_order(spark, events):
    rows = [
        fare(640000, at="2026-11-01T00:00:00Z"),
        fare(736000, sold=60, at="2026-11-01T02:00:00Z"),
        fare(640000, sold=50, at="2026-11-01T03:00:00Z"),
    ]
    for lsn, row in zip([10, 20, 30], rows, strict=True):
        events.add("fares", "u", after=row, lsn=lsn)
    in_order = _history(spark, events, FARES).drop("valid_to").orderBy("valid_from").collect()

    events.records.reverse()
    reversed_ = _history(spark, events, FARES).drop("valid_to").orderBy("valid_from").collect()
    assert in_order == reversed_
    assert [r.price_paise for r in in_order] == [640000, 736000, 640000]


def test_as_of_returns_the_price_at_that_moment(spark, events):
    events.add("fares", "c", after=fare(640000, at="2026-11-01T00:00:00Z"))
    events.add("fares", "u", after=fare(736000, sold=60, at="2026-11-01T02:00:00Z"))
    h = _history(spark, events, FARES)
    assert scd2.as_of(h, "2026-11-01 01:59:59").first().price_paise == 640000
    assert scd2.as_of(h, "2026-11-01 02:00:00").first().price_paise == 736000
    assert scd2.as_of(h, "2026-10-31 23:00:00").count() == 0
