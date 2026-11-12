from devtools.wal_capture import iso_timestamp, lsn_to_int, parse_change


def test_insert():
    table, op, before, after = parse_change(
        "table public.fares: INSERT: flight_id[uuid]:'f1' cabin[text]:'economy' price_paise[integer]:640000 "
        "seats_total[integer]:108 seats_sold[integer]:0 updated_at[timestamp with time zone]:'2026-11-01 00:00:00+00'"
    )
    assert (table, op, before) == ("fares", "c", None)
    assert after["price_paise"] == 640000 and after["updated_at"] == "2026-11-01T00:00:00Z"


def test_update_with_full_replica_identity_has_both_images():
    _, op, before, after = parse_change(
        "table public.bookings: UPDATE: old-key: id[uuid]:'b1' status[text]:'HELD' "
        "new-tuple: id[uuid]:'b1' status[text]:'CONFIRMED'"
    )
    assert op == "u" and before["status"] == "HELD" and after["status"] == "CONFIRMED"


def test_delete_and_quoted_text():
    _, op, before, after = parse_change("table public.flights: DELETE: id[uuid]:'f1' number[text]:'SL ''100'''")
    assert op == "d" and after is None and before["number"] == "SL '100'"


def test_other_tables_and_nulls():
    assert parse_change("table public._sim_state: UPDATE: key[text]:'tick' value[text]:'3'") is None
    _, _, _, after = parse_change("table public.bookings: INSERT: id[uuid]:'b1' channel[text]:null")
    assert after["channel"] is None


def test_lsn_and_time_formats():
    assert lsn_to_int("0/8810A40") == 0x8810A40
    assert lsn_to_int("1/0") == 1 << 32
    assert iso_timestamp("2026-11-01 00:05:00.25+00") == "2026-11-01T00:05:00.25Z"
