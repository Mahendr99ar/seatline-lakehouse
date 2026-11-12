"""What each source table is expected to look like.

A contract lists the columns the lakehouse knows about. Columns the source
adds later are not dropped: they land in `_extra` and the quality checks
report them, so a schema change shows up as a visible warning instead of a
silent loss or a broken job. Adding the column here (and to the silver
table) makes it a first-class column from the next batch on.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Contract:
    table: str
    key: tuple[str, ...]
    columns: dict[str, str]  # name -> Spark SQL type
    # Business time of a change. SCD2 versions are ordered by this, not by
    # when the change happened to reach the lake.
    event_time: str = "updated_at"
    # Columns whose changes start a new SCD2 version. Changes to anything
    # else (seats_sold on every booking, say) update the current version.
    tracked: tuple[str, ...] = field(default_factory=tuple)

    def ddl(self) -> str:
        return ", ".join(f"{name} {typ}" for name, typ in self.columns.items())


FLIGHTS = Contract(
    table="flights",
    key=("id",),
    columns={
        "id": "STRING",
        "number": "STRING",
        "origin": "STRING",
        "destination": "STRING",
        "departs_at": "TIMESTAMP",
        "arrives_at": "TIMESTAMP",
        "status": "STRING",
        "updated_at": "TIMESTAMP",
    },
    tracked=("departs_at", "arrives_at", "status"),
)

FARES = Contract(
    table="fares",
    key=("flight_id", "cabin"),
    columns={
        "flight_id": "STRING",
        "cabin": "STRING",
        "price_paise": "INT",
        "seats_total": "INT",
        "seats_sold": "INT",
        "updated_at": "TIMESTAMP",
    },
    tracked=("price_paise",),
)

BOOKINGS = Contract(
    table="bookings",
    key=("id",),
    columns={
        "id": "STRING",
        "flight_id": "STRING",
        "cabin": "STRING",
        "seats": "INT",
        "total_paise": "INT",
        "status": "STRING",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    tracked=("status",),
)

CONTRACTS = {c.table: c for c in (FLIGHTS, FARES, BOOKINGS)}

# Bookkeeping columns every silver table carries next to the contract columns.
META_COLUMNS = {
    "_lsn": "BIGINT",
    "_op": "STRING",
    "_deleted": "BOOLEAN",
    "_source_ts": "TIMESTAMP",
    "_ingested_at": "TIMESTAMP",
    "_extra": "MAP<STRING, STRING>",
}
