"""Silver: the current state of each source table, kept up to date by MERGE.

Two rules make the merge safe to replay and safe against reordering:

1. A change is applied only if its LSN is newer than the row's `_lsn`.
   Replaying a batch after a crash, or a stale event arriving late, changes
   nothing.
2. Deletes are soft. The row stays with `_deleted = true` and its LSN, so
   an older update that arrives after the delete is recognised as older and
   ignored, instead of bringing the row back.
"""

from __future__ import annotations

from .contracts import META_COLUMNS, Contract


def table_name(contract: Contract, catalog: str = "lake") -> str:
    return f"{catalog}.silver.{contract.table}"


def create_sql(contract: Contract, catalog: str = "lake") -> str:
    cols = {**contract.columns, **META_COLUMNS}
    body = ",\n  ".join(f"{n} {t}" for n, t in cols.items())
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name(contract, catalog)} (\n  {body}\n) USING iceberg\n"
        "TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read', "
        "'write.update.mode' = 'merge-on-read', 'write.delete.mode' = 'merge-on-read')"
    )


def merge_sql(contract: Contract, source_view: str, catalog: str = "lake") -> str:
    """MERGE for a view holding at most one change per key (see
    cdc.latest_per_key)."""
    on = " AND ".join(f"t.{k} = s.{k}" for k in contract.key)
    cols = [*contract.columns, *META_COLUMNS]
    assign = ", ".join(f"t.{c} = s.{c}" for c in cols)
    names = ", ".join(cols)
    values = ", ".join(f"s.{c}" for c in cols)
    return f"""
MERGE INTO {table_name(contract, catalog)} t
USING {source_view} s
ON {on}
WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {assign}
WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})
""".strip()


def fold(changes, contract: Contract):
    """What silver holds after applying every change in `changes`, computed
    without MERGE: for a key, the newest change by LSN wins, deletes
    included. Used where there is no Iceberg (the tests and the local
    runner); on the lake the MERGE above produces the same rows."""
    from .cdc import latest_per_key

    return latest_per_key(changes, contract)
