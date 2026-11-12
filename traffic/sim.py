"""Booking traffic for the source database.

A deterministic stand-in for a busy booking system. Each tick advances a
simulated clock by a few minutes and, in one transaction, books seats,
confirms or expires holds, cancels some bookings, moves fares with demand,
delays or cancels flights and purges old expired holds. That covers the
change types a CDC pipeline has to handle: inserts, updates of tracked and
untracked columns, deletes, and a column added halfway through.

The simulated clock lives in the database, so `run` can be stopped and
started again and carries on where it left off.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg

ROUTES = [
    ("DEL", "BOM", 130),
    ("DEL", "BLR", 165),
    ("BOM", "BLR", 100),
    ("DEL", "HYD", 135),
    ("BOM", "GOI", 70),
    ("BLR", "MAA", 60),
    ("DEL", "CCU", 130),
    ("HYD", "MAA", 75),
]
# Popular routes sell faster. Same order as ROUTES.
ROUTE_WEIGHT = [9, 7, 7, 5, 4, 3, 4, 2]
DEPARTURES = ["06:10", "13:45", "20:30"]
START = datetime(2026, 11, 1, tzinfo=timezone.utc)
TICK = timedelta(minutes=5)
HORIZON_DAYS = 7
SEATS = {"economy": 108, "business": 12}
CHANNELS = (["web"] * 55) + (["app"] * 35) + (["agent"] * 10)


def base_fare_paise(minutes: int, cabin: str) -> int:
    economy = (2500 + minutes * 30) * 100
    return economy * 3 if cabin == "business" else economy


def fare_for(base: int, sold: int, total: int, hours_left: float) -> int:
    """Price steps up as the cabin fills and again in the last day."""
    load = sold / total
    step = 1.0 if load < 0.5 else 1.15 if load < 0.75 else 1.35 if load < 0.9 else 1.6
    if hours_left < 24:
        step *= 1.2
    return int(round(base * step / 100) * 100)


@dataclass
class Config:
    seed: int = 7
    bookings_per_tick: float = 6.0
    confirm_rate: float = 0.78
    cancel_rate: float = 0.03
    delay_rate: float = 0.03
    flight_cancel_rate: float = 0.004
    # The bookings table gains a `channel` column at this tick, the way a
    # product team ships a column without asking the data team.
    schema_change_tick: int = 288


SCHEMA_STATE = """
CREATE TABLE IF NOT EXISTS _sim_state (key text PRIMARY KEY, value text NOT NULL)
"""


class Simulator:
    def __init__(self, conn: psycopg.Connection, config: Config | None = None):
        self.conn = conn
        self.cfg = config or Config()

    # ------------------------------------------------------------------ state

    def _get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM _sim_state WHERE key = %s", [key]).fetchone()
        return row[0] if row else None

    def _set(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO _sim_state VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [key, value],
        )

    def tick_number(self) -> int:
        return int(self._get("tick") or 0)

    def now(self) -> datetime:
        return START + TICK * self.tick_number()

    def has_channel(self) -> bool:
        return (
            self.conn.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name = 'bookings' AND column_name = 'channel'"
            ).fetchone()
            is not None
        )

    # ------------------------------------------------------------------ setup

    def setup(self) -> int:
        with self.conn.transaction():
            self.conn.execute(SCHEMA_STATE)
            if self._get("tick") is not None:
                return 0
            self._set("tick", "0")
            created = sum(self._add_day(START + timedelta(days=d)) for d in range(HORIZON_DAYS))
        return created

    def _add_day(self, day: datetime) -> int:
        """Schedules one day of departures. Flight ids come from the route and
        date, so the same day is never added twice."""
        n = 0
        for r, (a, b, minutes) in enumerate(ROUTES):
            for direction, (origin, dest) in enumerate([(a, b), (b, a)]):
                for slot, hhmm in enumerate(DEPARTURES):
                    h, m = map(int, hhmm.split(":"))
                    departs = day.replace(hour=h, minute=m) - timedelta(hours=5, minutes=30)  # IST to UTC
                    fid = uuid.uuid5(uuid.NAMESPACE_URL, f"{origin}-{dest}-{departs.isoformat()}")
                    number = f"SL {100 + r * 6 + direction * 3 + slot}"
                    res = self.conn.execute(
                        """INSERT INTO flights (id, number, origin, destination, departs_at, arrives_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                        [fid, number, origin, dest, departs, departs + timedelta(minutes=minutes), self.now()],
                    )
                    if res.rowcount:
                        for cabin, seats in SEATS.items():
                            self.conn.execute(
                                """INSERT INTO fares (flight_id, cabin, price_paise, seats_total, updated_at)
                                   VALUES (%s, %s, %s, %s, %s)""",
                                [fid, cabin, base_fare_paise(minutes, cabin), seats, self.now()],
                            )
                        n += 1
        return n

    # ------------------------------------------------------------------- tick

    def tick(self) -> dict[str, int]:
        """Advances the clock by one step and applies that step's changes."""
        counts = dict.fromkeys(
            ["booked", "confirmed", "expired", "cancelled", "repriced", "delayed", "flights_cancelled", "purged"], 0
        )
        with self.conn.transaction():
            t = self.tick_number() + 1
            self._set("tick", str(t))
            now = self.now()
            rng = random.Random(self.cfg.seed * 1_000_003 + t)

            if t == self.cfg.schema_change_tick and not self.has_channel():
                self.conn.execute("ALTER TABLE bookings ADD COLUMN channel text")
            if now.hour == 0 and now.minute == 0:
                self._add_day(now + timedelta(days=HORIZON_DAYS - 1))
            self.conn.execute(
                """UPDATE flights SET status = 'departed', updated_at = %s
                   WHERE departs_at <= %s AND status IN ('scheduled', 'delayed')""",
                [now, now],
            )

            touched: set[tuple[str, str]] = set()
            counts["confirmed"], counts["expired"] = self._resolve_holds(rng, now, touched)
            counts["booked"] = self._book(rng, now, touched)
            counts["cancelled"] = self._cancel_some(rng, now, touched)
            counts["delayed"], counts["flights_cancelled"] = self._disrupt(rng, now, touched)
            counts["repriced"] = self._reprice(now, touched)
            if now.hour % 12 == 0 and now.minute == 0:
                counts["purged"] = self.conn.execute(
                    "DELETE FROM bookings WHERE status = 'EXPIRED' AND updated_at < %s", [now - timedelta(hours=24)]
                ).rowcount
        return counts

    def _book(self, rng: random.Random, now: datetime, touched: set) -> int:
        flights = self.conn.execute(
            """SELECT f.id, f.origin, f.destination, f.departs_at FROM flights f
               WHERE f.status IN ('scheduled', 'delayed') AND f.departs_at > %s ORDER BY f.departs_at""",
            [now + timedelta(hours=1)],
        ).fetchall()
        if not flights:
            return 0
        weights = []
        for _, origin, dest, departs in flights:
            route = next(i for i, (a, b, _) in enumerate(ROUTES) if {a, b} == {origin, dest})
            days_out = max((departs - now).total_seconds() / 86400, 0.2)
            weights.append(ROUTE_WEIGHT[route] / days_out)  # nearer flights sell faster

        channel = self.has_channel()
        made = 0
        for _ in range(_poisson(rng, self.cfg.bookings_per_tick)):
            fid = rng.choices(flights, weights)[0][0]
            cabin = "business" if rng.random() < 0.12 else "economy"
            seats = rng.choices([1, 2, 3, 4], [55, 30, 10, 5])[0]
            row = self.conn.execute(
                "SELECT price_paise, seats_sold, seats_total FROM fares WHERE flight_id = %s AND cabin = %s FOR UPDATE",
                [fid, cabin],
            ).fetchone()
            price, sold, total = row
            if sold + seats > total:
                continue
            cols = "id, flight_id, cabin, seats, total_paise, status, created_at, updated_at"
            vals = [uuid.UUID(int=rng.getrandbits(128), version=4), fid, cabin, seats, price * seats, "HELD", now, now]
            if channel:
                cols += ", channel"
                vals.append(rng.choice(CHANNELS))
            self.conn.execute(f"INSERT INTO bookings ({cols}) VALUES ({', '.join(['%s'] * len(vals))})", vals)
            self._seats(fid, cabin, seats, now)
            touched.add((fid, cabin))
            made += 1
        return made

    def _resolve_holds(self, rng: random.Random, now: datetime, touched: set) -> tuple[int, int]:
        holds = self.conn.execute(
            "SELECT id, flight_id, cabin, seats FROM bookings WHERE status = 'HELD' AND created_at <= %s ORDER BY id",
            [now - TICK * 2],
        ).fetchall()
        confirmed = expired = 0
        for bid, fid, cabin, seats in holds:
            if rng.random() < self.cfg.confirm_rate:
                self._status(bid, "CONFIRMED", now)
                confirmed += 1
            else:
                self._status(bid, "EXPIRED", now)
                self._seats(fid, cabin, -seats, now)
                touched.add((fid, cabin))
                expired += 1
        return confirmed, expired

    def _cancel_some(self, rng: random.Random, now: datetime, touched: set) -> int:
        if rng.random() > self.cfg.cancel_rate * 10:
            return 0
        row = self.conn.execute(
            """SELECT b.id, b.flight_id, b.cabin, b.seats FROM bookings b JOIN flights f ON f.id = b.flight_id
               WHERE b.status = 'CONFIRMED' AND f.departs_at > %s ORDER BY b.id
               OFFSET %s LIMIT 1""",
            [now + timedelta(hours=3), rng.randrange(0, 50)],
        ).fetchone()
        if not row:
            return 0
        bid, fid, cabin, seats = row
        self._status(bid, "CANCELLED", now)
        self._seats(fid, cabin, -seats, now)
        touched.add((fid, cabin))
        return 1

    def _disrupt(self, rng: random.Random, now: datetime, touched: set) -> tuple[int, int]:
        delayed = cancelled = 0
        upcoming = self.conn.execute(
            """SELECT id FROM flights WHERE status IN ('scheduled', 'delayed')
               AND departs_at BETWEEN %s AND %s ORDER BY departs_at""",
            [now + timedelta(hours=1), now + timedelta(hours=12)],
        ).fetchall()
        if upcoming and rng.random() < self.cfg.delay_rate:
            fid = rng.choice(upcoming)[0]
            by = timedelta(minutes=rng.choice([30, 45, 60, 90, 120]))
            self.conn.execute(
                """UPDATE flights SET status = 'delayed', departs_at = departs_at + %s,
                   arrives_at = arrives_at + %s, updated_at = %s WHERE id = %s""",
                [by, by, now, fid],
            )
            delayed = 1
        if upcoming and rng.random() < self.cfg.flight_cancel_rate:
            fid = rng.choice(upcoming)[0]
            self.conn.execute("UPDATE flights SET status = 'cancelled', updated_at = %s WHERE id = %s", [now, fid])
            for bid, cabin, seats in self.conn.execute(
                "SELECT id, cabin, seats FROM bookings WHERE flight_id = %s AND status IN ('HELD', 'CONFIRMED')", [fid]
            ).fetchall():
                self._status(bid, "CANCELLED", now)
                self._seats(fid, cabin, -seats, now)
                touched.add((fid, cabin))
            cancelled = 1
        return delayed, cancelled

    def _reprice(self, now: datetime, touched: set) -> int:
        changed = 0
        for fid, cabin in sorted(touched):
            row = self.conn.execute(
                """SELECT fa.price_paise, fa.seats_sold, fa.seats_total, f.departs_at, f.origin, f.destination
                   FROM fares fa JOIN flights f ON f.id = fa.flight_id
                   WHERE fa.flight_id = %s AND fa.cabin = %s""",
                [fid, cabin],
            ).fetchone()
            price, sold, total, departs, origin, dest = row
            minutes = next(m for a, b, m in ROUTES if {a, b} == {origin, dest})
            target = fare_for(base_fare_paise(minutes, cabin), sold, total, (departs - now).total_seconds() / 3600)
            if target != price:
                self.conn.execute(
                    "UPDATE fares SET price_paise = %s, updated_at = %s WHERE flight_id = %s AND cabin = %s",
                    [target, now, fid, cabin],
                )
                changed += 1
        return changed

    def _status(self, bid, status: str, now: datetime) -> None:
        self.conn.execute("UPDATE bookings SET status = %s, updated_at = %s WHERE id = %s", [status, now, bid])

    def _seats(self, fid, cabin: str, delta: int, now: datetime) -> None:
        self.conn.execute(
            "UPDATE fares SET seats_sold = seats_sold + %s, updated_at = %s WHERE flight_id = %s AND cabin = %s",
            [delta, now, fid, cabin],
        )


def _poisson(rng: random.Random, lam: float) -> int:
    """Knuth's method; fine for the small rates used here."""
    limit, k, p = pow(2.718281828459045, -lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1
