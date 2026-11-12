-- The operational database Seatline Lakehouse reads from. It has the shape of
-- Seatline's flights and bookings, plus a fares table so prices can move.
-- Seatline Lakehouse never writes here; it only reads the write-ahead log.

CREATE TABLE flights (
    id          uuid PRIMARY KEY,
    number      text        NOT NULL,
    origin      char(3)     NOT NULL,
    destination char(3)     NOT NULL,
    departs_at  timestamptz NOT NULL,
    arrives_at  timestamptz NOT NULL,
    status      text        NOT NULL DEFAULT 'scheduled'
                CHECK (status IN ('scheduled', 'delayed', 'cancelled', 'departed')),
    updated_at  timestamptz NOT NULL
);

CREATE TABLE fares (
    flight_id   uuid    NOT NULL REFERENCES flights,
    cabin       text    NOT NULL CHECK (cabin IN ('economy', 'business')),
    price_paise integer NOT NULL CHECK (price_paise > 0),
    seats_total integer NOT NULL,
    seats_sold  integer NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL,
    PRIMARY KEY (flight_id, cabin)
);

CREATE TABLE bookings (
    id          uuid PRIMARY KEY,
    flight_id   uuid        NOT NULL REFERENCES flights,
    cabin       text        NOT NULL,
    seats       integer     NOT NULL,
    total_paise integer     NOT NULL,
    status      text        NOT NULL CHECK (status IN ('HELD', 'CONFIRMED', 'EXPIRED', 'CANCELLED')),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

-- FULL makes updates and deletes carry the whole old row, not just the key.
-- Without it a delete event would only say which id disappeared.
ALTER TABLE flights  REPLICA IDENTITY FULL;
ALTER TABLE fares    REPLICA IDENTITY FULL;
ALTER TABLE bookings REPLICA IDENTITY FULL;

-- Debezium reads this publication instead of creating its own, so its
-- database user needs REPLICATION and SELECT, not ownership of the tables.
CREATE PUBLICATION lakehouse FOR TABLE flights, fares, bookings;

-- Debezium writes here every few seconds (heartbeat.action.query). When no
-- watched table changes for a while, this keeps the replication slot moving,
-- so Postgres can recycle WAL instead of keeping it for the slot.
CREATE TABLE _heartbeat (id int PRIMARY KEY, at timestamptz NOT NULL);
INSERT INTO _heartbeat VALUES (1, now());
