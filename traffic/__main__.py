"""python -m traffic setup | run [--ticks N] [--sleep S]"""

import argparse
import os
import sys
import time

import psycopg

from .sim import Config, Simulator


def main() -> None:
    p = argparse.ArgumentParser(prog="traffic")
    p.add_argument("command", choices=["setup", "run"])
    p.add_argument("--dsn", default=os.environ.get("SOURCE_DSN", "postgresql://postgres@localhost/airline"))
    p.add_argument("--ticks", type=int, default=0, help="stop after this many ticks (0 = run until stopped)")
    p.add_argument("--sleep", type=float, default=float(os.environ.get("TICK_SLEEP", "0.5")))
    p.add_argument("--seed", type=int, default=int(os.environ.get("SEED", "7")))
    args = p.parse_args()

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        sim = Simulator(conn, Config(seed=args.seed))
        created = sim.setup()
        if created:
            print(f"scheduled {created} flights from {sim.now():%Y-%m-%d}", flush=True)
        if args.command == "setup":
            return
        done = 0
        while not args.ticks or done < args.ticks:
            counts = sim.tick()
            done += 1
            if sim.tick_number() % 12 == 0:
                summary = " ".join(f"{k}={v}" for k, v in counts.items() if v)
                print(f"{sim.now():%Y-%m-%d %H:%M} {summary}", flush=True)
            time.sleep(args.sleep)


if __name__ == "__main__":
    sys.exit(main())
