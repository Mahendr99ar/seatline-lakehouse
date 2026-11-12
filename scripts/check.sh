#!/bin/sh
# Compares the source tables with the lake's silver tables. Run from the
# repository root on the machine running docker compose. The numbers will
# differ by whatever changed in the last trigger interval or two.
set -eu

echo "== connector"
docker compose exec -T connect curl -s localhost:8083/connectors/src/status
echo

echo "== source rows"
docker compose exec -T source-db psql -U airline -d airline -tA -c \
  "SELECT 'flights', count(*) FROM flights UNION ALL
   SELECT 'fares', count(*) FROM fares UNION ALL
   SELECT 'bookings', count(*) FROM bookings"

echo "== lake rows (silver, not deleted)"
docker compose exec -T airflow python - <<'PY'
from lakehouse import lake
spark = lake.session("check")
for t in ("flights", "fares", "bookings"):
    n = spark.table(f"lake.silver.{t}").where("NOT _deleted").count()
    print(f"{t}|{n}")
print("bronze|" + str(spark.table(lake.BRONZE).count()))
spark.stop()
PY
