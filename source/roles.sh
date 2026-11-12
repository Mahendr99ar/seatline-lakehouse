#!/bin/sh
# Runs once when the source database is created (docker-entrypoint-initdb.d).
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD '${DEBEZIUM_PASSWORD}';
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
GRANT UPDATE ON _heartbeat TO debezium;
SQL
