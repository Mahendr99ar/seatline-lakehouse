#!/bin/sh
# The catalog Postgres also holds Airflow's metadata, in its own database.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
CREATE DATABASE airflow;
SQL
