#!/bin/sh
# Runs once, on an empty data volume. One Postgres serves two local metadata stores, each
# with its own login role and database (least privilege): the Iceberg REST catalog and
# Airflow. Passwords arrive as environment variables and are passed to psql as variables,
# so they never appear in this file or in the server log.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v catalog_pw="$CATALOG_DB_PASSWORD" -v airflow_pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE iceberg_catalog LOGIN PASSWORD :'catalog_pw';
CREATE DATABASE iceberg_catalog OWNER iceberg_catalog;
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;
SQL
