#!/bin/bash
# Creates all databases needed by the platform on first PostgreSQL start.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE hive_metastore;
    CREATE DATABASE superset;
    CREATE DATABASE ranger;
    CREATE DATABASE nessie;
EOSQL

echo "All platform databases created."
