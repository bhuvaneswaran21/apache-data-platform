#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker compose cp "$ROOT/flink/sql" flink-jobmanager:/opt/flink/sql 2>/dev/null || \
  docker cp "$ROOT/flink/sql" "$(docker compose ps -q flink-jobmanager)":/opt/flink/sql

echo "SQL files copied to /opt/flink/sql inside the container."
echo "Starting Flink SQL client..."
echo ""
echo "  Run these in order:"
echo "    Flink SQL> SOURCE '/opt/flink/sql/01_create_catalog.sql';"
echo "    Flink SQL> SOURCE '/opt/flink/sql/02_create_database.sql';"
echo "    Flink SQL> SOURCE '/opt/flink/sql/03_create_tables.sql';"
echo "    Flink SQL> SOURCE '/opt/flink/sql/04_run_pipeline.sql';"
echo ""

docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh
