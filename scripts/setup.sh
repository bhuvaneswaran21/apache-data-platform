#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "========================================"
echo "  Big Data Platform — Setup"
echo "========================================"


echo ""
echo ">>> 1/5  Downloading Flink JARs..."
bash scripts/download_jars.sh


echo ""
echo ">>> 2/5  Starting Docker Compose services..."
docker compose up -d


echo ""
echo ">>> 3/5  Waiting for Kafka and MinIO to be ready..."
for i in $(seq 1 30); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list &>/dev/null; then
    echo "  Kafka ready."
    break
  fi
  echo "  Waiting for Kafka ($i/30)..."
  sleep 5
done

for i in $(seq 1 20); do
  if docker compose exec -T minio curl -sf http://localhost:9000/minio/health/live &>/dev/null; then
    echo "  MinIO ready."
    break
  fi
  echo "  Waiting for MinIO ($i/20)..."
  sleep 5
done


echo ""
echo ">>> 4/5  Creating Kafka topic 'user_events'..."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --create --if-not-exists \
  --topic user_events \
  --partitions 3 \
  --replication-factor 1
echo "  Topic 'user_events' ready."

echo ""
echo ">>> 5/5  Verifying MinIO buckets..."
docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin123 2>/dev/null || true
for bucket in warehouse flink spark nifi; do
  docker compose exec -T minio mc mb --ignore-existing "local/$bucket" 2>/dev/null || true
  echo "  Bucket '$bucket' ✓"
done

echo ""
echo "========================================"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "    1. Start the Kafka producer:"
echo "       pip install kafka-python && python scripts/kafka_producer.py"
echo ""
echo "    2. Open the Flink SQL client:"
echo "       bash scripts/run_flink_sql.sh"
echo ""
echo "    3. Inside the SQL client, run in order:"
echo "       SOURCE '/opt/flink/sql/01_create_catalog.sql';"
echo "       SOURCE '/opt/flink/sql/02_create_database.sql';"
echo "       SOURCE '/opt/flink/sql/03_create_tables.sql';"
echo "       SOURCE '/opt/flink/sql/04_run_pipeline.sql';"
echo ""
echo "  UI endpoints:"
echo "    Flink      http://localhost:8084"
echo "    MinIO      http://localhost:9001  (minioadmin / minioadmin123)"
echo "    Kafka UI   http://localhost:9021"
echo "    Nessie     http://localhost:19120/api/v1/trees"
echo "    Trino      http://localhost:8085"
echo "    Airflow    http://localhost:8082  (admin / admin123)"
echo "    Superset   http://localhost:8088  (admin / admin123)"
echo "    Grafana    http://localhost:3000  (admin / grafana123)"
echo "========================================"
