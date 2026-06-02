#!/usr/bin/env bash
# End-to-end demo: starts the platform, produces events, runs the Flink pipeline,
# and prints live query results from Trino.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[demo]${NC} $*"; }
info() { echo -e "${CYAN}[info]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

wait_for() {
  local name="$1"; local cmd="$2"; local max="${3:-30}"
  for i in $(seq 1 "$max"); do
    if eval "$cmd" &>/dev/null; then log "$name ready."; return 0; fi
    echo -ne "  Waiting for $name ($i/$max)...\r"
    sleep 5
  done
  echo ""; warn "$name not ready after $((max*5))s — continuing anyway."
}

# ─── 1. Start platform ──────────────────────────────────────────────
log "Starting all services..."
docker compose up -d
sleep 10

# ─── 2. Wait for critical services ─────────────────────────────────
wait_for "Kafka" \
  "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list"
wait_for "Nessie" \
  "curl -sf http://localhost:19120/api/v1/trees"
wait_for "Flink jobmanager" \
  "curl -sf http://localhost:8084/jobs"

# ─── 3. Create Kafka topic ──────────────────────────────────────────
log "Creating Kafka topic 'user_events'..."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --create --if-not-exists \
  --topic user_events \
  --partitions 3 \
  --replication-factor 1 || true

# ─── 4. Copy SQL files into the Flink container ────────────────────
log "Copying Flink SQL files into jobmanager..."
docker compose cp flink/sql flink-jobmanager:/opt/flink/sql

# ─── 5. Start Kafka producer in background ─────────────────────────
log "Starting Kafka event producer (background)..."
pip install -q kafka-python 2>/dev/null || warn "pip not available — skipping producer install."
python scripts/kafka_producer.py &
PRODUCER_PID=$!
trap "kill $PRODUCER_PID 2>/dev/null || true" EXIT
log "Producer PID: $PRODUCER_PID"
sleep 5

# ─── 6. Submit Flink SQL pipeline ──────────────────────────────────
log "Submitting Flink SQL pipeline..."
docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh \
  --file /opt/flink/sql/01_create_catalog.sql \
  --file /opt/flink/sql/02_create_database.sql \
  --file /opt/flink/sql/03_create_tables.sql &

FLINK_SQL_PID=$!
sleep 15

log "Starting streaming INSERT (background job in Flink)..."
docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh \
  --file /opt/flink/sql/04_run_pipeline.sql &
sleep 20

# ─── 7. Show live results ──────────────────────────────────────────
log "Waiting 90s for Flink checkpoint + Iceberg commit..."
sleep 90

echo ""
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  LIVE QUERY RESULTS (via Flink SQL)                  ${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"

docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh <<'FSQL'
SET 'sql-client.execution.result-mode' = 'tableau';
USE CATALOG nessie_catalog;
USE events;
SELECT event_type, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS revenue
FROM user_events
GROUP BY event_type
ORDER BY cnt DESC;
FSQL

echo ""
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  FEDERATED QUERY (Trino: Iceberg + PostgreSQL)       ${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"

wait_for "Trino" "curl -sf http://localhost:8085/v1/info"
docker compose exec -T trino trino --execute "
SELECT e.product, p.category, COUNT(*) AS orders, ROUND(SUM(e.amount),2) AS revenue
FROM iceberg.events.user_events e
JOIN postgresql.public.products p ON e.product = p.name
WHERE e.event_type = 'purchase'
GROUP BY e.product, p.category
ORDER BY revenue DESC
LIMIT 5;" 2>/dev/null || warn "Trino query skipped — Trino not yet ready."

echo ""
log "Demo complete!"
echo ""
info "UIs:"
info "  Flink   → http://localhost:8084"
info "  MinIO   → http://localhost:9001  (minioadmin / minioadmin123)"
info "  Kafka   → http://localhost:9021"
info "  Nessie  → http://localhost:19120/api/v1/trees"
info "  Trino   → http://localhost:8085"
info "  Grafana → http://localhost:3000  (admin / grafana123)"
info "  Airflow → http://localhost:8082  (admin / admin123)"
info "  Superset→ http://localhost:8088  (admin / admin123)"
echo ""
info "Next: open Grafana and import dashboards/ecommerce_platform.json"
info "      or run: make spark-quality && make spark-agg"
