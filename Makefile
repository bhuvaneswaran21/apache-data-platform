.PHONY: help setup start stop restart clean logs producer flink-sql trino spark-quality spark-agg

help:
	@echo "Big Data Platform — make targets"
	@echo ""
	@echo "  make setup         Download JARs + start all services"
	@echo "  make start         docker compose up -d"
	@echo "  make stop          docker compose stop"
	@echo "  make restart       stop + start"
	@echo "  make clean         stop + remove all volumes (DATA LOSS)"
	@echo "  make logs          Follow all service logs"
	@echo ""
	@echo "  make producer      Start sample Kafka event producer"
	@echo "  make flink-sql     Open Flink SQL client"
	@echo "  make trino         Open Trino CLI"
	@echo "  make spark-quality Run Deequ quality check"
	@echo "  make spark-agg     Run daily aggregation job"
	@echo ""
	@echo "  make topic         Create user_events Kafka topic"
	@echo "  make nessie-trees  List Nessie branches/tags"

setup:
	@bash scripts/setup.sh

start:
	docker compose up -d

stop:
	docker compose stop

restart: stop start

clean:
	@echo "WARNING: This destroys all data. Press Ctrl+C to cancel, Enter to continue."
	@read confirm
	docker compose down -v

logs:
	docker compose logs -f --tail=50

# ── Pipeline tools ──────────────────────────────────────────────────
producer:
	pip install -q kafka-python
	python scripts/kafka_producer.py

flink-sql:
	bash scripts/run_flink_sql.sh

trino:
	docker compose exec trino trino --catalog iceberg

# ── Kafka ───────────────────────────────────────────────────────────
topic:
	docker compose exec kafka kafka-topics \
	  --bootstrap-server localhost:29092 \
	  --create --if-not-exists \
	  --topic user_events \
	  --partitions 3 \
	  --replication-factor 1

kafka-lag:
	docker compose exec kafka kafka-consumer-groups \
	  --bootstrap-server localhost:29092 \
	  --describe --group flink-iceberg-group

# ── Spark jobs ──────────────────────────────────────────────────────
SPARK_PACKAGES := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3

spark-quality:
	docker compose exec spark-master spark-submit \
	  --master spark://spark-master:7077 \
	  --packages "$(SPARK_PACKAGES),com.amazon.deequ:deequ:2.0.7-spark-3.5" \
	  /opt/spark/jobs/deequ_quality_check.py

spark-agg:
	docker compose exec spark-master spark-submit \
	  --master spark://spark-master:7077 \
	  --packages "$(SPARK_PACKAGES)" \
	  /opt/spark/jobs/daily_aggregation.py

# ── Nessie ──────────────────────────────────────────────────────────
nessie-trees:
	curl -s http://localhost:19120/api/v1/trees | python3 -m json.tool

nessie-commits:
	curl -s "http://localhost:19120/api/v1/trees/tree/main/log?maxRecords=10" | python3 -m json.tool

# ── Health checks ───────────────────────────────────────────────────
health:
	@echo "=== Service Health ==="
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"
