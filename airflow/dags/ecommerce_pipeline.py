from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ICEBERG_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.7.3",
])

DEEQU_PACKAGES = ICEBERG_PACKAGES + ",com.amazon.deequ:deequ:2.0.7-spark-3.5"

SPARK_CONF = {
    "spark.master":                        "spark://spark-master:7077",
    "spark.hadoop.fs.s3a.endpoint":        "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":      "minioadmin",
    "spark.hadoop.fs.s3a.secret.key":      "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl":
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    "spark.sql.catalog.nessie":              "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri":          "http://nessie:19120/api/v1",
    "spark.sql.catalog.nessie.ref":          "main",
    "spark.sql.catalog.nessie.warehouse":    "s3://warehouse",
    "spark.sql.catalog.nessie.io-impl":      "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint":          "http://minio:9000",
    "spark.sql.catalog.nessie.s3.path-style-access":  "true",
    "spark.sql.catalog.nessie.s3.access-key-id":      "minioadmin",
    "spark.sql.catalog.nessie.s3.secret-access-key":  "minioadmin123",
}

default_args = {
    "owner":            "data-platform",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def check_kafka_has_data(**ctx):
    """Skip pipeline if Kafka topic has no recent messages."""
    from kafka import KafkaConsumer
    try:
        consumer = KafkaConsumer(
            "user_events",
            bootstrap_servers="kafka:29092",
            consumer_timeout_ms=5000,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id="airflow-lag-check",
        )
        partitions = consumer.partitions_for_topic("user_events") or set()
        consumer.close()
        has_data = len(partitions) > 0
        print(f"Kafka topic has {len(partitions)} partition(s). Proceeding: {has_data}")
        return has_data
    except Exception as e:
        print(f"Kafka check failed ({e}), skipping pipeline.")
        return False


with DAG(
    dag_id="ecommerce_pipeline",
    description="Daily e-commerce data pipeline: Kafka→Spark→Iceberg→Quality→Analytics",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "iceberg", "spark"],
) as dag:

    kafka_check = ShortCircuitOperator(
        task_id="kafka_lag_check",
        python_callable=check_kafka_has_data,
    )

    spark_streaming = SparkSubmitOperator(
        task_id="spark_kafka_to_iceberg",
        application="/opt/airflow/spark/jobs/kafka_to_iceberg.py",
        conn_id="spark_default",
        packages=ICEBERG_PACKAGES,
        conf={
            **SPARK_CONF,
            "spark.streaming.stopGracefullyOnShutdown": "true",
        },
        execution_timeout=timedelta(minutes=10),
    )

    deequ_check = SparkSubmitOperator(
        task_id="deequ_quality_check",
        application="/opt/airflow/spark/jobs/deequ_quality_check.py",
        conn_id="spark_default",
        packages=DEEQU_PACKAGES,
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=15),
    )

    daily_agg = SparkSubmitOperator(
        task_id="daily_aggregation",
        application="/opt/airflow/spark/jobs/daily_aggregation.py",
        application_args=["{{ ds }}"],       
        conn_id="spark_default",
        packages=ICEBERG_PACKAGES,
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=20),
    )

    done = BashOperator(
        task_id="pipeline_complete",
        bash_command='echo "Pipeline complete for {{ ds }}. Check Trino at http://trino:8080"',
    )

    kafka_check >> spark_streaming >> deequ_check >> daily_agg >> done
