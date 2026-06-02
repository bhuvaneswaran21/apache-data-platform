from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_kafka_pipeline",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
) as dag:

    spark_task = SparkSubmitOperator(
    task_id="spark_stream_job",
    application="/opt/airflow/dags/stream_job.py",  
    conn_id="spark_default",
    packages=",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
    "org.apache.kafka:kafka-clients:3.5.1",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.94.4",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]),
    conf={
    "spark.master": "spark://spark-master:7077",

    "spark.executorEnv.AWS_REGION": "us-east-1",
    "spark.executorEnv.AWS_DEFAULT_REGION": "us-east-1",

    "spark.driverEnv.AWS_REGION": "us-east-1",
    "spark.driverEnv.AWS_DEFAULT_REGION": "us-east-1"
}
)