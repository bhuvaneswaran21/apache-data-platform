from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType


def run():

    spark = (
        SparkSession.builder
        .appName("KafkaToIceberg")

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref", "main")

        .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse")
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.nessie.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "minioadmin123")
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1")


        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Spark Session Created")

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS nessie.db
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.db.nifi_test (
            user_id STRING,
            event STRING,
            amount INT,
            ts STRING
        )
        USING iceberg
    """)

    print("Iceberg table ready")

    schema = (
        StructType()
        .add("user_id", StringType())
        .add("event", StringType())
        .add("amount", IntegerType())
        .add("ts", StringType())
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "nifi-test")
        .option("startingOffsets", "earliest")
        .load()
    )

    print("Kafka stream connected")

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )


    query = (
        parsed_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "s3a://spark/checkpoints/nifi-test"
        )
        .toTable("nessie.db.nifi_test")
    )

    print("Streaming started")

    query.awaitTermination(90)


if __name__ == "__main__":
    run()