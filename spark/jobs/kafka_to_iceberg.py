from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "user_events"
NESSIE_URI      = "http://nessie:19120/api/v1"
WAREHOUSE       = "s3://warehouse"
MINIO_ENDPOINT  = "http://minio:9000"
CHECKPOINT_PATH = "s3://spark/checkpoints/kafka_to_iceberg"
JDBC_URL        = "jdbc:postgresql://postgres:5432/postgres"
JDBC_PROPS      = {"user": "postgres", "password": "postgres123", "driver": "org.postgresql.Driver"}

EVENT_SCHEMA = StructType([
    StructField("user_id",    LongType(),   True),
    StructField("event_type", StringType(), True),
    StructField("product",    StringType(), True),
    StructField("amount",     DoubleType(), True),
    StructField("ts",         StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("KafkaToIceberg")
        # Nessie / Iceberg catalog
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",          "main")
        .config("spark.sql.catalog.nessie.warehouse",    WAREHOUSE)
        # S3 / MinIO settings
        .config("spark.sql.catalog.nessie.s3.endpoint",          MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access",  "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id",      "minioadmin")
        .config("spark.sql.catalog.nessie.s3.secret-access-key",  "minioadmin123")
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",         "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession):
    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.events")
    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.analytics")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.events.user_events (
            user_id     BIGINT,
            event_type  STRING,
            product     STRING,
            category    STRING,
            amount      DOUBLE,
            ts          STRING,
            event_time  TIMESTAMP,
            dt          STRING,
            hr          STRING
        )
        USING iceberg
        PARTITIONED BY (dt, hr)
        TBLPROPERTIES ('format-version'='2')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.analytics.daily_revenue (
            dt           STRING,
            category     STRING,
            event_type   STRING,
            event_count  BIGINT,
            total_revenue DOUBLE
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """)


def load_products(spark: SparkSession):
    return (
        spark.read.format("jdbc")
        .option("url",   JDBC_URL)
        .option("dbtable", "products")
        .options(**JDBC_PROPS)
        .load()
        .select("name", "category")
    )


def process_batch(batch_df, batch_id, products_broadcast):
    if batch_df.isEmpty():
        return

    enriched = (
        batch_df
        .join(products_broadcast, batch_df.product == products_broadcast.name, "left")
        .select(
            batch_df.user_id,
            batch_df.event_type,
            batch_df.product,
            when(col("category").isNull(), "unknown").otherwise(col("category")).alias("category"),
            batch_df.amount,
            batch_df.ts,
            to_timestamp(batch_df.ts, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
            date_format(to_timestamp(batch_df.ts, "yyyy-MM-dd'T'HH:mm:ss'Z'"), "yyyy-MM-dd").alias("dt"),
            date_format(to_timestamp(batch_df.ts, "yyyy-MM-dd'T'HH:mm:ss'Z'"), "HH").alias("hr"),
        )
    )

    enriched.write.format("iceberg").mode("append").save("nessie.events.user_events")
    print(f"[batch {batch_id}] wrote {enriched.count()} rows to nessie.events.user_events")


def main():
    spark = create_spark_session()
    ensure_tables(spark)

    products = load_products(spark).cache()

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("user_id").isNotNull())
    )

    query = (
        parsed.writeStream
        .foreachBatch(lambda df, bid: process_batch(df, bid, products))
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
