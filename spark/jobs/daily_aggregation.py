import sys
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round

NESSIE_URI     = "http://nessie:19120/api/v1"
WAREHOUSE      = "s3://warehouse"
MINIO_ENDPOINT = "http://minio:9000"
JDBC_URL       = "jdbc:postgresql://postgres:5432/postgres"
JDBC_PROPS     = {"user": "postgres", "password": "postgres123", "driver": "org.postgresql.Driver"}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DailyAggregation")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",          "main")
        .config("spark.sql.catalog.nessie.warehouse",    WAREHOUSE)
        .config("spark.sql.catalog.nessie.s3.endpoint",          MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access",  "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id",      "minioadmin")
        .config("spark.sql.catalog.nessie.s3.secret-access-key",  "minioadmin123")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def ensure_analytics_tables(spark: SparkSession):
    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.analytics")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.analytics.daily_revenue_by_category (
            dt            STRING,
            category      STRING,
            event_type    STRING,
            event_count   BIGINT,
            total_revenue DOUBLE,
            avg_order     DOUBLE
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES ('format-version'='2')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.analytics.top_products_daily (
            dt       STRING,
            product  STRING,
            category STRING,
            orders   BIGINT,
            revenue  DOUBLE
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES ('format-version'='2')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.analytics.hourly_event_counts (
            dt          STRING,
            hr          STRING,
            event_type  STRING,
            event_count BIGINT
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES ('format-version'='2')
    """)


def run_aggregations(spark: SparkSession, target_date: str):
    events = spark.read.format("iceberg") \
        .load("nessie.events.user_events") \
        .filter(col("dt") == target_date)

    products = spark.read.format("jdbc") \
        .option("url",     JDBC_URL) \
        .option("dbtable", "products") \
        .options(**JDBC_PROPS) \
        .load() \
        .select("name", "category")

    enriched = events.join(products, events.product == products.name, "left")

    # --- 1. Revenue by category ---
    revenue = (
        enriched
        .groupBy("dt", "category", "event_type")
        .agg(
            count("*").alias("event_count"),
            spark_round(spark_sum("amount"), 2).alias("total_revenue"),
            spark_round(avg("amount"), 2).alias("avg_order"),
        )
    )
    revenue.write.format("iceberg") \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .save("nessie.analytics.daily_revenue_by_category")

    # --- 2. Top products ---
    top_products = (
        enriched
        .filter(col("event_type") == "purchase")
        .groupBy("dt", "product", "category")
        .agg(
            count("*").alias("orders"),
            spark_round(spark_sum("amount"), 2).alias("revenue"),
        )
        .orderBy(col("revenue").desc())
    )
    top_products.write.format("iceberg") \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .save("nessie.analytics.top_products_daily")

    # --- 3. Hourly event counts ---
    hourly = (
        events
        .groupBy("dt", "hr", "event_type")
        .agg(count("*").alias("event_count"))
    )
    hourly.write.format("iceberg") \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .save("nessie.analytics.hourly_event_counts")

    print(f"[OK] Aggregations complete for dt={target_date}")


def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else \
        (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    spark = create_spark_session()
    ensure_analytics_tables(spark)
    run_aggregations(spark, target_date)


if __name__ == "__main__":
    main()
