import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

NESSIE_URI     = "http://nessie:19120/api/v1"
WAREHOUSE      = "s3://warehouse"
MINIO_ENDPOINT = "http://minio:9000"
JDBC_URL       = "jdbc:postgresql://postgres:5432/postgres"
JDBC_PROPS     = {"user": "postgres", "password": "postgres123", "driver": "org.postgresql.Driver"}

VALID_EVENT_TYPES = ["purchase", "view", "add_to_cart", "remove_from_cart", "wishlist"]
MIN_ROW_COUNT  = 1


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DeequQualityCheck")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",          "main")
        .config("spark.sql.catalog.nessie.warehouse",    WAREHOUSE)
        .config("spark.sql.catalog.nessie.s3.endpoint",         MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id",     "minioadmin")
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def run_checks(spark: SparkSession, df: DataFrame) -> list[dict]:
    """Run Deequ-style checks manually (works without PyDeequ installed)."""
    total = df.count()
    results = []

    def check(name: str, passed: bool, value, threshold=None):
        results.append({
            "check": name,
            "status": "SUCCESS" if passed else "FAILURE",
            "value": str(value),
            "threshold": str(threshold) if threshold else "N/A",
        })
        return passed

    # 1. Minimum row count
    check("row_count >= 1", total >= MIN_ROW_COUNT, total, MIN_ROW_COUNT)

    # 2. user_id completeness
    null_users = df.filter(col("user_id").isNull()).count()
    completeness = 1.0 - (null_users / max(total, 1))
    check("user_id completeness >= 0.99", completeness >= 0.99, round(completeness, 4), 0.99)

    # 3. event_type completeness
    null_events = df.filter(col("event_type").isNull()).count()
    ev_completeness = 1.0 - (null_events / max(total, 1))
    check("event_type completeness >= 0.99", ev_completeness >= 0.99, round(ev_completeness, 4), 0.99)

    # 4. amount non-negative for purchases
    purchases = df.filter(col("event_type") == "purchase")
    purchase_count = purchases.count()
    if purchase_count > 0:
        neg_amount = purchases.filter(col("amount") <= 0).count()
        pct_valid = 1.0 - (neg_amount / purchase_count)
        check("purchase amount > 0", pct_valid >= 1.0, round(pct_valid, 4), 1.0)

    # 5. event_type in allowed set
    invalid_types = df.filter(~col("event_type").isin(VALID_EVENT_TYPES)).count()
    type_validity = 1.0 - (invalid_types / max(total, 1))
    check("event_type in valid set", type_validity >= 0.99, round(type_validity, 4), 0.99)

    return results


def write_metrics(spark: SparkSession, results: list[dict], run_date: str):
    """Persist quality metrics to PostgreSQL."""
    rows = [
        (run_date, r["check"], r["status"], r["value"], r["threshold"])
        for r in results
    ]
    metrics_df = spark.createDataFrame(
        rows,
        ["run_date", "check_name", "status", "value", "threshold"]
    )

    metrics_df.write.format("jdbc") \
        .option("url",    JDBC_URL) \
        .option("dbtable", "data_quality_metrics") \
        .options(**JDBC_PROPS) \
        .mode("append") \
        .save()


def ensure_metrics_table(spark: SparkSession):
    spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("query", """
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                run_date   TEXT,
                check_name TEXT,
                status     TEXT,
                value      TEXT,
                threshold  TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """) \
        .options(**JDBC_PROPS) \
        .load()


def main():
    spark = create_spark_session()

    # Read latest partition of user_events
    df = spark.read.format("iceberg").load("nessie.events.user_events")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    results = run_checks(spark, df)

    print("\n=== Data Quality Report ===")
    failed = []
    for r in results:
        icon = "✓" if r["status"] == "SUCCESS" else "✗"
        print(f"  {icon} {r['check']:<45} value={r['value']}  threshold={r['threshold']}")
        if r["status"] == "FAILURE":
            failed.append(r["check"])

    try:
        ensure_metrics_table(spark)
        write_metrics(spark, results, run_date)
    except Exception as e:
        print(f"[warn] could not write metrics to postgres: {e}")

    if failed:
        print(f"\n[FAIL] {len(failed)} quality check(s) failed: {failed}")
        sys.exit(1)

    print("\n[PASS] All quality checks passed.")


if __name__ == "__main__":
    main()
