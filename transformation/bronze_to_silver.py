from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper,
    current_timestamp, lit, to_date
)
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime, timezone

# ── SPARK SESSION ────────────────────────────────────────────────
# We configure Spark to use Iceberg as the table format
# and Glue as the metadata catalog
# This is equivalent to setting up Delta Lake config in Databricks
# The catalog name "glue_catalog" is what we use in SQL queries
# Example: SELECT * FROM glue_catalog.sellerradar_silver.products

spark = SparkSession.builder \
    .appName("SellerRadar-BronzeToSilver") \
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ) \
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    ) \
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://sellerradar-silver-sritham/"
    ) \
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    ) \
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SellerRadar — Bronze to Silver Transformation")
print(f"Started: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)

# ── STEP 1: READ BRONZE LAYER ────────────────────────────────────
# Read ALL JSON files from S3 Bronze in one command
# Spark reads entire folders automatically — all dates, all products
# inferSchema=True means Spark automatically detects column types
# This is equivalent to spark.read.json() in Databricks

BRONZE_PATH = "s3://sellerradar-bronze-sritham/raw/amazon/"

print(f"\nStep 1: Reading Bronze data from {BRONZE_PATH}")

df_raw = spark.read.option("inferSchema", "true").json(BRONZE_PATH)

total_records = df_raw.count()
print(f"Total raw records found: {total_records}")
print("Raw schema:")
df_raw.printSchema()

# ── STEP 2: DATA QUALITY CHECKS ─────────────────────────────────
# Before transforming, understand what we have
# This is the 6-pillar data quality framework:
# 1. Completeness  — are required fields present?
# 2. Validity      — are values in expected ranges?
# 3. Uniqueness    — no unexpected duplicates?
# 4. Consistency   — same format across all records?
# 5. Accuracy      — does data match reality?
# 6. Timeliness    — is data fresh enough?

print("\nStep 2: Data Quality Report — Bronze Layer")
print("-" * 40)
print(f"Total records:     {total_records}")
print(f"Null prices:       {df_raw.filter(col('price').isNull()).count()}")
print(f"Null ratings:      {df_raw.filter(col('rating').isNull()).count()}")
print(f"Null titles:       {df_raw.filter(col('title').isNull()).count()}")
print(f"Distinct ASINs:    {df_raw.select('asin').distinct().count()}")
print(f"Distinct dates:    {df_raw.select(col('scraped_at').substr(1,10)).distinct().count()}")

# ── STEP 3: BRONZE TO SILVER TRANSFORMATION ───────────────────────
# Clean and standardize the raw data
# Key principle: we NEVER drop records silently
# Instead we flag them with dq_status so we can investigate later
# This is called "fail open" — keep everything, mark the bad ones

print("\nStep 3: Applying Bronze to Silver transformations...")

df_silver = df_raw \
    .withColumn(
        "price",
        col("price").cast(DoubleType())
    ) \
    .withColumn(
        "rating",
        col("rating").cast(DoubleType())
    ) \
    .withColumn(
        "reviews_count",
        col("reviews_count").cast(IntegerType())
    ) \
    .withColumn(
        "title",
        trim(col("title"))
    ) \
    .withColumn(
        "category",
        trim(upper(col("category")))
    ) \
    .withColumn(
        # Extract date for partitioning
        # Partitioning by date means Athena only scans
        # relevant date folders instead of the entire table
        # This makes queries much faster and cheaper
        "scrape_date",
        to_date(col("scraped_at"))
    ) \
    .withColumn(
        # Data quality status flag
        # VALID = all fields present and in expected range
        # INVALID_PRICE = price is null or negative
        # MISSING_TITLE = title is null
        # INVALID_RATING = rating outside 0-5 range
        "dq_status",
        when(
            col("price").isNull() | (col("price") <= 0),
            "INVALID_PRICE"
        ).when(
            col("title").isNull(),
            "MISSING_TITLE"
        ).when(
            col("rating").isNotNull() &
            ((col("rating") < 0) | (col("rating") > 5)),
            "INVALID_RATING"
        ).otherwise("VALID")
    ) \
    .withColumn(
        # When was this record processed into Silver?
        # Different from scraped_at (when it was collected)
        # Useful for debugging — if data looks wrong,
        # you can see exactly when it was processed
        "processed_at",
        current_timestamp()
    ) \
    .withColumn(
        "source_system",
        lit("amazon_in_rainforest_api")
    ) \
    .select(
        # Always select columns explicitly in production
        # Never use SELECT * — schema must be controlled
        "asin",
        "title",
        "price",
        "rating",
        "reviews_count",
        "in_stock",
        "category",
        "source",
        "source_system",
        "scraped_at",
        "scrape_date",
        "processed_at",
        "dq_status"
    )

# ── STEP 4: QUALITY SUMMARY AFTER TRANSFORMATION ────────────────
print("\nStep 4: Data Quality Summary — After Transformation")
print("-" * 40)

silver_count = df_silver.count()
valid_count = df_silver.filter(col("dq_status") == "VALID").count()
invalid_count = silver_count - valid_count

print(f"Total silver records:  {silver_count}")
print(f"Valid records:         {valid_count}")
print(f"Invalid records:       {invalid_count}")
print(f"Valid percentage:      {round((valid_count/silver_count)*100, 2)}%")

print("\nBreakdown by DQ status:")
df_silver.groupBy("dq_status").count().show()

print("\nBreakdown by ASIN and stock status:")
df_silver.groupBy("asin", "in_stock").count().orderBy("asin").show()

print("\nSample cleaned records:")
df_silver.select(
    "asin", "title", "price", "rating", "dq_status", "scrape_date"
).show(5, truncate=60)

# ── STEP 5: CREATE ICEBERG TABLE ────────────────────────────────
# Creates the table in Glue Data Catalog if it doesn't exist
# PARTITIONED BY (scrape_date) means files are organized by date
# This is the key to fast Athena queries
# LOCATION tells Iceberg where to store files in S3
# format-version 2 enables all modern Iceberg features
# including row-level deletes needed for SCD Type 2 later

print("\nStep 5: Creating Iceberg table in Glue catalog...")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.sellerradar_silver.products (
        asin            STRING      COMMENT 'Amazon Standard Identification Number',
        title           STRING      COMMENT 'Product title',
        price           DOUBLE      COMMENT 'Current price in INR',
        rating          DOUBLE      COMMENT 'Product rating out of 5',
        reviews_count   INT         COMMENT 'Total number of reviews',
        in_stock        STRING      COMMENT 'Stock availability status',
        category        STRING      COMMENT 'Product category',
        source          STRING      COMMENT 'Data source identifier',
        source_system   STRING      COMMENT 'Full source system name',
        scraped_at      STRING      COMMENT 'When data was collected from API',
        scrape_date     DATE        COMMENT 'Partition column - date of scrape',
        processed_at    TIMESTAMP   COMMENT 'When record was processed to Silver',
        dq_status       STRING      COMMENT 'Data quality status flag'
    )
    USING iceberg
    PARTITIONED BY (scrape_date)
    LOCATION 's3://sellerradar-silver-sritham/products/'
    TBLPROPERTIES (
        'table_type'     = 'ICEBERG',
        'format-version' = '2',
        'write.format.default' = 'parquet'
    )
""")

print("Iceberg table created or already exists in Glue catalog")

# ── STEP 6: WRITE TO SILVER ──────────────────────────────────────
# Append mode — we keep all historical data
# Every job run adds new records without overwriting old ones
# This preserves complete price history over time
# merge-schema = true means if we add columns later, it works automatically

print("\nStep 6: Writing to Silver Iceberg table...")

df_silver.writeTo("glue_catalog.sellerradar_silver.products") \
    .option("merge-schema", "true") \
    .append()

print(f"Successfully wrote {silver_count} records to Silver")

# ── STEP 7: VERIFY WITH SQL ──────────────────────────────────────
# Run a real SQL query on the Iceberg table
# This is the moment your raw JSON becomes queryable data
# The same query works in Athena after this job completes

print("\nStep 7: Verification — SQL query on Silver Iceberg table")
print("-" * 40)

spark.sql("""
    SELECT
        asin,
        MIN(price)      AS min_price,
        MAX(price)      AS max_price,
        ROUND(AVG(price), 2) AS avg_price,
        MAX(price) - MIN(price) AS price_range,
        COUNT(*)        AS total_scrapes,
        MAX(scrape_date) AS latest_scrape
    FROM glue_catalog.sellerradar_silver.products
    WHERE dq_status = 'VALID'
    GROUP BY asin
    ORDER BY total_scrapes DESC
""").show(20, truncate=False)

print("\n" + "=" * 60)
print("Bronze to Silver transformation COMPLETE")
print(f"Finished: {datetime.now(timezone.utc).isoformat()}")
print("Your data is now queryable in AWS Athena")
print("=" * 60)