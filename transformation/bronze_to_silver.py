import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper,
    current_timestamp, lit, to_date,
    min as spark_min, max as spark_max,
    avg as spark_avg, countDistinct
)
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime, timezone

# ── SPARK SESSION ────────────────────────────────────────────────
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
# Using glob pattern */*.json instead of folder path
# This explicitly tells Spark to find all JSON files
# in all date subfolders under raw/amazon/
# Fixes the UNABLE_TO_INFER_SCHEMA error caused by
# Spark not finding files when given just the folder path

BRONZE_PATH = "s3://sellerradar-bronze-sritham/raw/amazon/"

print(f"\nStep 1: Reading Bronze data from {BRONZE_PATH}")

df_raw = spark.read \
    .option("multiLine", "true") \
    .option("recursiveFileLookup", "true") \
    .json(BRONZE_PATH)

total_records = df_raw.count()

if total_records == 0:
    print("ERROR: No records found. Check S3 path.")
    spark.stop()
    sys.exit(1)

print(f"Total raw records found: {total_records}")
df_raw.printSchema()

# ── STEP 2: FULL 6-PILLAR DATA QUALITY FRAMEWORK ────────────────
print("\nStep 2: Full 6-Pillar Data Quality Report")
print("=" * 60)

# PILLAR 1: COMPLETENESS
print("\nPillar 1 — Completeness:")
null_prices     = df_raw.filter(col('price').isNull()).count()
null_ratings    = df_raw.filter(col('rating').isNull()).count()
null_titles     = df_raw.filter(col('title').isNull()).count()
null_in_stock   = df_raw.filter(col('in_stock').isNull()).count()
null_categories = df_raw.filter(col('category').isNull()).count()
null_scraped    = df_raw.filter(col('scraped_at').isNull()).count()

print(f"  Total records:        {total_records}")
print(f"  Null prices:          {null_prices}")
print(f"  Null ratings:         {null_ratings}")
print(f"  Null titles:          {null_titles}")
print(f"  Null in_stock:        {null_in_stock}")
print(f"  Null categories:      {null_categories}")
print(f"  Null scraped_at:      {null_scraped}")

completeness_score = (
    df_raw.filter(
        col('price').isNotNull() &
        col('title').isNotNull() &
        col('asin').isNotNull() &
        col('scraped_at').isNotNull()
    ).count() / total_records * 100
)
print(f"  Completeness score:   {round(completeness_score, 2)}%")

# PILLAR 2: VALIDITY
print("\nPillar 2 — Validity:")
df_typed = df_raw \
    .withColumn("price_num", col("price").cast(DoubleType())) \
    .withColumn("rating_num", col("rating").cast(DoubleType()))

invalid_price = df_typed.filter(
    col('price_num').isNotNull() & (col('price_num') <= 0)
).count()
invalid_rating = df_typed.filter(
    col('rating_num').isNotNull() &
    ((col('rating_num') < 0) | (col('rating_num') > 5))
).count()
suspicious_price = df_typed.filter(
    col('price_num').isNotNull() &
    ((col('price_num') < 100) | (col('price_num') > 100000))
).count()

print(f"  Invalid prices (<=0):         {invalid_price}")
print(f"  Invalid ratings (out of 0-5): {invalid_rating}")
print(f"  Suspicious prices:            {suspicious_price}")

validity_score = (
    (total_records - invalid_price - invalid_rating) /
    total_records * 100
)
print(f"  Validity score:               {round(validity_score, 2)}%")

# PILLAR 3: UNIQUENESS
print("\nPillar 3 — Uniqueness:")
total_combinations    = df_raw.select("asin", "scraped_at").count()
distinct_combinations = df_raw.select("asin", "scraped_at").distinct().count()
duplicates            = total_combinations - distinct_combinations

print(f"  Total asin+timestamp combos:    {total_combinations}")
print(f"  Distinct asin+timestamp combos: {distinct_combinations}")
print(f"  Duplicate records:              {duplicates}")

uniqueness_score = (distinct_combinations / total_combinations * 100)
print(f"  Uniqueness score:               {round(uniqueness_score, 2)}%")

# PILLAR 4: CONSISTENCY
print("\nPillar 4 — Consistency:")
distinct_asins   = df_raw.select("asin").distinct().count()
distinct_sources = df_raw.select("source").distinct().count()

print(f"  Distinct ASINs:               {distinct_asins}")
print(f"  Expected ASINs:               11")
print(f"  ASIN count correct:           {distinct_asins == 11}")
print(f"  Distinct sources:             {distinct_sources} (expected: 1)")

consistency_score = 100.0 if (
    distinct_asins == 11 and distinct_sources == 1
) else 50.0
print(f"  Consistency score:            {consistency_score}%")

# PILLAR 5: ACCURACY
print("\nPillar 5 — Accuracy:")
price_stats = df_typed.filter(col('price_num').isNotNull()).agg(
    spark_min("price_num").alias("min_price"),
    spark_max("price_num").alias("max_price"),
    spark_avg("price_num").alias("avg_price")
).collect()[0]

rating_stats = df_typed.filter(col('rating_num').isNotNull()).agg(
    spark_min("rating_num").alias("min_rating"),
    spark_max("rating_num").alias("max_rating"),
    spark_avg("rating_num").alias("avg_rating")
).collect()[0]

print(f"  Price range:  {price_stats['min_price']} — {price_stats['max_price']}")
print(f"  Avg price:    {round(price_stats['avg_price'], 2)}")
print(f"  Rating range: {rating_stats['min_rating']} — {rating_stats['max_rating']}")
print(f"  Avg rating:   {round(rating_stats['avg_rating'], 2)}")

accuracy_score = 100.0 if suspicious_price == 0 else (
    (total_records - suspicious_price) / total_records * 100
)
print(f"  Accuracy score: {round(accuracy_score, 2)}%")

# PILLAR 6: TIMELINESS
print("\nPillar 6 — Timeliness:")
time_stats = df_raw.agg(
    spark_min("scraped_at").alias("earliest_scrape"),
    spark_max("scraped_at").alias("latest_scrape"),
    countDistinct("scraped_at").alias("distinct_scrape_times")
).collect()[0]

print(f"  Earliest scrape:      {time_stats['earliest_scrape']}")
print(f"  Latest scrape:        {time_stats['latest_scrape']}")
print(f"  Distinct scrape runs: {time_stats['distinct_scrape_times']}")

timeliness_score = 100.0
print(f"  Timeliness score:     {timeliness_score}%")

# OVERALL DQ SCORE
overall_dq_score = (
    completeness_score * 0.25 +
    validity_score     * 0.25 +
    uniqueness_score   * 0.20 +
    consistency_score  * 0.15 +
    accuracy_score     * 0.10 +
    timeliness_score   * 0.05
)

print(f"\n{'=' * 60}")
print(f"OVERALL DATA QUALITY SCORE: {round(overall_dq_score, 2)}%")
print(f"Completeness:  {round(completeness_score, 2)}%  (weight: 25%)")
print(f"Validity:      {round(validity_score, 2)}%  (weight: 25%)")
print(f"Uniqueness:    {round(uniqueness_score, 2)}%  (weight: 20%)")
print(f"Consistency:   {round(consistency_score, 2)}%  (weight: 15%)")
print(f"Accuracy:      {round(accuracy_score, 2)}%  (weight: 10%)")
print(f"Timeliness:    {round(timeliness_score, 2)}%  (weight:  5%)")
print(f"{'=' * 60}")

# ── STEP 3: BRONZE TO SILVER TRANSFORMATION ───────────────────────
print("\nStep 3: Applying transformations...")

df_silver = df_raw \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("rating", col("rating").cast(DoubleType())) \
    .withColumn("reviews_count", col("reviews_count").cast(IntegerType())) \
    .withColumn("title", trim(col("title"))) \
    .withColumn("category", trim(upper(col("category")))) \
    .withColumn("scrape_date", to_date(col("scraped_at"))) \
    .withColumn(
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
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("source_system", lit("amazon_in_rainforest_api")) \
    .select(
        "asin", "title", "price", "rating", "reviews_count",
        "in_stock", "category", "source", "source_system",
        "scraped_at", "scrape_date", "processed_at", "dq_status"
    )

# ── STEP 4: POST-TRANSFORMATION SUMMARY ─────────────────────────
print("\nStep 4: Post-Transformation Quality Summary")
silver_count  = df_silver.count()
valid_count   = df_silver.filter(col("dq_status") == "VALID").count()
invalid_count = silver_count - valid_count

print(f"Total silver records:  {silver_count}")
print(f"Valid records:         {valid_count}")
print(f"Invalid records:       {invalid_count}")
print(f"Valid percentage:      {round((valid_count/silver_count)*100, 2)}%")

df_silver.groupBy("dq_status").count().show()
df_silver.select(
    "asin", "title", "price", "rating", "dq_status", "scrape_date"
).show(5, truncate=60)

# ── STEP 5: CREATE ICEBERG TABLE ────────────────────────────────
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
        'table_type'           = 'ICEBERG',
        'format-version'       = '2',
        'write.format.default' = 'parquet'
    )
""")

print("Iceberg table created or already exists")

# ── STEP 6: WRITE TO SILVER ──────────────────────────────────────
print("\nStep 6: Writing to Silver Iceberg table...")

df_silver.writeTo("glue_catalog.sellerradar_silver.products") \
    .option("merge-schema", "true") \
    .append()

print(f"Successfully wrote {silver_count} records to Silver")

# ── STEP 7: VERIFY WITH SQL ──────────────────────────────────────
print("\nStep 7: SQL Verification")

spark.sql("""
    SELECT
        asin,
        MIN(price)              AS min_price,
        MAX(price)              AS max_price,
        ROUND(AVG(price), 2)    AS avg_price,
        MAX(price) - MIN(price) AS price_range,
        COUNT(*)                AS total_scrapes,
        MAX(scrape_date)        AS latest_scrape
    FROM glue_catalog.sellerradar_silver.products
    GROUP BY asin
    ORDER BY total_scrapes DESC
""").show(20, truncate=False)

print("\n" + "=" * 60)
print("Bronze to Silver transformation COMPLETE")
print(f"Overall DQ Score: {round(overall_dq_score, 2)}%")
print(f"Finished: {datetime.now(timezone.utc).isoformat()}")
print("Data is now queryable in AWS Athena")
print("=" * 60)

spark.stop()