import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    round as spark_round, lag, rank,
    row_number, count, avg, min as spark_min,
    max as spark_max, sum as spark_sum,
    datediff, to_date, desc, asc,
    first, last, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime, timezone

# ── SPARK SESSION ────────────────────────────────────────────────
# Same Iceberg + Glue configuration as Bronze to Silver
# But now we also need the Gold catalog warehouse
# We register it as the same glue_catalog since Glue
# is our single metadata store for all layers

spark = SparkSession.builder \
    .appName("SellerRadar-SilverToGold") \
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
        "s3://sellerradar-gold-sritham/"
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
print("SellerRadar — Silver to Gold Transformation")
print(f"Started: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)

# ── READ SILVER LAYER ─────────────────────────────────────────────
# Read from our Silver Iceberg table
# We only process VALID records — invalid ones stay in Silver
# for investigation but never reach Gold
# This is the quality gate between Silver and Gold

print("\nReading Silver layer...")

# We need to read from Silver which uses a different S3 location

# Actually with Iceberg we read directly using spark.read
df_silver = spark.read \
    .option("multiLine", "true") \
    .option("recursiveFileLookup", "true") \
    .parquet("s3://sellerradar-silver-sritham/products/data/")

print(f"Silver records read: {df_silver.count()}")
print(f"Columns: {df_silver.columns}")

# Filter to only VALID records for Gold layer
df_valid = df_silver.filter(col("dq_status") == "VALID")
valid_count = df_valid.count()
print(f"Valid records for Gold processing: {valid_count}")

if valid_count == 0:
    print("ERROR: No valid records found in Silver layer.")
    spark.stop()
    sys.exit(1)

df_valid.cache()

# ── GOLD TABLE 1: PRICE HISTORY ──────────────────────────────────
# What it answers: How has each product's price changed over time?
# Key fields: asin, price, previous_price, price_diff, price_change_pct
# Why it matters: A 5% price drop by a competitor means the seller
# needs to respond. This table powers the price alert system.

print("\n" + "=" * 60)
print("Building Gold Table 1: price_history")
print("=" * 60)

# Window spec for price change calculation
# We partition by asin so each product has its own price timeline
# We order by scraped_at so we look at prices in chronological order
# LAG() gets the previous price for comparison

price_window = Window \
    .partitionBy("asin") \
    .orderBy("scraped_at")

df_price_history = df_valid \
    .withColumn(
        # LAG gets the value from the previous row in the window
        # LAG(price, 1) = price from 1 row back (previous scrape)
        # This is how you detect price changes in time series data
        "previous_price",
        lag("price", 1).over(price_window)
    ) \
    .withColumn(
        # Price difference = current price - previous price
        # Positive = price went up, Negative = price went down
        # NULL on first record (no previous price to compare)
        "price_diff",
        when(
            col("previous_price").isNotNull(),
            spark_round(col("price") - col("previous_price"), 2)
        ).otherwise(lit(None).cast(DoubleType()))
    ) \
    .withColumn(
        # Percentage change = (diff / previous) * 100
        # This normalizes price changes across different price ranges
        # A ₹100 change on a ₹500 product (20%) is more significant
        # than ₹100 change on a ₹5000 product (2%)
        "price_change_pct",
        when(
            col("previous_price").isNotNull() & (col("previous_price") > 0),
            spark_round(
                (col("price") - col("previous_price")) / col("previous_price") * 100,
                2
            )
        ).otherwise(lit(None).cast(DoubleType()))
    ) \
    .withColumn(
        # Flag records where price actually changed
        # This is what triggers the seller alert system
        "price_changed",
        when(
            col("price_diff").isNotNull() & (col("price_diff") != 0),
            lit(True)
        ).otherwise(lit(False))
    ) \
    .withColumn(
        # Direction of change for quick filtering
        "price_direction",
        when(col("price_diff") > 0, "UP")
        .when(col("price_diff") < 0, "DOWN")
        .when(col("price_diff") == 0, "STABLE")
        .otherwise("FIRST_RECORD")
    ) \
    .withColumn("gold_processed_at", current_timestamp()) \
    .select(
        "asin", "title", "price", "previous_price",
        "price_diff", "price_change_pct", "price_changed",
        "price_direction", "rating", "reviews_count",
        "in_stock", "category", "scraped_at", "scrape_date",
        "gold_processed_at"
    )

# Show price changes detected
print("\nPrice changes detected:")
df_price_history.filter(col("price_changed") == True) \
    .select("asin", "title", "price", "previous_price", "price_diff", "price_change_pct") \
    .show(10, truncate=50)

# Create Gold table
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.sellerradar_gold.price_history (
        asin                STRING      COMMENT 'Amazon product identifier',
        title               STRING      COMMENT 'Product title',
        price               DOUBLE      COMMENT 'Price at time of scrape',
        previous_price      DOUBLE      COMMENT 'Price from previous scrape',
        price_diff          DOUBLE      COMMENT 'Absolute price change',
        price_change_pct    DOUBLE      COMMENT 'Percentage price change',
        price_changed       BOOLEAN     COMMENT 'True if price changed from previous scrape',
        price_direction     STRING      COMMENT 'UP DOWN STABLE or FIRST_RECORD',
        rating              DOUBLE      COMMENT 'Product rating',
        reviews_count       INT         COMMENT 'Total review count',
        in_stock            STRING      COMMENT 'Stock status',
        category            STRING      COMMENT 'Product category',
        scraped_at          STRING      COMMENT 'When data was collected',
        scrape_date         DATE        COMMENT 'Partition column',
        gold_processed_at   TIMESTAMP   COMMENT 'When record reached Gold'
    )
    USING iceberg
    PARTITIONED BY (scrape_date)
    LOCATION 's3://sellerradar-gold-sritham/price_history/'
    TBLPROPERTIES (
        'table_type'     = 'ICEBERG',
        'format-version' = '2'
    )
""")

df_price_history.writeTo("glue_catalog.sellerradar_gold.price_history") \
    .option("merge-schema", "true") \
    .append()

print(f"price_history: {df_price_history.count()} records written")

# ── GOLD TABLE 2: COMPETITOR RANKING ─────────────────────────────
# What it answers: Who is cheapest right now? How has ranking changed?
# Key fields: asin, price_rank, price_rank_change, is_cheapest
# Why it matters: If a seller's product is rank 5 by price but was
# rank 2 yesterday, 3 competitors dropped price. Time to respond.

print("\n" + "=" * 60)
print("Building Gold Table 2: competitor_ranking")
print("=" * 60)

# Get the latest price for each product per day
# We use ROW_NUMBER to get the most recent record per asin per day
latest_window = Window \
    .partitionBy("asin", "scrape_date") \
    .orderBy(desc("scraped_at"))

df_latest = df_valid \
    .withColumn("row_num", row_number().over(latest_window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Rank competitors by price within each category and date
# RANK() assigns same rank to ties and skips next rank
# DENSE_RANK() assigns same rank to ties but doesn't skip
# We use DENSE_RANK so rank 1,2,3 even with ties
rank_window = Window \
    .partitionBy("category", "scrape_date") \
    .orderBy(asc("price"))

prev_rank_window = Window \
    .partitionBy("asin") \
    .orderBy("scrape_date")

df_ranking = df_latest \
    .withColumn(
        "price_rank",
        rank().over(rank_window)
    ) \
    .withColumn(
        "is_cheapest",
        when(col("price_rank") == 1, lit(True)).otherwise(lit(False))
    ) \
    .withColumn(
        "previous_rank",
        lag("price_rank", 1).over(prev_rank_window)
    ) \
    .withColumn(
        # Positive = rank improved (got cheaper relative to competitors)
        # Negative = rank worsened (others got cheaper)
        "rank_change",
        when(
            col("previous_rank").isNotNull(),
            col("previous_rank") - col("price_rank")
        ).otherwise(lit(None).cast(IntegerType()))
    ) \
    .withColumn("gold_processed_at", current_timestamp()) \
    .select(
        "asin", "title", "category", "price",
        "price_rank", "is_cheapest", "previous_rank", "rank_change",
        "rating", "reviews_count", "in_stock",
        "scrape_date", "gold_processed_at"
    )

print("\nCompetitor rankings today:")
df_ranking.orderBy("category", "price_rank") \
    .select("asin", "title", "price", "price_rank", "is_cheapest", "rank_change") \
    .show(11, truncate=40)

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.sellerradar_gold.competitor_ranking (
        asin                STRING,
        title               STRING,
        category            STRING,
        price               DOUBLE,
        price_rank          INT         COMMENT 'Price rank within category (1=cheapest)',
        is_cheapest         BOOLEAN     COMMENT 'True if cheapest in category',
        previous_rank       INT         COMMENT 'Rank on previous day',
        rank_change         INT         COMMENT 'Positive=improved, negative=worsened',
        rating              DOUBLE,
        reviews_count       INT,
        in_stock            STRING,
        scrape_date         DATE,
        gold_processed_at   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (scrape_date)
    LOCATION 's3://sellerradar-gold-sritham/competitor_ranking/'
    TBLPROPERTIES ('table_type'='ICEBERG', 'format-version'='2')
""")

df_ranking.writeTo("glue_catalog.sellerradar_gold.competitor_ranking") \
    .option("merge-schema", "true") \
    .append()

print(f"competitor_ranking: {df_ranking.count()} records written")

# ── GOLD TABLE 3: REVIEW TREND ────────────────────────────────────
# What it answers: Which products are gaining/losing reviews?
# Review velocity = reviews gained per day = proxy for sales velocity
# A product gaining 100 reviews/day is selling much faster
# than one gaining 5 reviews/day

print("\n" + "=" * 60)
print("Building Gold Table 3: review_trend")
print("=" * 60)

review_window = Window \
    .partitionBy("asin") \
    .orderBy("scrape_date")

df_review = df_latest \
    .withColumn(
        "previous_review_count",
        lag("reviews_count", 1).over(review_window)
    ) \
    .withColumn(
        "reviews_gained",
        when(
            col("previous_review_count").isNotNull(),
            col("reviews_count") - col("previous_review_count")
        ).otherwise(lit(0).cast(IntegerType()))
    ) \
    .withColumn(
        # Classify review velocity
        "review_velocity",
        when(col("reviews_gained") > 50, "HIGH")
        .when(col("reviews_gained") > 10, "MEDIUM")
        .when(col("reviews_gained") > 0, "LOW")
        .when(col("reviews_gained") == 0, "STAGNANT")
        .otherwise("DECLINING")
    ) \
    .withColumn("gold_processed_at", current_timestamp()) \
    .select(
        "asin", "title", "rating", "reviews_count",
        "previous_review_count", "reviews_gained",
        "review_velocity", "price", "category",
        "scrape_date", "gold_processed_at"
    )

print("\nReview trends:")
df_review.select("asin", "title", "reviews_count", "reviews_gained", "review_velocity") \
    .show(11, truncate=40)

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.sellerradar_gold.review_trend (
        asin                    STRING,
        title                   STRING,
        rating                  DOUBLE,
        reviews_count           INT,
        previous_review_count   INT,
        reviews_gained          INT         COMMENT 'Reviews gained since last scrape',
        review_velocity         STRING      COMMENT 'HIGH MEDIUM LOW STAGNANT DECLINING',
        price                   DOUBLE,
        category                STRING,
        scrape_date             DATE,
        gold_processed_at       TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (scrape_date)
    LOCATION 's3://sellerradar-gold-sritham/review_trend/'
    TBLPROPERTIES ('table_type'='ICEBERG', 'format-version'='2')
""")

df_review.writeTo("glue_catalog.sellerradar_gold.review_trend") \
    .option("merge-schema", "true") \
    .append()

print(f"review_trend: {df_review.count()} records written")

# ── GOLD TABLE 4: STOCK GAP ───────────────────────────────────────
# What it answers: Which competitors are currently out of stock?
# A stock gap = opportunity. If competitor X is out of stock,
# the seller should run ads on that product's keywords immediately.

print("\n" + "=" * 60)
print("Building Gold Table 4: stock_gap")
print("=" * 60)

# Get current stock status — most recent record per product
current_window = Window \
    .partitionBy("asin") \
    .orderBy(desc("scraped_at"))

df_stock = df_valid \
    .withColumn("row_num", row_number().over(current_window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \
    .withColumn(
        "is_out_of_stock",
        when(col("in_stock") != "in_stock", lit(True)).otherwise(lit(False))
    ) \
    .withColumn(
        "opportunity_score",
        # Products that are out of stock AND have high review counts
        # represent the biggest opportunity — proven demand, no supply
        when(
            (col("is_out_of_stock") == True) & (col("reviews_count") > 1000),
            lit("HIGH")
        ).when(
            col("is_out_of_stock") == True,
            lit("MEDIUM")
        ).otherwise(lit("NONE"))
    ) \
    .withColumn("gold_processed_at", current_timestamp()) \
    .select(
        "asin", "title", "in_stock", "is_out_of_stock",
        "opportunity_score", "price", "rating",
        "reviews_count", "category",
        "scraped_at", "scrape_date", "gold_processed_at"
    )

print("\nStock gap analysis:")
df_stock.select("asin", "title", "in_stock", "is_out_of_stock", "opportunity_score") \
    .show(11, truncate=40)

print(f"\nOut of stock competitors: {df_stock.filter(col('is_out_of_stock')==True).count()}")
print(f"High opportunity gaps:    {df_stock.filter(col('opportunity_score')=='HIGH').count()}")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.sellerradar_gold.stock_gap (
        asin                STRING,
        title               STRING,
        in_stock            STRING,
        is_out_of_stock     BOOLEAN     COMMENT 'True if competitor is out of stock',
        opportunity_score   STRING      COMMENT 'HIGH MEDIUM NONE',
        price               DOUBLE,
        rating              DOUBLE,
        reviews_count       INT,
        category            STRING,
        scraped_at          STRING,
        scrape_date         DATE,
        gold_processed_at   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (scrape_date)
    LOCATION 's3://sellerradar-gold-sritham/stock_gap/'
    TBLPROPERTIES ('table_type'='ICEBERG', 'format-version'='2')
""")

df_stock.writeTo("glue_catalog.sellerradar_gold.stock_gap") \
    .option("merge-schema", "true") \
    .append()

print(f"stock_gap: {df_stock.count()} records written")

# ── GOLD TABLE 5: MARKET SUMMARY ─────────────────────────────────
# What it answers: What is the overall market health today?
# One row per category per day. The big picture view.
# This is what the daily AI briefing reads to generate its summary.

print("\n" + "=" * 60)
print("Building Gold Table 5: market_summary")
print("=" * 60)

df_market = df_latest \
    .groupBy("category", "scrape_date") \
    .agg(
        count("*").alias("total_products"),
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        spark_round(avg("rating"), 2).alias("avg_rating"),
        spark_sum(
            when(col("in_stock") == "in_stock", 1).otherwise(0)
        ).alias("products_in_stock"),
        spark_sum(
            when(col("in_stock") != "in_stock", 1).otherwise(0)
        ).alias("products_out_of_stock"),
        spark_max("reviews_count").alias("max_reviews"),
        spark_round(avg("reviews_count"), 0).alias("avg_reviews")
    ) \
    .withColumn(
        "stock_availability_pct",
        spark_round(
            col("products_in_stock") / col("total_products") * 100,
            1
        )
    ) \
    .withColumn(
        "market_health",
        when(col("stock_availability_pct") >= 80, "HEALTHY")
        .when(col("stock_availability_pct") >= 50, "MODERATE")
        .otherwise("STRESSED")
    ) \
    .withColumn("gold_processed_at", current_timestamp())

print("\nMarket summary:")
df_market.show(truncate=False)

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.sellerradar_gold.market_summary (
        category                STRING,
        scrape_date             DATE,
        total_products          LONG,
        avg_price               DOUBLE,
        min_price               DOUBLE,
        max_price               DOUBLE,
        avg_rating              DOUBLE,
        products_in_stock       LONG,
        products_out_of_stock   LONG,
        max_reviews             LONG,
        avg_reviews             DOUBLE,
        stock_availability_pct  DOUBLE      COMMENT 'Percent of products in stock',
        market_health           STRING      COMMENT 'HEALTHY MODERATE STRESSED',
        gold_processed_at       TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (scrape_date)
    LOCATION 's3://sellerradar-gold-sritham/market_summary/'
    TBLPROPERTIES ('table_type'='ICEBERG', 'format-version'='2')
""")

df_market.writeTo("glue_catalog.sellerradar_gold.market_summary") \
    .option("merge-schema", "true") \
    .append()

print(f"market_summary: {df_market.count()} records written")

# ── FINAL VERIFICATION ───────────────────────────────────────────
print("\n" + "=" * 60)
print("Gold Layer Verification")
print("=" * 60)

print("\nPrice changes detected:")
spark.sql("""
    SELECT asin, price, previous_price, price_diff,
           price_change_pct, price_direction
    FROM glue_catalog.sellerradar_gold.price_history
    WHERE price_changed = true
    ORDER BY ABS(price_diff) DESC
""").show(10, truncate=False)

print("\nCompetitor rankings:")
spark.sql("""
    SELECT asin, title, price, price_rank, is_cheapest, rank_change
    FROM glue_catalog.sellerradar_gold.competitor_ranking
    ORDER BY price_rank ASC
""").show(11, truncate=40)

print("\nMarket summary:")
spark.sql("""
    SELECT category, avg_price, min_price, max_price,
           products_in_stock, market_health
    FROM glue_catalog.sellerradar_gold.market_summary
""").show(truncate=False)

print("\n" + "=" * 60)
print("Silver to Gold transformation COMPLETE")
print(f"Tables created: price_history, competitor_ranking,")
print(f"                review_trend, stock_gap, market_summary")
print(f"Finished: {datetime.now(timezone.utc).isoformat()}")
print("=" * 60)

spark.stop()