from databricks.connect import DatabricksSession

from pyspark.sql.functions import (
    col,
    round as spark_round,
    current_timestamp
)


# CONNECT TO DATABRICKS

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# READ FINAL SILVER TABLE

silver_df = spark.table(
    "workspace.silver.marketplace_sku_matched"
)

print(
    "\nSilver records:",
    silver_df.count()
)

print("\nSilver Schema:")
silver_df.printSchema()

# PREPARE GOLD DATA

gold_df = (
    silver_df

    # Convert price to numeric
    .withColumn(
        "price_numeric",
        spark_round(
            col("price").cast("double"),
            2
        )
    )

    # Gold metadata
    .withColumn(
        "gold_created_at",
        current_timestamp()
    )
)

# SELECT GOLD COLUMNS

gold_df = gold_df.select(

    # Marketplace information
    "listing_url",
    "product_title",
    "search_category",
    "brand",

    # Final SKU information
    "final_sku_id",
    "final_product_name",
    "final_model",

    # Matching information
    "final_match_score",
    "match_quality",
    "match_status",

    # Resale information
    "price_numeric",
    "currency",
    "condition",
    "seller",
    "location",

    # Data lineage
    "data_source",
    "is_synthetic",
    "scraped_at",
    "gold_created_at"
)

# CREATE GOLD SCHEMA

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.gold"
)

# WRITE GOLD DELTA TABLE

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.marketplace_resale_analytics"
    )

print(
    "\nGold table created successfully."
)

# VALIDATE GOLD TABLE

gold_table = spark.table(
    "workspace.gold.marketplace_resale_analytics"
)

print(
    "\nTotal Gold Records:",
    gold_table.count()
)


print("\nGold Match Status:")

gold_table.groupBy(
    "match_status"
).count().show()


print("\nGold Records by Data Source:")

gold_table.groupBy(
    "data_source"
).count().show()


print("\nGold Records by Brand:")

gold_table.groupBy(
    "brand"
).count().orderBy(
    "brand"
).show()


print("\nGold Sample:")

gold_table.select(
    "product_title",
    "brand",
    "final_sku_id",
    "final_product_name",
    "price_numeric",
    "match_status"
).show(
    20,
    truncate=False
)


print(
    "\nGold ingestion completed successfully."
)