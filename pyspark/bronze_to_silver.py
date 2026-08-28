from pyspark.sql import functions as F

# Read marketplace data from Bronze
marketplace_bronze = spark.table(
    "workspace.bronze.marketplace_listings"
)

# Bronze -> Silver transformation
marketplace_silver = (
    marketplace_bronze
    .drop("_rescued_data")
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("scraped_at", F.to_timestamp("scraped_at"))
    .dropDuplicates(["listing_url"])
    .filter(F.col("product_title").isNotNull())
)

# Preview Silver data
marketplace_silver.printSchema()
marketplace_silver.show(20, truncate=False)

# Write Silver Delta table
marketplace_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.marketplace_listings")