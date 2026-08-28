from pyspark.sql import functions as F

# Read marketplace data from Silver
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Silver -> Gold transformation
marketplace_gold = (
    marketplace_silver
    .groupBy("condition")
    .agg(
        F.count("*").alias("listing_count"),
        F.round(F.avg("price"), 2).alias("average_price"),
        F.min("price").alias("minimum_price"),
        F.max("price").alias("maximum_price")
    )
    .orderBy(F.desc("listing_count"))
)

# Preview Gold data
marketplace_gold.show(truncate=False)

# Write Gold Delta table
marketplace_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.marketplace_summary")