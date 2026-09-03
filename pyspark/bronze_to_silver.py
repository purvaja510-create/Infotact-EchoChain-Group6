from pyspark.sql import functions as F

# Read marketplace data from Bronze
marketplace_bronze = spark.table(
    "workspace.bronze.marketplace_listings"
)

# Bronze -> Silver transformation
marketplace_silver = (
    marketplace_bronze
    .drop("_rescued_data")
    .withColumn(
        "product_title_clean",
        F.trim(
            F.regexp_replace(
                F.col("product_title"),
                r"\s+",
                " "
            )
        )
    )
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("scraped_at", F.to_timestamp("scraped_at"))
    .filter(F.col("product_title_clean").isNotNull())
    .filter(F.col("price").isNotNull())
    .filter(F.col("price") > 0)
    .dropDuplicates(["listing_url"])
)

# Preview Silver data
marketplace_silver.printSchema()
marketplace_silver.show(20, truncate=False)

# Write Silver Delta table
marketplace_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.marketplace_listings")