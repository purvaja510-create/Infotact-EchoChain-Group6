from pyspark.sql import functions as F

# Read marketplace data from Silver
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Create a normalized product title
marketplace_normalized = (
    marketplace_silver
    .withColumn(
        "normalized_title",
        F.lower(
            F.regexp_replace(
                F.col("product_title"),
                r"[^a-zA-Z0-9]+",
                " "
            )
        )
    )
)

# Preview normalized titles
marketplace_normalized.select(
    "product_title",
    "normalized_title"
).show(20, truncate=False)