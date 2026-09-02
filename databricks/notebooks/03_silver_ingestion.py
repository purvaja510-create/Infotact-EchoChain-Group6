from databricks.connect import DatabricksSession
from pyspark.sql.functions import (col,trim,
    lower,
    regexp_replace,
    current_timestamp,
    to_timestamp,
    lit
)
#Connect to Databricks

spark=(
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")

# Read Bronze Table

bronze_df = spark.table(
    "workspace.bronze.ebay_electronics"
)

print("\nBronze records:", bronze_df.count())


silver_df = (
    bronze_df

    # Essential fields must exist
    .filter(
        col("product_title").isNotNull() &
        col("price").isNotNull() &
        col("listing_url").isNotNull()
    )

    # Clean title
    .withColumn(
        "product_title_clean",
        lower(
            trim(
                regexp_replace(
                    col("product_title"),
                    r"\s+",
                    " "
                )
            )
        )
    )

    # Convert price to numeric
    .withColumn(
        "price_numeric",
        regexp_replace(
            col("price"),
            r"[^0-9.]",
            ""
        ).cast("double")
    )

    # Standardize condition
    .withColumn(
        "condition_clean",
        lower(
            trim(col("condition"))
        )
    )

    # Convert scraped timestamp
    .withColumn(
        "scraped_at_timestamp",
        to_timestamp(col("scraped_at"))
    )

    # Add Silver metadata
    .withColumn(
        "cleaned_at",
        current_timestamp()
    )

    .withColumn(
        "pipeline_layer",
        lit("pipeline_layer")
    )
)


# Remove Invalid Prices

silver_df = silver_df.filter(
    col("price_numeric") > 0
)


# Remove Duplicate Listings

silver_df = silver_df.dropDuplicates(
    ["listing_url"]
)


#  Select Final Silver Columns


silver_df = silver_df.select(
    "search_category",
    "product_title",
    "product_title_clean",

    "price",
    "price_numeric",
    "currency",

    "condition",
    "condition_clean",

    "seller",
    "location",

    "listing_url",

    "scraped_at",
    "scraped_at_timestamp",

    # IMPORTANT provenance
    "data_source",
    "is_synthetic",

    "ingested_at",
    "cleaned_at"
)

# Validate Before Writing

print("\nSilver records before writing:", silver_df.count())

print("\nSilver Schema:")
silver_df.printSchema()


print("\nRecords by Data Source:")

silver_df.groupBy(
    "data_source"
).count().show()


print("\nReal vs Synthetic:")

silver_df.groupBy(
    "is_synthetic"
).count().show()


# Create Silver Schema

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.silver"
)

# Write Silver Delta Table

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.silver.ebay_electronics_clean"
    )
)

print(
    "\nSilver table created successfully:"
    " workspace.silver.ebay_electronics_clean"
)

# Verify Silver Table

silver_check = spark.table(
    "workspace.silver.ebay_electronics_clean"
)

print(
    "\nTotal Silver records:",
    silver_check.count()
)


silver_check.select(
    "product_title",
    "product_title_clean",
    "price_numeric",
    "condition_clean",
    "data_source",
    "is_synthetic"
).show(
    20,
    truncate=False
)


print("\nSilver ingestion completed successfully.")