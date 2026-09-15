from databricks.connect import DatabricksSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    when,
    current_timestamp,
    to_timestamp
)

# ---------------------------------------------------------
# Connect to Databricks
# ---------------------------------------------------------

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# ---------------------------------------------------------
# Read Bronze Table
# ---------------------------------------------------------

bronze_df = spark.table(
    "workspace.bronze.ebay_electronics"
)

print("\nBronze records:", bronze_df.count())

#Check  Schema
print("\nBronze Schema:")
bronze_df.printSchema()

# ---------------------------------------------------------
# Check Null Values
# ---------------------------------------------------------

important_columns = [
    "product_title",
    "price",
    "currency",
    "condition",
    "listing_url",
    "data_source",
    "is_synthetic"
]

print("\nNull Value Counts:")

for column_name in important_columns:

    null_count = (
        bronze_df
        .filter(col(column_name).isNull())
        .count()
    )

    print(
        f"{column_name} null values: {null_count}"
    )


# ---------------------------------------------------------
# Check Duplicate Listings
# ---------------------------------------------------------

duplicate_df = (
    bronze_df
    .groupBy("listing_url")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_df.count()

print(
    "\nDuplicate listing URLs:",
    duplicate_count
)


# ---------------------------------------------------------
# Clean Marketplace Data
# ---------------------------------------------------------

clean_df = (
    bronze_df

    # Remove records without essential information
    .filter(
        col("product_title").isNotNull() &
        col("price").isNotNull() &
        col("listing_url").isNotNull()
    )

    # Clean product title
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

    # Convert timestamp
    .withColumn(
        "scraped_at_timestamp",
        to_timestamp(col("scraped_at"))
    )

    # Cleaning timestamp
    .withColumn(
        "cleaned_at",
        current_timestamp()
    )
)


# ---------------------------------------------------------
# Remove Invalid Prices
# ---------------------------------------------------------

clean_df = clean_df.filter(
    col("price_numeric") > 0
)


# ---------------------------------------------------------
# Remove Duplicate Listings
# ---------------------------------------------------------

clean_df = clean_df.dropDuplicates(
    ["listing_url"]
)


# ---------------------------------------------------------
# Validate Cleaned Data
# ---------------------------------------------------------

print(
    "\nRecords after cleaning:",
    clean_df.count()
)


print("\nRecords by Data Source:")

clean_df.groupBy(
    "data_source"
).count().show()


print("\nSynthetic vs Real:")

clean_df.groupBy(
    "is_synthetic"
).count().show()


# ---------------------------------------------------------
# Preview Cleaned Data
# ---------------------------------------------------------

clean_df.select(
    "product_title",
    "product_title_clean",
    "price",
    "price_numeric",
    "condition",
    "condition_clean",
    "data_source",
    "is_synthetic"
).show(
    20,
    truncate=False
)


print(
    "\nBronze quality cleaning completed successfully"
)