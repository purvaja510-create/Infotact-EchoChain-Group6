from databricks.connect import DatabricksSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    when,
    current_timestamp
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

print("Bronze records:", bronze_df.count())


# ---------------------------------------------------------
# 1. Check Null Values
# ---------------------------------------------------------

important_columns = [
    "product_title",
    "price",
    "currency",
    "condition",
    "listing_url"
]

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
# 2. Remove Rows Missing Important Fields
# ---------------------------------------------------------

clean_df = bronze_df.dropna(
    subset=important_columns
)


# ---------------------------------------------------------
# 3. Clean Product Title
# ---------------------------------------------------------

clean_df = clean_df.withColumn(
    "product_title_clean",
    trim(
        regexp_replace(
            lower(col("product_title")),
            r"\s+",
            " "
        )
    )
)


# ---------------------------------------------------------
# 4. Convert Price to Numeric
# ---------------------------------------------------------

clean_df = clean_df.withColumn(
    "price_numeric",
    col("price").cast("double")
)


# ---------------------------------------------------------
# 5. Standardize Condition
# ---------------------------------------------------------

clean_df = clean_df.withColumn(
    "condition_clean",
    when(
        lower(col("condition")).contains("new"),
        "New"
    )
    .when(
        lower(col("condition")).contains("refurbished"),
        "Refurbished"
    )
    .when(
        lower(col("condition")).contains("used"),
        "Used"
    )
    .when(
        lower(col("condition")).contains("pre-owned"),
        "Used"
    )
    .otherwise("Other")
)


# ---------------------------------------------------------
# 6. Remove Duplicate Listings
# ---------------------------------------------------------

clean_df = clean_df.dropDuplicates(
    ["listing_url"]
)


# ---------------------------------------------------------
# 7. Add Cleaning Timestamp
# ---------------------------------------------------------

clean_df = clean_df.withColumn(
    "cleaned_at",
    current_timestamp()
)


# ---------------------------------------------------------
# Check Clean Data
# ---------------------------------------------------------

print(
    "Clean records:",
    clean_df.count()
)

clean_df.show(
    10,
    truncate=False
)