from databricks.connect import DatabricksSession

from pyspark.sql.functions import (
    col,
    lower,
    when,
    regexp_extract
)


# Connect to Databricks

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# Read Silver Table

silver_df = spark.table(
    "workspace.silver.ebay_electronics_clean"
)

print("Silver records:", silver_df.count())

silver_df.select(
    "product_title",
    "search_category",
    "price"
).show(20, truncate=False)


# Extract Brand

sku_df = silver_df.withColumn(
    "brand",

    when(
        lower(col("product_title")).contains("apple"),
        "Apple"
    )
    .when(
        lower(col("product_title")).contains("samsung"),
        "Samsung"
    )
    .when(
        lower(col("product_title")).contains("sony"),
        "Sony"
    )
    .when(
        lower(col("product_title")).contains("dell"),
        "Dell"
    )
    .when(
        lower(col("product_title")).contains("hp"),
        "HP"
    )
    .otherwise("Unknown")
)


# Extract SKU candidate such as A1660

sku_df = sku_df.withColumn(
    "sku_candidate",

    regexp_extract(
        col("product_title"),
        r"\b[Aa]\d{4}\b",
        0
    )
)


# Extract Dell model such as Latitude 5491

sku_df = sku_df.withColumn(
    "dell_model",

    regexp_extract(
        col("product_title"),
        r"(?i)(Latitude\s+\d{4})",
        1
    )
)


# Show results

sku_df.select(
    "product_title",
    "brand",
    "sku_candidate",
    "dell_model",
    "price"
).show(30, truncate=False)