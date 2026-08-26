from databricks.connect import DatabricksSession
from pyspark.sql.functions import col

spark=(
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")

bronze_df = spark.table(
    "workspace.bronze.ebay_electronics"
    )

print("Bronze records", bronze_df.count())

# ---------------------------------------------------------
# Read Bronze Table
# ---------------------------------------------------------

bronze_df = spark.table(
    "workspace.bronze.ebay_electronics"
)

print("Bronze records:", bronze_df.count())


# ---------------------------------------------------------
# Prepare Silver Data
# ---------------------------------------------------------

silver_df = (
    bronze_df

    # Required fields must not be null
    .dropna(
        subset=[
            "product_title",
            "price",
            "currency",
            "condition",
            "listing_url"
        ]
    )

    # Remove duplicate listings
    .dropDuplicates(
        ["listing_url"]
    )

    # Correct datatypes
    .withColumn(
        "price",
        col("price").cast("double")
    )

    .withColumn(
        "scraped_at",
        col("scraped_at").cast("timestamp")
    )
)


# ---------------------------------------------------------
# Create Silver Schema
# ---------------------------------------------------------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")


# ---------------------------------------------------------
# Write Silver Delta Table
# ---------------------------------------------------------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.silver.ebay_electronics_clean"
    )
)

print("Silver Delta table created successfully")


# ---------------------------------------------------------
# Verify Silver Table
# ---------------------------------------------------------

spark.sql("""
SELECT *
FROM workspace.silver.ebay_electronics_clean
LIMIT 10
""").show(truncate=False)


spark.sql("""
SELECT COUNT(*) AS total_records
FROM workspace.silver.ebay_electronics_clean
""").show()


# ---------------------------------------------------------
# Check Silver Schema
# ---------------------------------------------------------

silver_df.printSchema()