from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp,lit

# Connect VS Code to Databricks
spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")

# eBay JSON stored in Databricks Volume
input_path =  "/Volumes/workspace/default/raw_data/marketplace_electronics_final.json"

# Read JSON into Spark DataFrame
df = (
    spark.read
    .option("multiLine", "true")
    .json(input_path)
)

print("Marketplace  data loaded successfully")


# Check number of records
print("Total records:", df.count())



#Add Bronze Metadata
bronze_df = (
    df
    .withColumn(
        "ingested_at",
        current_timestamp()
    )
    .withColumn(
        "pipeline_layer",
        lit("bronze")
    )
)

print("\nBronze Schema:")

bronze_df.printSchema()

#Preview

bronze_df.select(
    "search_category",
    "product_title",
    "price",
    "currency",
    "condition",
    "data_source",
    "is_synthetic",
    "listing_url"
).show(
    10,
    truncate=False
)


#Create Bronze Schema

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

#Write Bronze DataFrame to Delta Table
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.bronze.ebay_electronics")
)

print("\nBronze table created successfully:"
    " workspace.bronze.ebay_electronics"
)


# Validate Bronze Table

bronze_check = spark.table(
    "workspace.bronze.ebay_electronics"
)

print(
    "\nTotal Bronze records:",
    bronze_check.count()
)


# Validate Source Distribution

print("\nRecords by Data Source:")

bronze_check.groupBy(
    "data_source"
).count().show()


print("\nSynthetic vs Real Records:")

bronze_check.groupBy(
    "is_synthetic"
).count().show()


print("\nBronze ingestion completed successfully.")