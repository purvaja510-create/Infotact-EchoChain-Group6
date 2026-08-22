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
file_path = "/Volumes/workspace/default/raw_data/ebay_electronics.json"

# Read JSON into Spark DataFrame
df = (
    spark.read
    .option("multiLine", "true")
    .json(file_path)
)

print("eBay data loaded successfully")

# Check records
df.show(10, truncate=False)

# Check number of records
print("Total records:", df.count())

# Check schema
df.printSchema()


#Create Bronze Schema
spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

#Add Bronze Metadata
bronze_df = (
    df
    .withColumn("source", lit("ebay"))
    .withColumn("ingested_at", current_timestamp())
)


#Write Bronze DataFrame to Delta Table
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.ebay_electronics")
)

print("Bronze Delta table created successfully")


#Verify Bronze Delta Table
result = spark.sql("""
SELECT *
FROM workspace.bronze.ebay_electronics
LIMIT 10
""")

result.show(truncate=False)


count_df = spark.sql("""
SELECT COUNT(*) AS total_records
FROM workspace.bronze.ebay_electronics
""")

count_df.show()