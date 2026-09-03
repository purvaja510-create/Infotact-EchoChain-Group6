from databricks.connect import DatabricksSession

from pyspark.sql.functions import (
    col,
    round as spark_round,
    current_timestamp
)


# CONNECT TO DATABRICKS

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# READ FINAL SILVER TABLE

silver_df = spark.table(
    "workspace.silver.marketplace_sku_matched"
)

print(
    "\nSilver records:",
    silver_df.count()
)

print("\nSilver Schema:")
silver_df.printSchema()