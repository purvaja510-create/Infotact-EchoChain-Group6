from pyspark.sql import functions as F

# Read marketplace data from the Bronze Delta table
marketplace_bronze = spark.table(
    "workspace.bronze.marketplace_listings"
)

# Check the Bronze schema
marketplace_bronze.printSchema()

# Preview the Bronze records
marketplace_bronze.show(20, truncate=False)