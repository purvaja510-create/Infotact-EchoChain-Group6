from pyspark.sql import functions as F

# Read final marketplace dataset
marketplace_bronze = (
    spark.read
    .option("multiLine", "true")
    .json("/Volumes/workspace/default/raw_data/marketplace_electronics_final.json")
)

# Add Bronze ingestion metadata
marketplace_bronze = (
    marketplace_bronze
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("pipeline_layer", F.lit("bronze"))
)

# Preview Bronze data
marketplace_bronze.printSchema()
marketplace_bronze.show(20, truncate=False)

# Write Bronze Delta table
marketplace_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.bronze.marketplace_listings")