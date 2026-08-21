from databricks.connect import DatabricksSession

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