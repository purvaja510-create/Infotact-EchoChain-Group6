from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    count,
    avg,
    concat_ws
)
from rapidfuzz.fuzz import ratio
import csv
import os


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("EchoChain Member 3 - PySpark") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. INPUT DATA - BRONZE SOURCE
# ============================================================

input_file = "../data/raw/marketplace_listings.json"

print("\n===== LOADING BRONZE DATA =====")

df = spark.read \
    .option("multiLine", True) \
    .json(input_file)

print("Bronze data loaded successfully.")


# ============================================================
# 3. SHOW ORIGINAL DATA
# ============================================================

print("\n===== ORIGINAL DATA =====")

df.show(10, truncate=False)

print("\n===== ORIGINAL SCHEMA =====")

df.printSchema()


# ============================================================
# 4. DATA CLEANING
# ============================================================

print("\n===== CLEANING DATA =====")

# Remove duplicate records
df = df.dropDuplicates()

# Remove rows with null values
df = df.dropna()

# Clean text columns
text_columns = [
    "listing_id",
    "brand",
    "model",
    "category",
    "condition",
    "currency",
    "seller",
    "location",
    "description"
]

for column_name in text_columns:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )


# ============================================================
# 5. STANDARDIZE DATA
# ============================================================

if "brand" in df.columns:
    df = df.withColumn(
        "brand",
        trim(col("brand"))
    )

if "category" in df.columns:
    df = df.withColumn(
        "category",
        lower(trim(col("category")))
    )

if "condition" in df.columns:
    df = df.withColumn(
        "condition",
        lower(trim(col("condition")))
    )


# ============================================================
# 6. PRICE TRANSFORMATION
# ============================================================

if "price" in df.columns:

    df = df.withColumn(
        "price",
        col("price").cast("double")
    )

    df = df.withColumn(
        "price_category",
        when(col("price") < 10000, "Low")
        .when(col("price") < 40000, "Medium")
        .otherwise("High")
    )


# ============================================================
# 7. CREATE PRODUCT NAME
# ============================================================

df = df.withColumn(
    "product_name",
    concat_ws(
        " ",
        col("brand"),
        col("model")
    )
)


print("\n===== CLEANED & TRANSFORMED DATA =====")

df.show(10, truncate=False)


# ============================================================
# 8. FUZZY MATCHING
# ============================================================

print("\n===== FUZZY MATCHING =====")

# Collect only required columns for matching
matching_rows = df.select(
    "listing_id",
    "brand",
    "model",
    "product_name"
).collect()

matches = []

for current_row in matching_rows:

    current_id = current_row["listing_id"]
    current_product = current_row["product_name"]

    best_match_id = None
    best_score = 0.0

    for candidate_row in matching_rows:

        candidate_id = candidate_row["listing_id"]

        # Don't compare a listing with itself
        if current_id == candidate_id:
            continue

        candidate_product = candidate_row["product_name"]

        score = ratio(
            current_product.lower(),
            candidate_product.lower()
        )

        if score > best_score:

            best_score = score
            best_match_id = candidate_id

    matches.append(
        (
            current_id,
            best_match_id,
            round(float(best_score), 2)
        )
    )


# Create Spark DataFrame for fuzzy matching results

match_df = spark.createDataFrame(
    matches,
    [
        "listing_id",
        "matched_listing_id",
        "match_score"
    ]
)


# Join fuzzy matching results

df = df.join(
    match_df,
    on="listing_id",
    how="left"
)


# Match classification

df = df.withColumn(
    "match_status",
    when(
        col("match_score") >= 90,
        "Strong Match"
    )
    .when(
        col("match_score") >= 70,
        "Possible Match"
    )
    .otherwise(
        "No Strong Match"
    )
)


print("\n===== FUZZY MATCHING RESULTS =====")

df.select(
    "listing_id",
    "brand",
    "model",
    "matched_listing_id",
    "match_score",
    "match_status"
).show(20, truncate=False)


# ============================================================
# 9. SILVER TABLE
# ============================================================

print("\n===== CREATING SILVER TABLE =====")

silver_df = df.select(
    "listing_id",
    "brand",
    "model",
    "product_name",
    "category",
    "condition",
    "price",
    "currency",
    "seller",
    "location",
    "description",
    "price_category",
    "matched_listing_id",
    "match_score",
    "match_status"
)

print("\n===== SILVER TABLE =====")

silver_df.show(20, truncate=False)


# ============================================================
# 10. GOLD TABLE
# ============================================================

print("\n===== CREATING GOLD TABLE =====")

gold_df = silver_df.groupBy(
    "category"
).agg(
    count("*").alias("total_listings"),
    avg("price").alias("average_price")
).orderBy(
    col("total_listings").desc()
)


print("\n===== GOLD TABLE =====")

gold_df.show(truncate=False)


# ============================================================
# 11. CREATE OUTPUT DIRECTORIES
# ============================================================

project_root = os.path.abspath("..")

silver_folder = os.path.join(
    project_root,
    "data",
    "processed",
    "silver"
)

gold_folder = os.path.join(
    project_root,
    "data",
    "processed",
    "gold"
)

os.makedirs(
    silver_folder,
    exist_ok=True
)

os.makedirs(
    gold_folder,
    exist_ok=True
)


# ============================================================
# 12. SAVE SILVER TABLE
# ============================================================

print("\n===== SAVING SILVER TABLE =====")

silver_rows = silver_df.collect()

silver_file = os.path.join(
    silver_folder,
    "marketplace_silver.csv"
)

with open(
    silver_file,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow(
        silver_df.columns
    )

    for row in silver_rows:
        writer.writerow(
            row
        )


# ============================================================
# 13. SAVE GOLD TABLE
# ============================================================

print("\n===== SAVING GOLD TABLE =====")

gold_rows = gold_df.collect()

gold_file = os.path.join(
    gold_folder,
    "marketplace_gold.csv"
)

with open(
    gold_file,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow(
        gold_df.columns
    )

    for row in gold_rows:
        writer.writerow(
            row
        )


# ============================================================
# 14. FINAL STATUS
# ============================================================

print("\n==========================================")
print("       ECHOCHAIN MEMBER 3 COMPLETE")
print("==========================================")

print("\nSilver table saved at:")
print(silver_file)

print("\nGold table saved at:")
print(gold_file)

print("\nPipeline:")
print("Bronze JSON")
print("    ↓")
print("PySpark Cleaning")
print("    ↓")
print("Transformation")
print("    ↓")
print("Fuzzy Matching")
print("    ↓")
print("Silver Table")
print("    ↓")
print("Gold Table")
print("    ↓")
print("Power BI")


# ============================================================
# 15. STOP SPARK
# ============================================================

spark.stop()

print("\nSpark session stopped successfully.")
print("Member 3 PySpark processing completed.")