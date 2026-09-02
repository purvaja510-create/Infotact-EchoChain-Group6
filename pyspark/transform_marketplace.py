
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, count, avg
from rapidfuzz.fuzz import ratio
import csv
import os

# ============================================================
# 1. PROJECT PATH
# ============================================================

project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

input_file = os.path.join(
    project_root,
    "data",
    "raw",
    "marketplace_electronics_final.json"
)

# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("EchoChain Member 3 - PySpark")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 3. LOAD BRONZE DATA
# ============================================================

print("\n===== LOADING BRONZE DATA =====")

df = (
    spark.read
    .option("multiLine", True)
    .json(input_file)
)

print("Bronze data loaded successfully.")

print("\n===== ORIGINAL DATA =====")
df.show(10, truncate=False)

print("\n===== ORIGINAL SCHEMA =====")
df.printSchema()

# ============================================================
# 4. CLEAN DATA
# ============================================================

print("\n===== CLEANING DATA =====")

df = df.dropDuplicates()

# Remove rows only when important fields are missing
required_columns = [
    "listing_url",
    "product_title",
    "price",
    "search_category"
]

existing_required_columns = [
    column_name
    for column_name in required_columns
    if column_name in df.columns
]

df = df.dropna(
    subset=existing_required_columns
)

text_columns = [
    "condition",
    "currency",
    "data_source",
    "listing_url",
    "location",
    "product_title",
    "scraped_at",
    "search_category",
    "seller"
]

for column_name in text_columns:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )

# ============================================================
# 5. STANDARDIZE TEXT
# ============================================================

if "condition" in df.columns:
    df = df.withColumn(
        "condition",
        lower(trim(col("condition")))
    )

if "search_category" in df.columns:
    df = df.withColumn(
        "search_category",
        lower(trim(col("search_category")))
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

if "product_title" in df.columns:

    df = df.withColumn(
        "product_name",
        col("product_title")
    )

print("\n===== CLEANED & TRANSFORMED DATA =====")
df.show(10, truncate=False)

# ============================================================
# 8. FUZZY MATCHING
# ============================================================

print("\n===== FUZZY MATCHING =====")

matching_rows = df.select(
    "listing_url",
    "product_name"
).collect()

matches = []

for current_row in matching_rows:

    current_url = current_row["listing_url"]
    current_product = current_row["product_name"]

    best_match_url = None
    best_score = 0.0

    if current_product is None:

        matches.append(
            (current_url, None, 0.0)
        )

        continue

    for candidate_row in matching_rows:

        candidate_url = candidate_row["listing_url"]

        if current_url == candidate_url:
            continue

        candidate_product = candidate_row["product_name"]

        if candidate_product is None:
            continue

        score = ratio(
            str(current_product).lower(),
            str(candidate_product).lower()
        )

        if score > best_score:

            best_score = score
            best_match_url = candidate_url

    matches.append(
        (
            current_url,
            best_match_url,
            round(float(best_score), 2)
        )
    )

match_df = spark.createDataFrame(
    matches,
    [
        "listing_url",
        "matched_listing_url",
        "match_score"
    ]
)

df = df.join(
    match_df,
    on="listing_url",
    how="left"
)

# ============================================================
# 9. MATCH STATUS
# ============================================================

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
    "listing_url",
    "product_title",
    "matched_listing_url",
    "match_score",
    "match_status"
).show(20, truncate=False)

# ============================================================
# 10. CREATE SILVER TABLE
# ============================================================

print("\n===== CREATING SILVER TABLE =====")

silver_columns = [
    "listing_url",
    "condition",
    "currency",
    "data_source",
    "is_synthetic",
    "location",
    "price",
    "product_title",
    "product_name",
    "scraped_at",
    "search_category",
    "seller",
    "price_category",
    "matched_listing_url",
    "match_score",
    "match_status"
]

silver_columns = [
    column_name
    for column_name in silver_columns
    if column_name in df.columns
]

silver_df = df.select(
    *silver_columns
)

print("\n===== SILVER TABLE =====")
silver_df.show(20, truncate=False)

# ============================================================
# 11. CREATE GOLD TABLE
# ============================================================

print("\n===== CREATING GOLD TABLE =====")

gold_df = (
    silver_df
    .groupBy("search_category")
    .agg(
        count("*").alias("total_listings"),
        avg("price").alias("average_price")
    )
    .orderBy(
        col("total_listings").desc()
    )
)

print("\n===== GOLD TABLE =====")
gold_df.show(truncate=False)

# ============================================================
# 12. CREATE OUTPUT FOLDERS
# ============================================================

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
# 13. SAVE SILVER CSV
# ============================================================

print("\n===== SAVING SILVER TABLE =====")

silver_file = os.path.join(
    silver_folder,
    "marketplace_silver.csv"
)

silver_rows = silver_df.collect()

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
        writer.writerow(row)

print("Silver table saved successfully.")

# ============================================================
# 14. SAVE GOLD CSV
# ============================================================

print("\n===== SAVING GOLD TABLE =====")

gold_file = os.path.join(
    gold_folder,
    "marketplace_gold.csv"
)

gold_rows = gold_df.collect()

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
        writer.writerow(row)

print("Gold table saved successfully.")

# ============================================================
# 15. FINAL
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

spark.stop()

print("\nSpark session stopped successfully.")
print("Member 3 PySpark processing completed.")