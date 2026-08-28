from databricks.connect import DatabricksSession
import pandas as pd

from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    levenshtein,
    length,
    greatest,
    lit,
    row_number,
    round,
    when
)

from pyspark.sql.window import Window


# =========================================================
# CONNECT TO DATABRICKS
# =========================================================

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# =========================================================
# READ SILVER EBAY DATA
# =========================================================

marketplace_df = spark.table(
    "workspace.silver.ebay_electronics_clean"
)

print("Marketplace records:", marketplace_df.count())


# =========================================================
# READ SKU MASTER
# =========================================================

local_sku_path = r"C:\\Users\\bhanu\\OneDrive\\Desktop\\Echo\\Infotact-EchoChain-Group6\\data\\raw\\sku_master.csv"

sku_pdf = pd.read_csv(local_sku_path)

sku_master_df = spark.createDataFrame(sku_pdf)

print("SKU master records:", sku_master_df.count())

sku_master_df.printSchema()
sku_master_df.show(10, truncate=False)


# =========================================================
# CLEAN EBAY PRODUCT TITLE
# =========================================================

marketplace_df = marketplace_df.withColumn(
    "marketplace_title_clean",
    trim(
        regexp_replace(
            lower(col("product_title")),
            r"[^a-zA-Z0-9\s]",
            " "
        )
    )
)

marketplace_df = marketplace_df.withColumn(
    "marketplace_title_clean",
    regexp_replace(
        col("marketplace_title_clean"),
        r"\s+",
        " "
    )
)


# =========================================================
# CLEAN SKU MASTER PRODUCT NAME
# =========================================================
# Expected SKU master columns:
#
# sku_id
# brand
# model
# product_name
#
# Example:
# APPLE-A1660,Apple,iPhone 7,Apple iPhone 7
# DELL-5491,Dell,Latitude 5491,Dell Latitude 5491


sku_master_df = sku_master_df.withColumn(
    "master_title_clean",
    trim(
        regexp_replace(
            lower(col("product_name")),
            r"[^a-zA-Z0-9\s]",
            " "
        )
    )
)

sku_master_df = sku_master_df.withColumn(
    "master_title_clean",
    regexp_replace(
        col("master_title_clean"),
        r"\s+",
        " "
    )
)


# =========================================================
# COMPARE EBAY LISTINGS WITH SKU MASTER
# =========================================================
# Each eBay listing is compared with every SKU master record.
# This is fine for our current small project dataset.

comparison_df = marketplace_df.crossJoin(
    sku_master_df
)


# =========================================================
# CALCULATE LEVENSHTEIN DISTANCE
# =========================================================

comparison_df = comparison_df.withColumn(
    "edit_distance",
    levenshtein(
        col("marketplace_title_clean"),
        col("master_title_clean")
    )
)


# =========================================================
# CALCULATE MAXIMUM STRING LENGTH
# =========================================================

comparison_df = comparison_df.withColumn(
    "max_length",
    greatest(
        length(col("marketplace_title_clean")),
        length(col("master_title_clean"))
    )
)


# =========================================================
# CALCULATE FUZZY MATCH SCORE
# =========================================================
# Score:
#
# 100 = exact match
# 80+ = strong match
# 60-79 = possible match
# below 60 = weak match

comparison_df = comparison_df.withColumn(
    "match_score",
    round(
        (
            lit(1.0) -
            (
                col("edit_distance") /
                col("max_length")
            )
        ) * 100,
        2
    )
)


# =========================================================
# FIND BEST SKU FOR EACH EBAY LISTING
# =========================================================

window_spec = (
    Window
    .partitionBy("listing_url")
    .orderBy(col("match_score").desc())
)

matched_df = comparison_df.withColumn(
    "match_rank",
    row_number().over(window_spec)
)


# =========================================================
# KEEP ONLY BEST MATCH
# =========================================================

best_matches_df = (
    matched_df
    .filter(col("match_rank") == 1)
)


# =========================================================
# DISPLAY FUZZY MATCHING RESULTS
# =========================================================

print("\nBest SKU Matches:")

best_matches_df.select(
    "product_title",
    "sku_id",
    "brand",
    "model",
    "product_name",
    "match_score"
).show(
    50,
    truncate=False
)


# =========================================================
# ADD MATCH QUALITY
# =========================================================

from pyspark.sql.functions import when

best_matches_df = best_matches_df.withColumn(
    "match_quality",

    when(
        col("match_score") >= 80,
        "Strong Match"
    )
    .when(
        col("match_score") >= 60,
        "Possible Match"
    )
    .otherwise(
        "Weak Match"
    )
)


# =========================================================
# DISPLAY FINAL RESULTS
# =========================================================

print("\nFinal Fuzzy Matching Results:")

best_matches_df.select(
    "product_title",
    "sku_id",
    "brand",
    "model",
    "product_name",
    "match_score",
    "match_quality"
).show(
    50,
    truncate=False
)


# =========================================================
# MATCH QUALITY SUMMARY
# =========================================================

print("\nMatch Quality Summary:")

best_matches_df.groupBy(
    "match_quality"
).count().show()


# =========================================================
# TOTAL MATCHED RECORDS
# =========================================================

print(
    "Total marketplace records:",
    marketplace_df.count()
)

print(
    "Total matched records:",
    best_matches_df.count()
)


print("\nFuzzy matching completed successfully")