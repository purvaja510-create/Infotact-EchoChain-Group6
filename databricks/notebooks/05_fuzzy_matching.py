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
    row_number,
    when,
    round as spark_round
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# CONNECT TO DATABRICKS

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# READ SILVER EBAY DATA

marketplace_df = spark.table(
    "workspace.silver.ebay_electronics_clean"
)

print("\nMarketplace records:", marketplace_df.count())


# READ SKU MASTER

sku_master_path = (
    r"C:\Users\bhanu\OneDrive\Desktop\Echo"
    r"\Infotact-EchoChain-Group6"
    r"\data\raw\sku_master.csv"
)

sku_pdf = pd.read_csv(sku_master_path)

print("SKU master records:", len(sku_pdf))  

sku_master_df = spark.createDataFrame(sku_pdf)


# CLEAN MARKETPLACE TITLE

sku_master_df = sku_master_df.withColumn(
    "master_title_clean",
    lower(
        regexp_replace(
            (col("product_name")),
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

# IDENTIFY MARKETPLACE BRAND

marketplace_df = marketplace_df.withColumn(
    "market_brand",

    when(
        lower(col("product_title")).contains("apple"),
        "Apple"
    )
    .when(
        lower(col("product_title")).contains("dell"),
        "Dell"
    )
    .when(
        lower(col("product_title")).contains("hp"),
        "HP"
    )
    .when(
        lower(col("product_title")).contains("samsung"),
        "Samsung"
    )
    .when(
        lower(col("product_title")).contains("sony"),
        "Sony"
    )
    .otherwise("Unknown")
)

print("\nMarketplace Records by Brand:")

marketplace_df.groupBy(
    "market_brand"
).count().orderBy(
    "market_brand"
).show()


# Clean SKU Master Product Names

sku_master_df = sku_master_df.withColumn(
    "master_title_clean",
    lower(
        trim(
            regexp_replace(
                col("product_name"),
                r"[^a-zA-Z0-9\s-]",
                " "
            )
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


# Join Marketplace Listings to Same Brand SKUs

comparison_df = (
    marketplace_df.alias("market")
    .join(
        sku_master_df.alias("master"),

        col("market.market_brand")
        ==
        col("master.brand"),

        "inner"
    )
)

print(
    "\nCandidate comparisons:",
    comparison_df.count()
)

# CALCULATE LEVENSHTEIN DISTANCE

comparison_df = comparison_df.withColumn(
    "edit_distance",
    levenshtein(
        col("market.product_title_clean"),
        col("master.master_title_clean")
    )
)
# CHECK MARKETPLACE BRANDS

print("\nMarketplace Records by Brand:")

marketplace_df.groupBy(
    "market_brand"
).count().orderBy("market_brand").show()

# CALCULATE MAXIMUM STRING LENGTH

comparison_df = comparison_df.withColumn(
    "max_length",
    greatest(
        length(col("market.product_title_clean")),
        length(col("master.master_title_clean"))
    )
)

# Calculate Similarity Score

comparison_df = comparison_df.withColumn(
    "match_score",
    spark_round(
        (
            1 -
            (
                col("edit_distance") /
                col("max_length")
            )
        ) * 100,
        2
    )
)


# Rank Matches Per Marketplace Listing
 

window_spec = (
    Window
    .partitionBy(
        col("market.listing_url")
    )
    .orderBy(
        col("match_score").desc()
    )
)

ranked_df = comparison_df.withColumn(
    "match_rank",
    row_number().over(
        window_spec
    )
)

# Keep Best Match

best_matches_df = (
    ranked_df
    .filter(
        col("match_rank") == 1
    )
)


# Add Match Quality Category

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


# Display Results

print("\nBest Fuzzy Matches:")

best_matches_df.select(
    col("market.product_title")
        .alias("marketplace_title"),

    col("market.market_brand")
        .alias("brand"),

    col("master.sku_id")
        .alias("matched_sku_id"),

    col("master.product_name")
        .alias("matched_product"),

    "match_score",
    "match_quality",

    col("market.data_source")
        .alias("data_source"),

    col("market.is_synthetic")
        .alias("is_synthetic")

).orderBy(
    col("match_score").desc()
).show(
    50,
    truncate=False)


# MATCH QUALITY SUMMARY

print("\nMatch Quality Summary:")

best_matches_df.groupBy(
    "match_quality"
).count().show()


# Matched Records by Brand

print("\nMatched Records by Brand:")

best_matches_df.groupBy(
    col("market.market_brand")
        .alias("brand")
).count().orderBy(
    "brand"
).show()



# Totals


total_marketplace = (
    marketplace_df.count()
)

total_matched = (
    best_matches_df.count()
)

print(
    "\nTotal Marketplace Records:",
    total_marketplace
)

print(
    "Total Matched Records:",
    total_matched
)


print(
    "\nFuzzy matching completed successfully."
)