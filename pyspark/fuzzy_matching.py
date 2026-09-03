from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read marketplace SKU candidates from Bronze
marketplace_candidates = spark.table(
    "workspace.bronze.marketplace_sku_candidates"
)

# Read the new 24-SKU master from the Databricks volume
sku_master = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/default/raw_data/sku_master.csv")
)

# Normalize fields for comparison
marketplace_candidates = (
    marketplace_candidates
    .withColumn("model_normalized", F.lower(F.trim("extracted_model")))
)

sku_master = (
    sku_master
    .withColumn("model_normalized", F.lower(F.trim("model")))
)

# Match marketplace candidates against SKUs from the same brand
candidate_matches = (
    marketplace_candidates.alias("m")
    .join(
        sku_master.alias("s"),
        F.lower(F.col("m.brand")) == F.lower(F.col("s.brand")),
        "inner"
    )
    .withColumn(
        "match_score",
        F.round(
            (
                1
                - (
                    F.levenshtein(
                        F.col("m.model_normalized"),
                        F.col("s.model_normalized")
                    )
                    / F.greatest(
                        F.length(F.col("m.model_normalized")),
                        F.length(F.col("s.model_normalized"))
                    )
                )
            ) * 100,
            2
        )
    )
)

# Select the highest-scoring SKU for each listing
best_matches = (
    candidate_matches
    .withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("m.listing_url")
            .orderBy(F.desc("match_score"))
        )
    )
    .filter(F.col("match_rank") == 1)
    .select(
        F.col("m.product_title").alias("product_title"),
        F.col("m.listing_url").alias("listing_url"),
        F.col("m.brand").alias("brand"),
        F.col("m.extracted_model").alias("extracted_model"),
        F.col("s.sku_id").alias("candidate_sku"),
        F.col("s.model").alias("matched_model"),
        F.col("match_score")
    )
)

# Classify match confidence
best_matches = (
    best_matches
    .withColumn(
        "match_status",
        F.when(F.col("match_score") >= 90, "Strong Match")
         .when(F.col("match_score") >= 70, "Possible Match")
         .otherwise("Weak Match")
    )
)

# Preview matching results
best_matches.orderBy(
    F.desc("match_score")
).show(50, truncate=False)

# Save marketplace-to-SKU matching results
best_matches.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.bronze.marketplace_sku_matches")