from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read marketplace data from Silver
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Step 1: Normalize marketplace product titles
marketplace_normalized = (
    marketplace_silver
    .withColumn(
        "normalized_title",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.col("product_title"),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )
)

# Step 2: Create the pristine internal SKU reference
sku_reference = (
    spark.table("workspace.bronze.bom")
    .select(
        "sku",
        "brand",
        "model"
    )
    .distinct()
    .withColumn(
        "normalized_model",
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.col("model"),
                    r"[^a-zA-Z0-9]+",
                    " "
                )
            )
        )
    )
)

# Step 3: Compare marketplace listings against internal SKUs
fuzzy_matches = (
    marketplace_normalized
    .crossJoin(sku_reference)
    .withColumn(
        "model_match_score",
        F.round(
            (
                1
                - (
                    F.levenshtein(
                        F.col("normalized_title"),
                        F.col("normalized_model")
                    )
                    / F.greatest(
                        F.length(F.col("normalized_title")),
                        F.length(F.col("normalized_model"))
                    )
                )
            ) * 100,
            2
        )
    )
)

# Step 4: Keep the best SKU match for each marketplace listing
best_matches = (
    fuzzy_matches
    .withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("product_title")
            .orderBy(F.desc("model_match_score"))
        )
    )
    .filter(F.col("match_rank") == 1)
)

# Step 5: Display the best matches
best_matches.select(
    "product_title",
    "sku",
    "brand",
    "model",
    "model_match_score"
).orderBy(
    F.desc("model_match_score")
).show(50, truncate=False)
