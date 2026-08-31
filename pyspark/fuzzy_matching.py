from pyspark.sql import functions as F

# Read marketplace data from Silver
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Step 1: Normalize product titles
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

# Step 2: Add a temporary ID for comparison
marketplace_normalized = marketplace_normalized.withColumn(
    "listing_id_temp",
    F.monotonically_increasing_id()
)

# Step 3: Create two copies for pairwise comparison
left = marketplace_normalized.alias("left")
right = marketplace_normalized.alias("right")

# Step 4: Calculate fuzzy similarity using Levenshtein distance
fuzzy_matches = (
    left.crossJoin(right)
    .filter(F.col("left.listing_id_temp") < F.col("right.listing_id_temp"))
    .withColumn(
        "max_length",
        F.greatest(
            F.length(F.col("left.normalized_title")),
            F.length(F.col("right.normalized_title"))
        )
    )
    .withColumn(
        "match_score",
        F.round(
            (
                1 - (
                    F.levenshtein(
                        F.col("left.normalized_title"),
                        F.col("right.normalized_title")
                    ) / F.col("max_length")
                )
            ) * 100,
            2
        )
    )
)

# Step 5: Display the top fuzzy matches
fuzzy_matches.select(
    F.col("left.product_title").alias("product_title_1"),
    F.col("right.product_title").alias("product_title_2"),
    "match_score"
).orderBy(
    F.desc("match_score")
).show(30, truncate=False)