from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# EchoChain - Silver to Gold Lifecycle Analytics
# ============================================================

# Read Silver marketplace data
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Read marketplace-to-SKU fuzzy matches
sku_matches = spark.table(
    "workspace.bronze.marketplace_sku_matches"
)

# Read verified marketplace-to-official SKU mappings
official_mapping = spark.table(
    "workspace.bronze.marketplace_official_sku_mapping"
)

# Read synthetic internal BOM data
bom = spark.table(
    "workspace.bronze.bom"
)

# Read synthetic warranty data
warranty = spark.table(
    "workspace.bronze.warranty"
)


# ============================================================
# Prepare marketplace listings
# ============================================================

marketplace = (
    marketplace_silver
    .select(
        "product_title",
        "listing_url",
        "price",
        "currency",
        "condition",
        "seller",
        "location",
        "scraped_at"
    )
    .filter(F.col("price").isNotNull())
    .filter(F.col("price") > 0)
)


# ============================================================
# Prepare SKU matches
# ============================================================

matches = (
    sku_matches
    .filter(F.col("match_status") == "Strong Match")
    .select(
        "product_title",
        "listing_url",
        "brand",
        "extracted_model",
        "candidate_sku",
        "matched_model",
        "match_score"
    )
)


# ============================================================
# Prepare official SKU mapping
# ============================================================

mapping = (
    official_mapping
    .select(
        "candidate_sku",
        "official_sku"
    )
    .dropDuplicates(["candidate_sku"])
)


# ============================================================
# Prepare BOM
# ============================================================

bom_clean = (
    bom
    .select(
        "sku",
        "component",
        "component_type",
        F.col("manufacturing_cost").cast("double").alias(
            "manufacturing_cost"
        ),
        "currency"
    )
)


# ============================================================
# Prepare warranty data
# ============================================================

warranty_clean = (
    warranty
    .select(
        "sku",
        "component",
        "failure_date",
        "failure_type",
        F.col("failure_count").cast("int").alias(
            "failure_count"
        )
    )
)


# ============================================================
# Join marketplace -> fuzzy match -> official SKU
# ============================================================

marketplace_mapped = (
    marketplace.alias("m")
    .join(
        matches.alias("match"),
        F.col("m.listing_url") == F.col("match.listing_url"),
        "inner"
    )
    .join(
        mapping.alias("map"),
        F.col("match.candidate_sku") == F.col("map.candidate_sku"),
        "inner"
    )
    .select(
        F.col("m.product_title").alias("product_title"),
        F.col("m.listing_url").alias("listing_url"),
        F.col("match.brand").alias("brand"),
        F.col("match.extracted_model").alias("extracted_model"),
        F.col("match.candidate_sku").alias("candidate_sku"),
        F.col("map.official_sku").alias("official_sku"),
        F.col("match.matched_model").alias("matched_model"),
        F.col("match.match_score").alias("match_score"),
        F.col("m.price").alias("secondary_market_price"),
        F.col("m.currency").alias("currency"),
        F.col("m.condition").alias("condition"),
        F.col("m.seller").alias("seller"),
        F.col("m.location").alias("location"),
        F.col("m.scraped_at").alias("scraped_at")
    )
)


# ============================================================
# Join mapped marketplace listings with BOM
# ============================================================

marketplace_lifecycle = (
    marketplace_mapped.alias("m")
    .join(
        bom_clean.alias("b"),
        F.col("m.official_sku") == F.col("b.sku"),
        "inner"
    )
    .select(
        F.col("m.product_title").alias("product_title"),
        F.col("m.listing_url").alias("listing_url"),
        F.col("m.brand").alias("brand"),
        F.col("m.extracted_model").alias("extracted_model"),
        F.col("m.candidate_sku").alias("candidate_sku"),
        F.col("m.official_sku").alias("official_sku"),
        F.col("m.matched_model").alias("matched_model"),
        F.col("m.match_score").alias("match_score"),
        F.col("m.secondary_market_price").alias(
            "secondary_market_price"
        ),
        F.col("m.currency").alias("marketplace_currency"),
        F.col("m.condition").alias("condition"),
        F.col("m.seller").alias("seller"),
        F.col("m.location").alias("location"),
        F.col("m.scraped_at").alias("scraped_at"),
        F.col("b.component").alias("component"),
        F.col("b.component_type").alias("component_type"),
        F.col("b.manufacturing_cost").alias(
            "component_manufacturing_cost"
        ),
        F.col("b.currency").alias("bom_currency")
    )
)


# ============================================================
# Join warranty information
# ============================================================

marketplace_lifecycle = (
    marketplace_lifecycle.alias("l")
    .join(
        warranty_clean.alias("w"),
        (
            (F.col("l.official_sku") == F.col("w.sku"))
            & (F.col("l.component") == F.col("w.component"))
        ),
        "left"
    )
    .select(
        "l.*",
        F.col("w.failure_date").alias("failure_date"),
        F.col("w.failure_type").alias("failure_type"),
        F.coalesce(
            F.col("w.failure_count"),
            F.lit(0)
        ).alias("failure_count")
    )
)


# ============================================================
# Calculate component health score
# ============================================================

marketplace_lifecycle = (
    marketplace_lifecycle
    .withColumn(
        "component_health_score",
        F.greatest(
            F.lit(0),
            F.lit(100) - (
                F.col("failure_count") * F.lit(10)
            )
        )
    )
)


# ============================================================
# Calculate product-level lifecycle metrics
# ============================================================

component_window = Window.partitionBy(
    "official_sku",
    "listing_url"
)

marketplace_lifecycle = (
    marketplace_lifecycle
    .withColumn(
        "total_manufacturing_cost",
        F.sum(
            "component_manufacturing_cost"
        ).over(component_window)
    )
    .withColumn(
        "component_count",
        F.count(
            "component"
        ).over(component_window)
    )
    .withColumn(
        "circularity_score",
        F.round(
            F.avg(
                "component_health_score"
            ).over(component_window),
            2
        )
    )
)


# ============================================================
# Calculate secondary-market depreciation
#
# Formula:
# ((Manufacturing Cost - Secondary Price)
#  / Manufacturing Cost) * 100
#
# Positive value = depreciation
# Negative value = secondary price above manufacturing cost
# ============================================================

marketplace_lifecycle = (
    marketplace_lifecycle
    .withColumn(
        "secondary_market_depreciation_pct",
        F.round(
            (
                (
                    F.col("total_manufacturing_cost")
                    - F.col("secondary_market_price")
                )
                / F.col("total_manufacturing_cost")
            ) * 100,
            2
        )
    )
)


# ============================================================
# Add lifecycle classification
# ============================================================

marketplace_lifecycle = (
    marketplace_lifecycle
    .withColumn(
        "circularity_status",
        F.when(
            F.col("circularity_score") >= 80,
            "High Circularity"
        )
        .when(
            F.col("circularity_score") >= 60,
            "Medium Circularity"
        )
        .otherwise(
            "Low Circularity"
        )
    )
)


# ============================================================
# Final Gold columns
# ============================================================

marketplace_product_health = (
    marketplace_lifecycle
    .select(
        "product_title",
        "listing_url",
        "brand",
        "extracted_model",
        "candidate_sku",
        "official_sku",
        "matched_model",
        "match_score",
        "secondary_market_price",
        "marketplace_currency",
        "condition",
        "seller",
        "location",
        "scraped_at",
        "component",
        "component_type",
        "component_manufacturing_cost",
        "bom_currency",
        "failure_date",
        "failure_type",
        "failure_count",
        "component_health_score",
        "component_count",
        "total_manufacturing_cost",
        "circularity_score",
        "circularity_status",
        "secondary_market_depreciation_pct"
    )
)


# ============================================================
# Preview Gold data
# ============================================================

marketplace_product_health.orderBy(
    F.desc("circularity_score")
).show(
    30,
    truncate=False
)


# ============================================================
# Write Gold Delta table
# ============================================================

marketplace_product_health.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.marketplace_product_health"
    )