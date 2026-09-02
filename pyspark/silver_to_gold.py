from pyspark.sql import functions as F


# Read Silver marketplace data
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)


# Read marketplace-to-candidate SKU matches
sku_matches = spark.table(
    "workspace.bronze.marketplace_sku_matches"
)


# Read verified marketplace-to-official SKU mappings
official_mapping = spark.table(
    "workspace.bronze.marketplace_official_sku_mapping"
)


# Read internal BOM data
bom = spark.table(
    "workspace.bronze.bom"
).drop("_rescued_data")


# Read warranty and component failure data
warranty = spark.table(
    "workspace.bronze.warranty"
).drop("_rescued_data")


# Silver -> Gold lifecycle transformation
marketplace_product_health = (
    marketplace_silver
    .filter(F.col("price").isNotNull())
    .join(
        sku_matches,
        on="product_title",
        how="inner"
    )
    .join(
        official_mapping,
        on="candidate_sku",
        how="inner"
    )
    .join(
        bom,
        official_mapping["official_sku"] == bom["sku"],
        how="inner"
    )
    .join(
        warranty,
        (
            (bom["sku"] == warranty["sku"])
            & (bom["component"] == warranty["component"])
        ),
        how="left"
    )
    .select(
        marketplace_silver["product_title"],
        sku_matches["brand"],
        sku_matches["extracted_model"],
        sku_matches["candidate_sku"],
        official_mapping["official_sku"],
        marketplace_silver["price"],
        marketplace_silver["currency"],
        marketplace_silver["condition"],
        marketplace_silver["seller"],
        marketplace_silver["location"],
        marketplace_silver["listing_url"],
        marketplace_silver["scraped_at"],
        bom["component"],
        bom["component_type"],
        bom["manufacturing_cost"],
        warranty["failure_date"],
        warranty["failure_type"],
        warranty["failure_count"]
    )
)


# Gold data validation
marketplace_product_health = marketplace_product_health.filter(
    F.col("official_sku").isNotNull()
)


# Preview Gold data
marketplace_product_health.show(
    20,
    truncate=False
)


# Write Gold Delta table
marketplace_product_health.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.gold.marketplace_product_health"
    )