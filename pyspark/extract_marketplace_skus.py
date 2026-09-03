from pyspark.sql import functions as F

# Read cleaned marketplace data from Silver
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Extract brand and model identifiers from marketplace titles
marketplace_sku_candidates = (
    marketplace_silver
    .withColumn(
        "brand",
        F.when(F.lower("product_title_clean").contains("apple"), "Apple")
         .when(F.lower("product_title_clean").contains("samsung"), "Samsung")
         .when(F.lower("product_title_clean").contains("sony"), "Sony")
         .when(F.lower("product_title_clean").contains("dell"), "Dell")
         .when(F.lower("product_title_clean").contains("hp"), "HP")
         .otherwise("Unknown")
    )
    .withColumn(
        "extracted_model",
        F.when(
            F.col("brand") == "Apple",
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bA\d{4}\b",
                0
            )
        )
        .when(
            F.col("brand") == "Dell",
            F.coalesce(
                F.regexp_extract(
                    "product_title_clean",
                    r"(?i)\bLatitude\s+\d{4}\b",
                    0
                ),
                F.regexp_extract(
                    "product_title_clean",
                    r"(?i)\bDC\d{5}\b",
                    0
                )
            )
        )
        .when(
            F.col("brand") == "HP",
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\b\d{2}-[A-Z]{2}\d{4}[A-Z]{0,2}\b",
                0
            )
        )
        .when(
            F.col("brand") == "Samsung",
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bNP[A-Z0-9-]+\b",
                0
            )
        )
        .when(
            F.col("brand") == "Sony",
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bModel\s+\d+\b",
                0
            )
        )
        .otherwise("")
    )
    .filter(F.col("extracted_model") != "")
)

# Preview extracted marketplace identifiers
marketplace_sku_candidates.select(
    "product_title",
    "brand",
    "extracted_model"
).show(30, truncate=False)

# Save SKU candidates for the matching stage
marketplace_sku_candidates.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.bronze.marketplace_sku_candidates")