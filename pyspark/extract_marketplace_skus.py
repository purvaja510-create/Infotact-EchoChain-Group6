from pyspark.sql import functions as F

# Read cleaned marketplace data from Silver
marketplace_silver = spark.table(
    "workspace.silver.marketplace_listings"
)

# Extract marketplace SKU candidates from product titles.
# The logic supports both explicit brand names and common product aliases.
marketplace_sku_candidates = (
    marketplace_silver
    .withColumn(
        "title_lower",
        F.lower(F.col("product_title_clean"))
    )
    .withColumn(
        "extracted_model",
        F.when(
            F.col("title_lower").contains("iphone 13"),
            "A2633"
        )
        .when(
            F.col("title_lower").contains("iphone 7"),
            "A1660"
        )
        .when(
            F.col("title_lower").rlike(r"(?i)\blatitude\s+\d{4}\b"),
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bLatitude\s+\d{4}\b",
                0
            )
        )
        .when(
            F.col("title_lower").rlike(r"(?i)\bdc\d{5}\b"),
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bDC\d{5}\b",
                0
            )
        )
        .when(
            F.col("title_lower").rlike(r"(?i)\b\d{2}-[a-z]{2}\d{4}[a-z]{0,2}\b"),
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\b\d{2}-[A-Z]{2}\d{4}[A-Z]{0,2}\b",
                0
            )
        )
        .when(
            F.col("title_lower").rlike(r"(?i)\bnp[a-z0-9-]+\b"),
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bNP[A-Z0-9-]+\b",
                0
            )
        )
        .when(
            F.col("title_lower").rlike(r"(?i)\bmodel\s+\d+\b"),
            F.regexp_extract(
                "product_title_clean",
                r"(?i)\bModel\s+\d+\b",
                0
            )
        )
        .otherwise("")
    )
    .withColumn(
        "brand",
        F.when(
            F.col("extracted_model").rlike(r"(?i)^A\d{4}$"),
            "Apple"
        )
        .when(
            F.col("extracted_model").rlike(r"(?i)^Latitude\s+\d{4}$")
            | F.col("extracted_model").rlike(r"(?i)^DC\d{5}$"),
            "Dell"
        )
        .when(
            F.col("extracted_model").rlike(
                r"(?i)^\d{2}-[A-Z]{2}\d{4}[A-Z]{0,2}$"
            ),
            "HP"
        )
        .when(
            F.col("extracted_model").rlike(r"(?i)^NP[A-Z0-9-]+$"),
            "Samsung"
        )
        .when(
            F.col("extracted_model").rlike(r"(?i)^Model\s+\d+$"),
            "Sony"
        )
        .otherwise("Unknown")
    )
    .drop("title_lower")
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