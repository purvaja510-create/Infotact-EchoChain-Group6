from databricks.connect import DatabricksSession
from pyspark.sql.functions import (
    col,
    lower,
    when,
    regexp_extract,
    lit,
    concat_ws,
    upper,
    trim,
    regexp_replace
)


# Connect to Databricks

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("bhanuprasadpujari2000")
    .getOrCreate()
)

print("Databricks connected successfully")


# Read Silver Table

silver_df = spark.table(
    "workspace.silver.ebay_electronics_clean"
)

print("\nSilver records:", silver_df.count())


# Extract Brand

sku_df = silver_df.withColumn(
    "brand",

    when(
        lower(col("product_title")).contains("apple"),
        "Apple"
    )
    .when(
        lower(col("product_title")).contains("samsung"),
        "Samsung"
    )
    .when(
        lower(col("product_title")).contains("sony"),
        "Sony"
    )
    .when(
        lower(col("product_title")).contains("dell"),
        "Dell"
    )
    .when(
        lower(col("product_title")).contains("hp"),
        "HP"
    )
    .otherwise("Unknown")
)


# APPLE MODEL
# Example: A1660


sku_df = (
    sku_df

    # Apple: A1660, A2633 etc.
    .withColumn(
        "apple_model",
        regexp_extract(
            col("product_title"),
            r"\b[Aa]\d{4}\b",
            0
        )
    )

    # Dell: Latitude 5491
    .withColumn(
        "dell_latitude",
        regexp_extract(
            col("product_title"),
            r"(?i)(Latitude\s+\d{4})",
            1
        )
    )

    # Dell: DC15250
    .withColumn(
        "dell_dc",
        regexp_extract(
            col("product_title"),
            r"(?i)\b(DC\d{5})\b",
            1
        )
    )

    # HP: 14-em0002wm, 15-fb3093dx etc.
    .withColumn(
        "hp_model",
        regexp_extract(
            col("product_title"),
            r"(?i)\b(\d{2}-[a-z]{2}\d{4}[a-z]{0,2})\b",
            1
        )
    )

    # Samsung: NP750XQB-KA2US
    .withColumn(
        "samsung_model",
        regexp_extract(
            col("product_title"),
            r"(?i)\b(NP[A-Z0-9-]+)\b",
            1
        )
    )

    # Sony: Model 61
    .withColumn(
        "sony_model",
        regexp_extract(
            col("product_title"),
            r"(?i)(Model\s+\d+)",
            1
        )
    )
)

# CREATE FINAL MODEL COLUMN

sku_df = sku_df.withColumn(
    "model",

    when(
        col("brand") == "Apple",
        col("apple_model")
    )
    .when(
        col("brand") == "Dell",
        when(
            trim(col("dell_latitude")) != "",
            col("dell_latitude")
        ).otherwise(col("dell_dc"))
    )
    .when(
        col("brand") == "HP",
        col("hp_model")
    )
    .when(
        col("brand") == "Samsung",
        col("samsung_model")
    )
    .when(
        col("brand") == "Sony",
        col("sony_model")
    )
)


# CONVERT EMPTY MODEL TO NULL

sku_df = sku_df.withColumn(
    "model",
    when(
        col("model") == "",
        None
    ).otherwise(col("model"))
)


#CHECK EXTRACTED  RESULT

print("\n SKU Extracted Results:")

sku_df.select(
    "product_title",
    "brand",
    "model",
    "data_source",
    "is_synthetic"
).show(50, truncate=False)


# Extraction Statistics

print("\nRecords by Brand:")

sku_df.groupBy(
    "brand"
).count().orderBy("brand").show()


print("\nModel Extraction Status:")

sku_df.select(
    when(
        col("model").isNotNull(),
        "Model Extracted"
    ).otherwise(
        "Model Missing"
    ).alias("status")
).groupBy("status").count().show()



# CREATE SKU MASTER
# Only keep products where model was successfully extracted

sku_master_df = (
    sku_df
    .filter(
        (col("brand") != "Unknown") &
        col("model").isNotNull() &
        (trim(col("model")) != "")
    )
    .select(
        "brand",
        "model"
    )
    .dropDuplicates(
        ["brand", "model"]
    )
    .withColumn(
        "product_title",
        trim(
            concat_ws(
            col("brand") + " " + col("model")
        )
    )
)
.withColumn(
        "sku_id",
        upper(
            regexp_replace(
                concat_ws(
                    "-",
                    col("brand"),
                    col("model")
                ),
                r"\s+",
                "-"
            )
        )
    )

    .select(
        "sku_id",
        "brand",
        "model",
        "product_title"
    )
)

# Create Canonical SKU Master

sku_master_df = (
    sku_df

    .filter(
        (col("brand") != "Unknown") &
        col("model").isNotNull() &
        (trim(col("model")) != "")
    )

    .select(
        "brand",
        "model"
    )

    .dropDuplicates(
        ["brand", "model"]
    )

    # Canonical product name
    .withColumn(
        "product_name",
        trim(
            concat_ws(
                " ",
                col("brand"),
                col("model")
            )
        )
    )

    # Canonical SKU ID
    .withColumn(
        "sku_id",
        upper(
            regexp_replace(
                concat_ws(
                    "-",
                    col("brand"),
                    col("model")
                ),
                r"\s+",
                "-"
            )
        )
    )

    .select(
        "sku_id",
        "brand",
        "model",
        "product_name"
    )
)

# Save Local SKU Master CSV

from pathlib import Path

repo_root = Path(__file__).resolve().parents[2]

output_path = (
    repo_root /
    "data" /
    "raw" /
    "sku_master.csv"
)

sku_master_df.toPandas().to_csv(
    output_path,
    index=False
)

print(
    "\nMock SKU master saved to:",
    output_path
)

print(
    "\nSKU extraction completed successfully."
)