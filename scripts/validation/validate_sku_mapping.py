"""
Validate marketplace-to-official SKU mappings.

Marketplace candidate SKUs are generated from marketplace product titles.
Official SKU mappings are considered valid only when the candidate SKU
exists in the marketplace matches and the official SKU exists in the BOM.
"""

from pyspark.sql import functions as F


MARKETPLACE_MATCHES = "workspace.bronze.marketplace_sku_matches"
OFFICIAL_MAPPING = "workspace.bronze.marketplace_official_sku_mapping"
BOM = "workspace.bronze.bom"


def validate_mapping(spark):
    matches = spark.table(MARKETPLACE_MATCHES)
    official = spark.table(OFFICIAL_MAPPING)
    bom = spark.table(BOM)

    # Basic counts
    total_match_rows = matches.count()
    total_candidate_skus = matches.select("candidate_sku").distinct().count()
    official_mapping_rows = official.count()

    # Check for missing candidate SKUs
    unmatched_rows = matches.filter(
        F.col("candidate_sku").isNull()
    ).count()

    # Check that every official mapping has a valid candidate SKU
    invalid_candidate_mappings = (
        official
        .join(
            matches.select("candidate_sku").distinct(),
            on="candidate_sku",
            how="left_anti"
        )
        .count()
    )

    # Check that every official SKU exists in the BOM
    invalid_official_skus = (
        official
        .join(
            bom.select(F.col("sku").alias("official_sku")).distinct(),
            on="official_sku",
            how="left_anti"
        )
        .count()
    )

    print("EchoChain Marketplace SKU Mapping Validation")
    print("----------------------------------------------")
    print(f"Marketplace match rows: {total_match_rows}")
    print(f"Candidate SKUs: {total_candidate_skus}")
    print(f"Missing candidate SKUs: {unmatched_rows}")
    print(f"Verified official mappings: {official_mapping_rows}")
    print(f"Invalid candidate mappings: {invalid_candidate_mappings}")
    print(f"Official SKUs missing from BOM: {invalid_official_skus}")

    # Validation result
    if (
        total_match_rows > 0
        and total_candidate_skus > 0
        and unmatched_rows == 0
        and invalid_candidate_mappings == 0
        and invalid_official_skus == 0
    ):
        print("Mapping status: PASSED")
    else:
        print("Mapping status: REVIEW REQUIRED")


# Run this function from a Databricks notebook with:
#
# validate_mapping(spark)