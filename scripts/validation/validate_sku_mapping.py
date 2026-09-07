"""
Validate EchoChain marketplace-to-official SKU mappings.

The validation checks:
- Marketplace match records exist
- Candidate SKUs exist
- Marketplace matches have no missing candidate SKUs
- Official mappings reference valid candidate SKUs
- Official SKUs exist in the internal BOM
- Strong-match records are available for the lifecycle pipeline
"""

from pyspark.sql import functions as F


MARKETPLACE_MATCHES = (
    "workspace.bronze.marketplace_sku_matches"
)

OFFICIAL_MAPPING = (
    "workspace.bronze.marketplace_official_sku_mapping"
)

BOM = "workspace.bronze.bom"


def validate_mapping(spark):

    matches = spark.table(MARKETPLACE_MATCHES)
    official = spark.table(OFFICIAL_MAPPING)
    bom = spark.table(BOM)

    # Basic counts
    total_match_rows = matches.count()

    total_candidate_skus = (
        matches
        .select("candidate_sku")
        .distinct()
        .count()
    )

    strong_match_rows = (
        matches
        .filter(F.col("match_status") == "Strong Match")
        .count()
    )

    official_mapping_rows = official.count()

    # Check for missing candidate SKUs
    unmatched_rows = (
        matches
        .filter(F.col("candidate_sku").isNull())
        .count()
    )

    # Check that every official mapping has a valid
    # candidate SKU in the marketplace matches
    invalid_candidate_mappings = (
        official
        .join(
            matches
            .select("candidate_sku")
            .distinct(),
            on="candidate_sku",
            how="left_anti"
        )
        .count()
    )

    # Check that every official SKU exists in the BOM
    invalid_official_skus = (
        official
        .join(
            bom
            .select(
                F.col("sku")
                .alias("official_sku")
            )
            .distinct(),
            on="official_sku",
            how="left_anti"
        )
        .count()
    )

    # Check for duplicate candidate-to-official mappings
    duplicate_mappings = (
        official
        .groupBy("candidate_sku")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("EchoChain Marketplace SKU Mapping Validation")
    print("----------------------------------------------")
    print(f"Marketplace match rows: {total_match_rows}")
    print(f"Candidate SKUs: {total_candidate_skus}")
    print(f"Strong match rows: {strong_match_rows}")
    print(f"Verified official mappings: {official_mapping_rows}")
    print(f"Missing candidate SKUs: {unmatched_rows}")
    print(
        f"Invalid candidate mappings: "
        f"{invalid_candidate_mappings}"
    )
    print(
        f"Official SKUs missing from BOM: "
        f"{invalid_official_skus}"
    )
    print(
        f"Duplicate candidate mappings: "
        f"{duplicate_mappings}"
    )

    # Validation result
    if (
        total_match_rows > 0
        and total_candidate_skus > 0
        and strong_match_rows > 0
        and official_mapping_rows > 0
        and unmatched_rows == 0
        and invalid_candidate_mappings == 0
        and invalid_official_skus == 0
        and duplicate_mappings == 0
    ):
        print("Mapping status: PASSED")
    else:
        print("Mapping status: REVIEW REQUIRED")


# Run this function from a Databricks notebook with:
#
# validate_mapping(spark)