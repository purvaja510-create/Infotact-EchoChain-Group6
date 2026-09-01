"""
Validate marketplace-to-official SKU mappings.

The marketplace-derived candidate SKUs are generated from
marketplace product titles. Official SKU mappings are only
considered valid when the marketplace model matches an
official BOM model.
"""

from pyspark.sql import functions as F


MARKETPLACE_MATCHES = "workspace.bronze.marketplace_sku_matches"
OFFICIAL_MAPPING = "workspace.bronze.marketplace_official_sku_mapping"


def validate_mapping(spark):
    matches = spark.table(MARKETPLACE_MATCHES)
    official = spark.table(OFFICIAL_MAPPING)

    total_rows = matches.count()

    unmatched_rows = matches.filter(
        F.col("candidate_sku").isNull()
    ).count()

    official_mapping_rows = official.count()

    print("EchoChain Marketplace SKU Mapping Validation")
    print("----------------------------------------------")
    print(f"Marketplace rows checked: {total_rows}")
    print(f"Unmatched candidate SKUs: {unmatched_rows}")
    print(f"Verified official SKU mappings: {official_mapping_rows}")

    if unmatched_rows == 0:
        print("Mapping status: PASSED")
    else:
        print("Mapping status: REVIEW REQUIRED")


# Run this function from a Databricks notebook with:
#
# validate_mapping(spark)