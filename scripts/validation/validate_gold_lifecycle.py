"""
Validate the EchoChain Gold lifecycle analytics table.

The validation checks:
- Gold table contains data
- Marketplace listing coverage
- SKU coverage
- Required lifecycle metrics are populated
- Component health scores are within the expected range
- Circularity scores are within the expected range
"""

from pyspark.sql import functions as F


GOLD_TABLE = "workspace.gold.marketplace_product_health"


def validate_gold_lifecycle(spark):
    gold = spark.table(GOLD_TABLE)

    total_rows = gold.count()
    listing_count = gold.select("listing_url").distinct().count()
    sku_count = gold.select("official_sku").distinct().count()

    null_sku = gold.filter(
        F.col("official_sku").isNull()
    ).count()

    null_price = gold.filter(
        F.col("secondary_market_price").isNull()
    ).count()

    null_manufacturing_cost = gold.filter(
        F.col("total_manufacturing_cost").isNull()
    ).count()

    null_circularity = gold.filter(
        F.col("circularity_score").isNull()
    ).count()

    null_depreciation = gold.filter(
        F.col("secondary_market_depreciation_pct").isNull()
    ).count()

    invalid_health_score = gold.filter(
        (F.col("component_health_score") < 0)
        | (F.col("component_health_score") > 100)
    ).count()

    invalid_circularity_score = gold.filter(
        (F.col("circularity_score") < 0)
        | (F.col("circularity_score") > 100)
    ).count()

    print("EchoChain Gold Lifecycle Validation")
    print("------------------------------------")
    print(f"Gold rows: {total_rows}")
    print(f"Marketplace listings: {listing_count}")
    print(f"Official SKUs: {sku_count}")
    print(f"Null official SKUs: {null_sku}")
    print(f"Null secondary-market prices: {null_price}")
    print(f"Null manufacturing costs: {null_manufacturing_cost}")
    print(f"Null circularity scores: {null_circularity}")
    print(f"Null depreciation values: {null_depreciation}")
    print(f"Invalid component health scores: {invalid_health_score}")
    print(f"Invalid circularity scores: {invalid_circularity_score}")

    if (
        total_rows > 0
        and listing_count > 0
        and sku_count > 0
        and null_sku == 0
        and null_price == 0
        and null_manufacturing_cost == 0
        and null_circularity == 0
        and null_depreciation == 0
        and invalid_health_score == 0
        and invalid_circularity_score == 0
    ):
        print("Gold lifecycle status: PASSED")
    else:
        print("Gold lifecycle status: REVIEW REQUIRED")


# Run this function from a Databricks notebook with:
# validate_gold_lifecycle(spark)