
````markdown
# EchoChain Data Validation

This directory contains validation scripts for the EchoChain secondary-market
lifecycle analytics pipeline.

The validation scripts check data quality at different stages of the pipeline,
from the raw marketplace dataset through SKU matching and the final Gold
lifecycle analytics table.

## Validation Scripts

### 1. `validate_marketplace_data.py`

Validates the final marketplace dataset before downstream processing.

#### Checks Performed

- Required field validation
- Duplicate listing URL detection
- Price format validation
- Positive price validation
- Currency validation
- Product title validation
- Synthetic-data flag validation
- Record count reporting

#### Input

```text
data/raw/marketplace_electronics_final.json
````

#### Dataset

The final marketplace dataset contains **582 records**.

---

### 2. `validate_sku_mapping.py`

Validates marketplace SKU matching and the mapping from marketplace candidate
SKUs to official internal SKUs.

#### Checks Performed

* Marketplace match record count
* Candidate SKU count
* Strong-match record count
* Official SKU mapping count
* Missing candidate SKU detection
* Invalid candidate mapping detection
* Official SKU existence in the internal BOM
* Duplicate candidate-to-official mapping detection

#### Databricks Tables

```text
workspace.bronze.marketplace_sku_matches
workspace.bronze.marketplace_official_sku_mapping
workspace.bronze.bom
```

#### Current Pipeline Result

The marketplace SKU matching stage produced **575 Strong Match records**.

The official mapping is validated against the internal BOM before the
lifecycle analytics stage.

---

### 3. `validate_gold_lifecycle.py`

Validates the final Gold lifecycle analytics table.

#### Checks Performed

* Gold row count
* Marketplace listing coverage
* Official SKU coverage
* Null official SKU detection
* Null secondary-market price detection
* Null manufacturing-cost detection
* Null circularity-score detection
* Null depreciation detection
* Component health score range validation
* Circularity score range validation

#### Databricks Table

```text
workspace.gold.marketplace_product_health
```

#### Current Validation Result

The Gold lifecycle table contains:

```text
Gold rows: 2300
Marketplace listings: 575
Official SKUs: 24
Null official SKUs: 0
Null secondary-market prices: 0
Null manufacturing costs: 0
Null circularity scores: 0
Null depreciation values: 0
Invalid component health scores: 0
Invalid circularity scores: 0

Gold lifecycle status: PASSED
```

The **2,300 Gold rows** represent 575 marketplace listings joined with
4 BOM components per product.

## Pipeline Validation Flow

```text
Final Marketplace Dataset
        |
        v
validate_marketplace_data.py
        |
        v
Bronze / Silver Marketplace Data
        |
        v
SKU Extraction + Fuzzy Matching
        |
        v
validate_sku_mapping.py
        |
        v
Official SKU + BOM + Warranty
        |
        v
Gold Lifecycle Analytics
        |
        v
validate_gold_lifecycle.py
```

## Final Gold Metrics

The validated Gold table currently contains:

```text
Gold rows: 2300
Unique marketplace listings: 575
Unique SKUs: 24
Average Circularity Score: 78.04
Average Secondary-Market Depreciation: -2.58%
Total Warranty Failures: 5050
```

The Gold table is also optimized using Delta Z-Ordering on:

```text
official_sku, component
```

This supports efficient SKU-level and component-level lifecycle analysis.

```

**Only replace the README for now.** Don't commit yet.
```
