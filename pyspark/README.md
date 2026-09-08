
# EchoChain PySpark

## Overview

The `pyspark` module contains the data processing and transformation
scripts used in the EchoChain lifecycle analytics pipeline.

PySpark is used to clean marketplace data, extract product SKUs, perform
fuzzy matching, combine marketplace data with internal BOM and warranty
data, and prepare the final Gold lifecycle analytics dataset.

## Pipeline Scripts

### Marketplace Bronze Ingestion

`ingest_marketplace_bronze.py`

Loads marketplace listing data into the Bronze layer in Databricks.

### Bronze to Silver

`bronze_to_silver.py`

Cleans and transforms marketplace data from the Bronze layer into the
Silver layer.

Processing includes field standardization and marketplace data
preparation for downstream SKU processing.

### SKU Extraction

`extract_marketplace_skus.py`

Extracts product model and SKU-related information from marketplace
listing titles and prepares the data for matching against the official
SKU master.

### Fuzzy SKU Matching

`fuzzy_matching.py`

Matches marketplace listings against the official SKU master using
fuzzy matching techniques to handle differences and inconsistencies in
marketplace product titles.

### Silver to Gold

`silver_to_gold.py`

Combines matched marketplace listings with BOM and warranty data and
produces the final Gold lifecycle analytics dataset.

The Gold dataset contains product-level and component-level lifecycle
metrics used by Power BI.

## Processing Flow

```text
Bronze Marketplace Data
          |
          v
     Bronze to Silver
          |
          v
       Silver
          |
          v
    SKU Extraction
          |
          v
    Fuzzy Matching
          |
          v
Marketplace + BOM + Warranty
          |
          v
         Gold
          |
          v
       Power BI
```

## Key Processing Areas

The PySpark pipeline supports:

* Marketplace data cleaning
* Text processing
* SKU extraction
* Fuzzy SKU matching
* BOM integration
* Warranty integration
* Component-level lifecycle analysis
* Circularity calculations
* Secondary-market depreciation analysis
* Gold dataset preparation

## Output

The final processing stage produces the Gold lifecycle analytics dataset:

`workspace.gold.marketplace_product_health`

This dataset is used as the primary data source for the EchoChain
Power BI dashboard.

```