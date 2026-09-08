
# EchoChain Data

## Overview

The `data` directory contains the datasets used by the EchoChain data
engineering and lifecycle analytics pipeline.

The project combines external secondary-market marketplace data with
internal product and lifecycle data to support product-level and
component-level analysis.

## Data Organization

```text
data/
└── raw/
    ├── marketplace_electronics_final.json
    ├── sku_master.csv
    ├── bom.csv
    └── warranty.csv
```

## Dataset Categories

### Marketplace Data

Secondary-market listing data provides information about products
available in the resale market, including pricing, condition, and
listing details.

### Product Reference Data

The SKU master provides the official product and SKU reference used for
matching marketplace listings.

### Product Structure Data

The Bill of Materials (BOM) dataset provides component-level product
structure and manufacturing cost information.

### Warranty Data

Warranty data provides component-level failure information used to
analyze product and component lifecycle performance.

## Data Processing

The raw datasets are processed through the EchoChain Bronze, Silver, and
Gold data layers in Databricks using PySpark.

The final Gold dataset is used by Power BI for lifecycle analytics and
executive reporting.

```