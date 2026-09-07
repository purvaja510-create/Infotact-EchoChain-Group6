````markdown
# EchoChain: Circular Economy & Secondary Market Lifecycle Analytics

## Infotact Solutions Internship | Project 2

EchoChain is a data engineering and analytics project focused on understanding the post-sale lifecycle of electronic products.

The project combines secondary-market marketplace listings with internal product data such as Bills of Materials (BOM) and warranty records. The goal is to identify secondary-market value, warranty and component failure patterns, product circularity, and potential refurbishment or buy-back opportunities.

## Project Objective

Traditional product analytics often focus on the product only until the point of sale. EchoChain extends this view into the secondary market by connecting marketplace data with internal product and warranty information.

The solution helps answer questions such as:

- How much value does a product retain in the secondary market?
- Which products have high warranty failure rates?
- Which components show higher lifecycle risk?
- How circular is each product?
- Which products or components may be suitable for refurbishment or buy-back?
- Which marketplace listings require further investigation?

## Project Architecture

EchoChain follows a Bronze-Silver-Gold lakehouse architecture.

```text
Secondary-Market Listings
          |
          v
       Scrapy
          |
          v
   Raw Marketplace Data
          |
          v
   Databricks / Delta Lake
          |
          v
        Bronze
          |
          v
       PySpark
          |
          v
        Silver
          |
          v
 SKU Extraction & Fuzzy Matching
          |
          v
Marketplace + BOM + Warranty
          |
          v
         Gold
          |
          v
       Power BI
          |
          v
 Executive Lifecycle Analytics
````

## Technology Stack

| Technology       | Purpose                                                                                |
| ---------------- | -------------------------------------------------------------------------------------- |
| **Scrapy**       | Collect secondary-market electronics listing data                                      |
| **Databricks**   | Lakehouse environment for data processing and analytics                                |
| **Delta Lake**   | Bronze, Silver, and Gold data storage                                                  |
| **PySpark**      | Data cleaning, SKU extraction, fuzzy matching, transformation, and lifecycle analytics |
| **Power BI**     | Executive dashboards, KPIs, lifecycle analysis, and drill-down                         |
| **Python**       | Data processing and validation scripts                                                 |
| **Git & GitHub** | Version control and collaborative development                                          |

## Data Pipeline

### 1. Marketplace Data

Secondary-market electronics listings are collected and prepared for downstream processing. The marketplace data contains attributes such as product title, brand, model, price, condition, seller, location, and listing URL.

### 2. Bronze Layer

Raw marketplace data is ingested into Databricks Delta tables while preserving the source-level information.

### 3. Silver Layer

The marketplace data is cleaned and standardized for analytics. Text processing and SKU extraction are performed to identify product candidates from marketplace titles.

### 4. SKU Matching

Marketplace titles are often inconsistent with the internal SKU master. PySpark fuzzy matching is used to match extracted marketplace candidates with official internal SKUs.

This allows messy secondary-market data to be connected with structured internal product information.

### 5. Gold Lifecycle Analytics

The matched marketplace listings are combined with internal BOM and warranty information at the component level.

The Gold layer provides product and component lifecycle metrics used by Power BI.

## Key Analytics

### Circularity Score

The **Circularity Score** is used to evaluate the potential for products and components to remain within the economic lifecycle through resale, reuse, and refurbishment.

### Secondary-Market Value

Marketplace prices are analyzed to understand the value retained by products after the original sale.

### Warranty Failures

Warranty records are combined with product and component information to identify failure patterns and lifecycle risks.

### Component Health

Component-level health scores help identify components that may require attention or have higher lifecycle risk.

### Secondary-Market Depreciation

Secondary-market depreciation is calculated relative to the manufacturing-cost baseline used in the analysis.

A negative depreciation value indicates that the observed secondary-market price is higher than the manufacturing-cost baseline and can therefore represent a market premium.

### Marketplace Anomaly Analysis

Marketplace prices are screened using an IQR-based approach at the SKU level to identify listings that require further investigation while retaining the original marketplace values.

## Power BI Dashboard

The final Power BI report contains four main pages:

### Home

Provides navigation to the main analytical sections of the report.

### Executive Lifecycle Overview

Provides an executive-level view of:

* Average Circularity Score
* Average Secondary-Market Price
* Total Warranty Failures
* Secondary-Market Depreciation
* Brand-level market performance
* Warranty failures by component
* Product lifecycle health by SKU and component

The page supports drill-down from:

**Brand → Official SKU → Component**

### Product & Component Lifecycle Analysis

Provides deeper analysis of component health, warranty failures, lifecycle risk, and component-level performance.

### Marketplace & Anomaly Analysis

Focuses on secondary-market listings, market premiums, depreciation patterns, and potentially anomalous marketplace prices.

## Current Gold Dataset

The validated Gold lifecycle dataset currently contains:

```text
Gold rows:                  2,300
Marketplace listings:         575
Official SKUs:                 24
Average Circularity Score:  78.04
Average Depreciation:       -2.58%
Total Warranty Failures:    5,050
```

The 2,300 Gold rows represent 575 matched marketplace listings joined with four BOM components per product.

## Data Validation

The project includes validation scripts for different stages of the pipeline.

```text
scripts/validation/
├── validate_marketplace_data.py
├── validate_sku_mapping.py
└── validate_gold_lifecycle.py
```

The validation process checks:

* Required marketplace fields
* Duplicate listing URLs
* Price and currency validity
* SKU matching results
* Official SKU mappings
* BOM coverage
* Missing lifecycle values
* Component health score ranges
* Circularity score ranges
* Final Gold dataset coverage

Current validation results:

```text
Marketplace dataset: PASSED
SKU mapping:         PASSED
Gold lifecycle data: PASSED
```

## Delta Lake Optimization

The Gold lifecycle table is optimized using Delta Lake Z-Ordering on:

```text
official_sku, component
```

This supports efficient SKU-level and component-level lifecycle analysis.

## Repository Structure

```text
Infotact-EchoChain-Group6/
│
├── data/
│   └── raw/
│       ├── bom.csv
│       ├── marketplace_electronics_final.json
│       ├── sku_master.csv
│       └── warranty.csv
│
├── databricks/
│   └── schemas/
│
├── docs/
│   ├── architecture.md
│   ├── project-setup.md
│   └── technology-stack.md
│
├── images/
│
├── powerbi/
│   └── EchoChain_Circular_Economy_Lifecycle_Analytics.pbix
│
├── pyspark/
│   ├── bronze_to_silver.py
│   ├── extract_marketplace_skus.py
│   ├── fuzzy_matching.py
│   ├── ingest_marketplace_bronze.py
│   └── silver_to_gold.py
│
├── scrapy/
│   └── echochain_scraper/
│
├── scripts/
│   └── validation/
│       ├── validate_gold_lifecycle.py
│       ├── validate_marketplace_data.py
│       └── validate_sku_mapping.py
│
├── requirements.txt
└── README.md
```

## Project Outcome

EchoChain demonstrates how messy secondary-market data can be integrated with structured internal product information to create a unified product lifecycle view.

The final solution connects:

**Marketplace Data → SKU Matching → BOM & Warranty → Component Lifecycle → Circularity → Secondary-Market Value → Business Insights**

This enables organizations to move beyond traditional post-sale visibility and identify opportunities for **refurbishment, reuse, resale, and product buy-back strategies**.
