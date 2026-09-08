
# EchoChain Data Pipeline

## Overview

EchoChain uses a Bronze/Silver/Gold data pipeline to combine
secondary-market marketplace data with internal SKU, BOM, and warranty
information for lifecycle analytics.

## Pipeline Flow

```text
Secondary Market Data
        |
        v
Raw Marketplace Data
        |
        v
      Bronze
        |
        v
Data Cleaning & Transformation
        |
        v
      Silver
        |
        v
SKU Extraction
        |
        v
Fuzzy SKU Matching
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

## 1. Bronze Layer

The Bronze layer stores ingested marketplace data in Databricks Delta
tables while preserving the original marketplace information.

The marketplace ingestion process loads details including:

* Product title
* Listing URL
* Brand
* Price
* Currency
* Condition
* Seller
* Location
* Scrape timestamp

## 2. Silver Layer

The Silver layer cleans and standardizes marketplace data before it is
used for analytics.

This stage includes text cleaning, normalization of listing information,
and preparation of product titles for SKU extraction.

## 3. SKU Extraction

Marketplace product titles are processed to identify potential SKU and
model information.

The extracted information is then used to generate candidate SKUs for
comparison with the internal SKU master.

## 4. Fuzzy SKU Matching

Marketplace titles do not always follow the structured format of the
internal SKU information. PySpark fuzzy matching is used to compare
marketplace candidates with the official SKU master.

Strong matches are retained for downstream lifecycle analysis.

## 5. BOM and Warranty Integration

Matched marketplace listings are combined with internal Bill of
Materials (BOM) and warranty information.

This connects secondary-market product value with component-level
manufacturing and warranty information.

## 6. Gold Layer

The Gold layer contains the final lifecycle analytics dataset.

It supports metrics including:

* Circularity Score
* Secondary-market price
* Secondary-market depreciation
* Warranty failures
* Component health
* Component manufacturing cost
* Product and component lifecycle analysis

The final Gold dataset contains 2,300 component-level rows representing
575 matched marketplace listings across 24 official SKUs.

## 7. Power BI

The Gold dataset is connected to Power BI for executive lifecycle
analytics.

The dashboard provides:

* Portfolio-level lifecycle KPIs
* Secondary-market value analysis
* Warranty failure analysis
* Component lifecycle analysis
* Circularity analysis
* Marketplace anomaly analysis
* Brand → SKU → Component drill-down

## Pipeline Optimization

The Gold Delta table is optimized using Z-Ordering on:

* `official_sku`
* `component`

This improves data organization and supports efficient downstream
lifecycle analytics queries.
```
