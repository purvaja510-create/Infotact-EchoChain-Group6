
# Databricks & Delta Lake

## Overview

Databricks provides the lakehouse environment for processing and storing
EchoChain data using Delta Lake and a Bronze, Silver, and Gold
architecture.

The Databricks layer combines secondary-market marketplace data with
internal product, BOM, and warranty data to prepare the final lifecycle
analytics dataset.

## Data Flow

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
     Bronze Layer
          |
          v
     Silver Layer
          |
          v
 SKU Extraction &
 Fuzzy Matching
          |
          v
Marketplace + BOM + Warranty
          |
          v
      Gold Layer
          |
          v
       Power BI
```

## Bronze Layer

The Bronze layer stores structured source data used by the downstream
processing pipeline.

Key Bronze datasets include:

* Marketplace listing data
* Bill of Materials (BOM)
* Warranty data
* Official marketplace-to-SKU mapping

## Silver Layer

The Silver layer contains cleaned and transformed marketplace data.

Processing includes:

* Text cleaning
* Marketplace field standardization
* SKU extraction
* Marketplace listing validation
* Fuzzy matching against the official SKU master

## Gold Layer

The Gold layer contains the final lifecycle analytics dataset used by
Power BI.

The Gold dataset combines:

* Marketplace listings
* Official SKU information
* Bill of Materials
* Warranty records
* Component manufacturing costs
* Component health metrics
* Circularity metrics
* Secondary-market depreciation

The final Gold dataset contains product-level and component-level
lifecycle information for detailed analysis.

## Delta Lake Optimization

The Gold lifecycle table is optimized using Delta Lake Z-Ordering on:

```sql
official_sku, component
```

This supports efficient queries for SKU-level and component-level
lifecycle analysis.

## Downstream Analytics

The Gold dataset is consumed by Power BI to provide:

* Executive lifecycle overview
* Product and component lifecycle analysis
* Marketplace analysis
* Price anomaly analysis
* Circularity analysis
* Warranty and component health analysis

```