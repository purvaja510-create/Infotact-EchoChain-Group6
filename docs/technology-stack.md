# EchoChain Technology Stack

## Overview

EchoChain uses a modern data engineering and analytics stack to combine
secondary-market data with internal manufacturing, BOM, and warranty data.

## Technologies

### Scrapy

Used for building Python spiders to collect secondary-market
electronics listings, including pricing, condition, seller, location,
and listing information.

### Databricks

Used as the primary data engineering and lakehouse environment for
processing and managing EchoChain data.

### Delta Lake

Used as the storage layer within Databricks, following a
Bronze/Silver/Gold architecture for structured data processing.

### PySpark

Used for data processing and transformation, including:

- Data cleaning
- Text processing
- SKU extraction
- Fuzzy SKU matching
- Marketplace, BOM, and warranty integration
- Data aggregation
- Lifecycle analytics preparation

### Power BI

Used as the business intelligence and visualization layer for
executive reporting, lifecycle analysis, component drill-downs,
marketplace analysis, and anomaly analysis.

## Technology Flow

```text
Secondary Market Data
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
```