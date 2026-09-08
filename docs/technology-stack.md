# EchoChain Technology Stack

## Overview

EchoChain uses a modern data engineering and analytics stack to combine
secondary-market data with internal manufacturing and warranty data.

## Technologies

### Scrapy

Used for building Python spiders to collect secondary-market
electronics listings, including pricing, condition, seller, and location
information.

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
executive reporting, lifecycle analysis, marketplace analysis,
component drill-downs, and anomaly analysis.

## Technology Flow

```text
Secondary Market Data
        ↓
      Scrapy
        ↓
Raw Marketplace Data
        ↓
Databricks / Delta Lake
        ↓
      Bronze
        ↓
     PySpark
        ↓
      Silver
        ↓
SKU Extraction & Fuzzy Matching
        ↓
Marketplace + BOM + Warranty
        ↓
       Gold
        ↓
     Power BI
        ↓
Executive Lifecycle Analytics
```
