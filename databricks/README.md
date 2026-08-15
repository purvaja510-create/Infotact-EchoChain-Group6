# Databricks & Delta Lake Setup

## Purpose

The Databricks layer will provide the lakehouse storage and processing foundation for EchoChain's lifecycle analytics pipeline.

The project architecture uses Databricks with Delta Lake to combine scraped secondary-market data with internal manufacturing data such as Bills of Materials (BOM) and warranty records.

## Planned Data Flow

```text
Secondary Market Listings
        |
        v
   Scrapy Extraction
        |
        v
   Raw JSON / CSV
        |
        v
 Databricks Bronze Layer
        |
        v
    Silver Layer
        |
        v
     Gold Layer
        |
        v
     Power BI
