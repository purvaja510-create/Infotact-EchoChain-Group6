# EchoChain Architecture

## High-Level Architecture

EchoChain follows an end-to-end data engineering and analytics pipeline.

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
 SKU Extraction &
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
        |
        v
Executive Lifecycle Analytics
```
