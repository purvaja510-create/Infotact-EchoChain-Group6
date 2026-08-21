# ebay electronics Bronze Table Schema

## Table

`bronze.ebay_electronics`

## Purpose

The Bronze table stores the raw ebay electronics listing data extracted by the EchoChain Scrapy pipeline.

The Bronze layer should preserve the source data with minimal transformation so that the original scraped information remains available for downstream processing and auditing.

## Source

```text
data/raw/ebay_electronics.json
