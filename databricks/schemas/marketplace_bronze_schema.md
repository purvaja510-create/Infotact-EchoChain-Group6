# eBay Electronics Bronze Table Schema

## Table

`workspace.bronze.ebay_electronics`

## Purpose

The Bronze table stores the raw marketplace listing data extracted by the EchoChain Scrapy pipeline.

The Bronze layer preserves the source data with minimal transformation so that the original marketplace information remains available for downstream processing and auditing.

## Source

`data/raw/marketplace_listings.json`
