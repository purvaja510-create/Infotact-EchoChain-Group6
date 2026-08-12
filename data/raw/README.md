# Raw Secondary-Market Data

This directory contains the mock secondary-market source used by the EchoChain Scrapy data-acquisition pipeline.

## Source

`mock_marketplace.html` simulates a secondary-market electronics marketplace containing product and component listings.

## Listing Attributes

- Listing ID
- Product title
- Brand
- Model
- Category
- Condition
- Price
- Currency
- Seller
- Location
- Description

## Purpose

The synthetic marketplace data provides a controlled and reproducible source for developing and testing the Scrapy extraction pipeline before downstream processing through Databricks, Delta Lake, and PySpark.