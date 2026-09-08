# Raw Data

## Overview

This directory contains the raw datasets used as inputs for the
EchoChain data engineering and lifecycle analytics pipeline.

The datasets include secondary-market marketplace listings, the internal
SKU master, Bill of Materials (BOM), and warranty information.

## Datasets

### Marketplace Listings

`marketplace_electronics_final.json`

Contains secondary-market electronics listing data used as the external
marketplace source.

Key attributes include:

- Product title
- Brand
- Model
- Category
- Price
- Currency
- Condition
- Seller
- Location
- Listing URL
- Data source
- Scraped timestamp

### SKU Master

`sku_master.csv`

Contains the official product SKU reference data used for SKU extraction
and fuzzy matching of marketplace listings.

### Bill of Materials

`bom.csv`

Contains component-level Bill of Materials information for the products
in the SKU master.

The dataset includes component details such as:

- SKU
- Component
- Component type
- Manufacturing cost
- Currency

### Warranty Data

`warranty.csv`

Contains component-level warranty failure information used to analyze
product and component lifecycle performance.

The dataset includes information such as:

- SKU
- Component
- Failure date
- Failure type
- Failure count

## Purpose

The raw datasets provide the source inputs for the EchoChain Bronze,
Silver, and Gold data processing pipeline.

Marketplace listings are matched with the internal SKU master and then
combined with BOM and warranty data to support secondary-market,
circularity, warranty, component health, and lifecycle analysis.