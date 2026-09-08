
# EchoChain Scrapy

## Overview

The Scrapy module supports the collection of secondary-market
electronics listing data used in the EchoChain lifecycle analytics
pipeline.

The collected marketplace data provides the external secondary-market
view that is later combined with internal product, BOM, and warranty
data.

## Spiders

The project includes marketplace spiders for collecting listing
information.

### Marketplace Spider

The marketplace spider collects listing-level information such as:

- Product title
- Marketplace price
- Currency
- Product condition
- Seller
- Location
- Listing URL
- Source information
- Scraped timestamp

### eBay API Spider

The eBay API spider provides an API-based approach for retrieving
secondary-market listing information.

## Data Flow

The Scrapy module forms the initial data collection layer of the
EchoChain pipeline.

```text
Secondary-Market Marketplace
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
   Silver / Gold Pipeline
            |
            v
      Power BI Analytics
```

## Role in EchoChain

The marketplace data collected through Scrapy is used to analyze:

* Secondary-market prices
* Product resale behavior
* Product circularity
* Marketplace anomalies
* Product and component lifecycle performance

The marketplace data is later matched with the internal SKU master,
BOM, and warranty datasets to support product-level and component-level
lifecycle analysis.

## Project Structure

```text
scrapy/
├── echochain_scraper/
│   ├── spiders/
│   │   ├── marketplace_spider.py
│   │   └── ebay_api_spider.py
│   ├── items.py
│   ├── middlewares.py
│   ├── pipelines.py
│   └── settings.py
└── scrapy.cfg
```
