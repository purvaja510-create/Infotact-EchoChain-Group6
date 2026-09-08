

# EchoChain Power BI

## Overview

The `powerbi` folder contains the final Power BI report used to
visualize EchoChain product and component lifecycle analytics.

The report uses the Gold lifecycle analytics dataset from Databricks as
its primary data source.

## Power BI Report

`EchoChain_Circular_Economy_Lifecycle_Analytics.pbix`

The report provides an executive-ready view of secondary-market value,
product circularity, warranty performance, component health, and
marketplace anomalies.

## Dashboard Pages

### Home

Provides an entry point to the report and navigation to the main
analytical sections.

### Executive Lifecycle Overview

Provides a portfolio-level view of:

- Circularity score
- Secondary-market price
- Warranty failures
- Depreciation versus manufacturing cost
- Brand-level lifecycle performance
- Product and component lifecycle health

The lifecycle matrix supports drill-down from Brand to Official SKU to
Component.

### Product & Component Lifecycle Analysis

Provides component-level analysis including:

- Component health
- Warranty failures
- Component lifecycle risk
- Component-level lifecycle details

### Marketplace & Anomaly Analysis

Provides analysis of secondary-market listings, including:

- Marketplace listing count
- Market premium
- Secondary-market depreciation
- Marketplace listing details
- SKU-level price anomalies

## Data Source

The report uses:

`workspace.gold.marketplace_product_health`

The Gold dataset combines marketplace listings with official SKU,
BOM, warranty, manufacturing cost, and component health information.

## Report Scope

The final report covers:

- 575 matched marketplace listings
- 24 official SKUs
- 2,300 component-level Gold rows

## Purpose

The Power BI report provides the final analytics layer of EchoChain,
helping identify product lifecycle performance, component risks,
secondary-market opportunities, and potential refurbishment or
buy-back opportunities.
```