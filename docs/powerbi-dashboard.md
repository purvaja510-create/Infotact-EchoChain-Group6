
# EchoChain Power BI Dashboard

## Overview

The EchoChain Power BI dashboard provides an executive view of
secondary-market value, product circularity, warranty performance,
component health, and marketplace anomalies.

The report is connected to the Gold lifecycle analytics dataset and
supports interactive analysis from brand level down to individual
components.

## Dashboard Pages

### 1. Home

The Home page provides an entry point to the EchoChain dashboard and
provides navigation to the main analytical sections.

The available sections are:

- Executive Lifecycle Overview
- Product & Component Lifecycle Analysis
- Marketplace & Anomaly Analysis

### 2. Executive Lifecycle Overview

This page provides a portfolio-level view of product lifecycle
performance.

Key KPIs include:

- Average Circularity Score
- Average Secondary-Market Price
- Total Warranty Failures
- Average Depreciation vs. Manufacturing Cost

The page also includes:

- Secondary-market value and circularity by brand
- Warranty failures versus secondary-market value
- Warranty failures by component
- Product lifecycle health by SKU and component

The lifecycle matrix supports drill-down from:

Brand → Official SKU → Component

### 3. Product & Component Lifecycle Analysis

This page focuses on component-level lifecycle performance.

It provides insights into:

- Average component health
- Components requiring attention
- Warranty failures by component
- Component health versus warranty failures
- Component lifecycle risk
- Component-level lifecycle details

This analysis helps identify components that may require attention
because of repeated failures or lower health scores.

### 4. Marketplace & Anomaly Analysis

This page focuses on secondary-market listings and unusual marketplace
pricing patterns.

Key analysis includes:

- Marketplace listing count
- Average market premium
- Secondary-market depreciation by brand
- Marketplace listing details
- SKU-level price anomaly detection

An IQR-based approach is used to identify unusually high marketplace
prices while retaining the original marketplace values for analysis.

## Key Measures

The dashboard uses lifecycle measures derived from the Gold dataset,
including:

- Circularity Score
- Secondary-Market Price
- Warranty Failures
- Component Health Score
- Secondary-Market Depreciation
- Market Premium
- Marketplace Anomaly Flag

## Business Insights

The dashboard helps answer key lifecycle questions:

1. Which products retain value in the secondary market?
2. Which products have higher warranty failure levels?
3. Which components show lower lifecycle health?
4. Which products have stronger circularity performance?
5. Which marketplace listings show unusual pricing?
6. Which products or components may represent refurbishment or
   buy-back opportunities?

## Dashboard Data

The Power BI report uses the Gold lifecycle analytics dataset:

`workspace.gold.marketplace_product_health`

The dataset combines:

- Marketplace listings
- Official SKU information
- Bill of Materials (BOM)
- Warranty data
- Component manufacturing costs
- Component health metrics
- Lifecycle analytics

## Report Scope

The dashboard covers:

- 575 matched marketplace listings
- 24 official SKUs
- 2,300 component-level Gold rows

The report is designed to provide an executive-ready view while also
supporting detailed product and component lifecycle investigation.
```