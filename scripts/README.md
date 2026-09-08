# EchoChain Scripts

## Overview

The `scripts` folder contains utility scripts used to validate the
EchoChain data pipeline and verify the quality of the processed
datasets.

## Validation Scripts

The validation scripts are located in the `validation/` folder.

### Marketplace Data Validation

`validate_marketplace_data.py`

Validates the marketplace dataset for expected structure and data
quality.

### SKU Mapping Validation

`validate_sku_mapping.py`

Validates the marketplace-to-SKU matching results, including mapping
completeness and match quality.

### Gold Lifecycle Validation

`validate_gold_lifecycle.py`

Validates the final Gold lifecycle analytics dataset, including row
counts, SKU coverage, listing coverage, null checks, and metric ranges.

## Purpose

These validation scripts help ensure that data is complete, consistent,
and suitable for downstream Power BI reporting.

## Folder Structure

```text
scripts/
├── validation/
│   ├── README.md
│   ├── validate_marketplace_data.py
│   ├── validate_sku_mapping.py
│   └── validate_gold_lifecycle.py
└── README.md
```