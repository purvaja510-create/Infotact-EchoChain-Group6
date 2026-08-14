# Marketplace Data Validation

This directory contains validation scripts for the EchoChain secondary-market data.

## Validation Script

`validate_marketplace_data.py` validates the scraped marketplace JSON before downstream processing.

## Checks Performed

- Required field validation
- Duplicate listing ID detection
- Price format validation
- Currency validation
- Record count reporting

## Input

The validator reads:

`data/raw/marketplace_listings.json`

## Current Validation Result

The current mock marketplace dataset contains 10 records and passes all validation checks.

```text
Records checked: 10
Validation status: PASSED
No data-quality errors found.