"""
Validate the EchoChain marketplace dataset.

The validation checks:
- Required marketplace fields are present
- Listing URLs are unique
- Prices are numeric and greater than zero
- Marketplace currency is USD
- Product titles are populated
- Synthetic-data flag is present
"""

import json
from pathlib import Path


DATA_FILE = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "raw"
    / "marketplace_electronics_final.json"
)


REQUIRED_FIELDS = [
    "search_category",
    "product_title",
    "price",
    "currency",
    "condition",
    "seller",
    "location",
    "listing_url",
    "scraped_at",
    "data_source",
    "is_synthetic",
]


def load_data():
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def validate_data(records):
    errors = []
    listing_urls = set()

    for index, record in enumerate(records, start=1):

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in record or record[field] in (None, ""):
                errors.append(
                    f"Record {index}: missing required field '{field}'"
                )

        # Check duplicate listing URLs
        listing_url = record.get("listing_url")

        if listing_url in listing_urls:
            errors.append(
                f"Record {index}: duplicate listing_url '{listing_url}'"
            )

        if listing_url:
            listing_urls.add(listing_url)

        # Check price
        try:
            price = float(record.get("price"))

            if price <= 0:
                errors.append(
                    f"Record {index}: price must be greater than zero "
                    f"'{record.get('price')}'"
                )

        except (TypeError, ValueError):
            errors.append(
                f"Record {index}: invalid price "
                f"'{record.get('price')}'"
            )

        # Check currency
        if record.get("currency") != "USD":
            errors.append(
                f"Record {index}: unexpected currency "
                f"'{record.get('currency')}'"
            )

        # Check product title
        if not record.get("product_title"):
            errors.append(
                f"Record {index}: missing product title"
            )

        # Check synthetic flag
        if not isinstance(record.get("is_synthetic"), bool):
            errors.append(
                f"Record {index}: is_synthetic must be true or false"
            )

    return errors


def main():
    records = load_data()
    errors = validate_data(records)

    print("EchoChain Marketplace Data Validation")
    print("-------------------------------------")
    print(f"Records checked: {len(records)}")

    if errors:
        print("Validation status: FAILED")
        print(f"Errors found: {len(errors)}")

        for error in errors:
            print(f"- {error}")

    else:
        print("Validation status: PASSED")
        print("No data-quality errors found.")


if __name__ == "__main__":
    main()