import json
from pathlib import Path


DATA_FILE = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "raw"
    / "marketplace_listings.json"
)

REQUIRED_FIELDS = [
    "listing_id",
    "brand",
    "model",
    "category",
    "condition",
    "price",
    "currency",
    "seller",
    "location",
    "description",
]


def load_data():
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def validate_data(records):
    errors = []

    listing_ids = []

    for index, record in enumerate(records, start=1):

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in record or record[field] in (None, ""):
                errors.append(
                    f"Record {index}: missing required field '{field}'"
                )

        # Check duplicate listing IDs
        listing_id = record.get("listing_id")

        if listing_id in listing_ids:
            errors.append(
                f"Record {index}: duplicate listing_id '{listing_id}'"
            )

        listing_ids.append(listing_id)

        # Check price
        try:
            float(record.get("price"))
        except (TypeError, ValueError):
            errors.append(
                f"Record {index}: invalid price '{record.get('price')}'"
            )

        # Check currency
        if record.get("currency") != "INR":
            errors.append(
                f"Record {index}: unexpected currency "
                f"'{record.get('currency')}'"
            )

    return errors


def main():
    records = load_data()
    errors = validate_data(records)

    print("EchoChain Marketplace Data Validation")
    print("-------------------------------------")
    print(f"Records checked: {len(records)}")

    if errors:
        print(f"Validation status: FAILED")
        print(f"Errors found: {len(errors)}")

        for error in errors:
            print(f"- {error}")
    else:
        print("Validation status: PASSED")
        print("No data-quality errors found.")


if __name__ == "__main__":
    main()