import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "raw"

EBAY_FILE = RAW_DIR / "ebay_electronics.json"
MOCK_FILE = RAW_DIR / "mock_marketplace_electronics.json"
OUTPUT_FILE = RAW_DIR / "marketplace_electronics_final.json"


# -------------------------------
# Load data
# -------------------------------

with open(EBAY_FILE, "r", encoding="utf-8") as f:
    ebay_data = json.load(f)

with open(MOCK_FILE, "r", encoding="utf-8") as f:
    mock_data = json.load(f)


print("Existing eBay records:", len(ebay_data))
print("Mock records:", len(mock_data))


# -------------------------------
# Mark original eBay records
# -------------------------------

for item in ebay_data:
    item["data_source"] = "ebay_sandbox"
    item["is_synthetic"] = False


# -------------------------------
# Combine
# -------------------------------

combined_data = ebay_data + mock_data


# -------------------------------
# Remove duplicate listing URLs
# -------------------------------

unique_records = []
seen_urls = set()

for item in combined_data:

    url = item.get("listing_url")

    if url and url not in seen_urls:
        seen_urls.add(url)
        unique_records.append(item)

    elif not url:
        unique_records.append(item)


# -------------------------------
# Save final dataset
# -------------------------------

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(
        unique_records,
        f,
        indent=2,
        ensure_ascii=False
    )


print("--------------------------------")
print("EchoChain Dataset Merge Complete")
print("--------------------------------")
print("eBay records     :", len(ebay_data))
print("Mock records     :", len(mock_data))
print("Before dedupe    :", len(combined_data))
print("Final records    :", len(unique_records))
print("Removed duplicate:", len(combined_data) - len(unique_records))
print("Output           :", OUTPUT_FILE)