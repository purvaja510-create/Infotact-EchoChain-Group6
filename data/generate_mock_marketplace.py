import json
import random
from pathlib import Path
from datetime import datetime, timedelta


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

TOTAL_RECORDS = 500

random.seed(42)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "raw" / "mock_marketplace_electronics.json"


# ---------------------------------------------------------
# Product Master
# ---------------------------------------------------------

products = [
    {
        "brand": "Apple",
        "category": "Apple iPhone",
        "model": "A1660",
        "names": ["iPhone 7", "Apple iPhone 7"],
        "price_range": (80, 220),
    },
    {
        "brand": "Apple",
        "category": "Apple iPhone",
        "model": "A2633",
        "names": ["iPhone 13", "Apple iPhone 13"],
        "price_range": (250, 550),
    },
    {
        "brand": "Dell",
        "category": "Dell Laptop",
        "model": "Latitude 5491",
        "names": ["Dell Latitude 5491", "Latitude 5491 Laptop"],
        "price_range": (180, 500),
    },
    {
        "brand": "Dell",
        "category": "Dell Laptop",
        "model": "DC15250",
        "names": ["Dell DC15250 Laptop", "Dell Laptop DC15250"],
        "price_range": (250, 650),
    },
    {
        "brand": "HP",
        "category": "HP Laptop",
        "model": "14-em0002wm",
        "names": ["HP 14-em0002wm Laptop", "HP Laptop 14-em0002wm"],
        "price_range": (200, 550),
    },
    {
        "brand": "HP",
        "category": "HP Laptop",
        "model": "15-fb3093dx",
        "names": ["HP 15-fb3093dx Laptop", "HP Laptop 15-fb3093dx"],
        "price_range": (300, 700),
    },
    {
        "brand": "HP",
        "category": "HP Laptop",
        "model": "16-by0015dx",
        "names": [
            "HP OmniBook 3 16-by0015dx",
            "HP 16-by0015dx Laptop",
        ],
        "price_range": (350, 800),
    },
    {
        "brand": "Samsung",
        "category": "Samsung Galaxy",
        "model": "NP750XQB-KA2US",
        "names": [
            "Samsung Galaxy Book4 Edge NP750XQB-KA2US",
            "Galaxy Book4 Edge NP750XQB-KA2US",
        ],
        "price_range": (450, 1000),
    },
    {
        "brand": "Sony",
        "category": "Sony Headphones",
        "model": "Model 61",
        "names": [
            "Sony Neckband Model 61",
            "Sony Model 61 Wireless Headphones",
        ],
        "price_range": (30, 150),
    },
]


conditions = [
    "New",
    "Used",
    "Used - Good",
    "Used - Excellent",
    "Certified Refurbished",
    "Open Box",
]

storage_options = [
    "",
    " 64GB",
    " 128GB",
    " 256GB",
]

extras = [
    "",
    " Unlocked",
    " WiFi",
    " With Charger",
    " Tested Working",
    " Great Condition",
    " Free Shipping",
    " Fast Shipping",
]

cities = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Dallas",
    "Seattle",
    "Miami",
]


# ---------------------------------------------------------
# Generate Records
# ---------------------------------------------------------

records = []

for i in range(1, TOTAL_RECORDS + 1):

    product = random.choice(products)

    base_name = random.choice(product["names"])

    condition = random.choice(conditions)

    storage = random.choice(storage_options)

    extra = random.choice(extras)

    # Add variation/noise to marketplace title
    title_patterns = [
        f"{base_name}{storage}{extra}",
        f"{base_name} {condition}{extra}",
        f"{product['brand']} {product['model']} {condition}{storage}",
        f"{base_name}{storage} - {condition}",
    ]

    product_title = random.choice(title_patterns)

    min_price, max_price = product["price_range"]

    price = round(
        random.uniform(min_price, max_price),
        2
    )

    scraped_time = (
        datetime.now()
        - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
    )

    record = {
        "search_category": product["category"],
        "product_title": product_title,
        "price": f"{price:.2f}",
        "currency": "USD",
        "condition": condition,
        "seller": f"mock_seller_{random.randint(1, 100)}",
        "location": random.choice(cities),

        # Unique synthetic URL — not a real eBay URL
        "listing_url": (
            f"https://mock.echochain.local/listing/{i:05d}"
        ),

        "scraped_at": scraped_time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        # Important provenance fields
        "data_source": "synthetic_mock",
        "is_synthetic": True,
    }

    records.append(record)


# ---------------------------------------------------------
# Save JSON
# ---------------------------------------------------------

OUTPUT_FILE.parent.mkdir(
    parents=True,
    exist_ok=True
)

with open(
    OUTPUT_FILE,
    "w",
    encoding="utf-8"
) as file:

    json.dump(
        records,
        file,
        indent=2,
        ensure_ascii=False
    )


print("---------------------------------------")
print("EchoChain Mock Marketplace Generator")
print("---------------------------------------")
print(f"Records generated : {len(records)}")
print(f"Output file       : {OUTPUT_FILE}")
print("Data source       : synthetic_mock")
print("---------------------------------------")