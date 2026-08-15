import os
import base64
import requests
import json
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")


def get_access_token():

    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"

    encoded_credentials = base64.b64encode(
        credentials.encode()
    ).decode()

    response = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope"
        }
    )

    if response.status_code != 200:
        print("Token error:", response.status_code)
        print(response.text)

    response.raise_for_status()

    return response.json()["access_token"]


def search_electronics():

    token = get_access_token()

    response = requests.get(
    "https://api.ebay.com/buy/browse/v1/item_summary/search",
    headers={
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"
        },

        params={
        "q": "Apple iPhone" ,
        "limit": 50
        }
    )

   
    response.raise_for_status()

    data = response.json()

    listings = []

    for item in data.get("itemSummaries", []):

        price = item.get("price", {})
        seller = item.get("seller", {})

        listings.append({
            "product_title": item.get("title"),
            "price": price.get("value"),
            "currency": price.get("currency"),
            "condition": item.get("condition"),
            "seller": seller.get("username"),
            "location": item.get(
                "itemLocation",
                {}
            ).get("city"),
            "listing_url": item.get("itemWebUrl"),
            "scraped_at": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        })

    return listings


if __name__ == "__main__":

    listings = search_electronics()

    project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
    )

    output_path = os.path.join(
    project_root,
    "data",
    "raw",
    "ebay_electronics.json"
    )

    os.makedirs(
        os.path.dirname(output_path),
        exist_ok=True
    )

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            listings,
            file,
            indent=4,
            ensure_ascii=False
        )

    print(
        f"Saved {len(listings)} listings to {output_path}"
    )