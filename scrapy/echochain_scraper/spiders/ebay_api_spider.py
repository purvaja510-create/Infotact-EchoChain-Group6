import scrapy
import base64
import os
from datetime import datetime
from urllib.parse import urlencode
from dotenv import load_dotenv


load_dotenv()


class EbayApiSpider(scrapy.Spider):

    name = "electronics"

    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")

    products = [
        "Dell Laptop",
        "HP Laptop",
        "Apple iPhone",
        "Samsung Galaxy",
        "Sony Headphones"
    ]

    async def start(self):

        # Check credentials exist
        if not self.client_id or not self.client_secret:
            self.logger.error(
                "EBAY_CLIENT_ID or EBAY_CLIENT_SECRET not found."
            )
            return

        credentials = (
            f"{self.client_id}:{self.client_secret}"
        )

        encoded = base64.b64encode(
            credentials.encode()
        ).decode()

        # Request OAuth token
        yield scrapy.FormRequest(
            url=(
                "https://api.sandbox.ebay.com/"
                "identity/v1/oauth2/token"
            ),
            method="POST",
            headers={
                "Authorization": f"Basic {encoded}",
                "Content-Type":
                    "application/x-www-form-urlencoded"
            },
            formdata={
                "grant_type": "client_credentials",
                "scope":
                    "https://api.ebay.com/oauth/api_scope"
            },
            callback=self.parse_token
        )

    def parse_token(self, response):

        self.logger.info(
            "Token response status: %s",
            response.status
        )

        data = response.json()

        token = data.get("access_token")

        if not token:
            self.logger.error(
                "Access token not received: %s",
                response.text
            )
            return

        self.logger.info(
            "eBay access token generated successfully."
        )

        # Search each electronics category
        for product in self.products:

            params = {
                "q": product,
                "limit": 50
            }

            url = (
                "https://api.sandbox.ebay.com/"
                "buy/browse/v1/item_summary/search?"
                + urlencode(params)
            )

            yield scrapy.Request(
                url=url,
                headers={
                    "Authorization":
                        f"Bearer {token}",
                    "X-EBAY-C-MARKETPLACE-ID":
                        "EBAY_US"
                },
                callback=self.parse_items,
                cb_kwargs={
                    "search_category": product
                }
            )

    def parse_items(
        self,
        response,
        search_category
    ):

        data = response.json()

        for item in data.get(
            "itemSummaries",
            []
        ):

            price = item.get(
                "price",
                {}
            )

            seller = item.get(
                "seller",
                {}
            )

            location = item.get(
                "itemLocation",
                {}
            )

            yield {
                "search_category":
                    search_category,

                "product_title":
                    item.get("title"),

                "price":
                    price.get("value"),

                "currency":
                    price.get("currency"),

                "condition":
                    item.get("condition"),

                "seller":
                    seller.get("username"),

                "location":
                    location.get("city"),

                "listing_url":
                    item.get("itemWebUrl"),

                "scraped_at":
                    datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
            }