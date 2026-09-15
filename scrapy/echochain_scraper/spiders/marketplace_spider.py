from pathlib import Path

import scrapy
from scrapy.http import HtmlResponse

from echochain_scraper.items import MarketplaceListingItem


class MarketplaceSpider(scrapy.Spider):
    name = "marketplace"

    async def start(self):
        file_path = (
            Path(__file__).resolve().parents[3]
            / "data"
            / "raw"
            / "mock_marketplace.html"
        )

        html = file_path.read_text(encoding="utf-8")

        response = HtmlResponse(
            url=file_path.as_uri(),
            body=html.encode("utf-8"),
            encoding="utf-8",
        )

        for item in self.parse(response):
            yield item

    def parse(self, response):
        for listing in response.css("div.listing"):
            item = MarketplaceListingItem()

            item["listing_id"] = listing.css(
                "::attr(data-listing-id)"
            ).get()

            item["brand"] = listing.css(
                "::attr(data-brand)"
            ).get()

            item["model"] = listing.css(
                "::attr(data-model)"
            ).get()

            item["category"] = listing.css(
                "::attr(data-category)"
            ).get()

            item["condition"] = listing.css(
                "::attr(data-condition)"
            ).get()

            item["price"] = listing.css(
                "::attr(data-price)"
            ).get()

            item["currency"] = listing.css(
                "::attr(data-currency)"
            ).get()

            item["seller"] = listing.css(
                "::attr(data-seller)"
            ).get()

            item["location"] = listing.css(
                "::attr(data-location)"
            ).get()

            description = listing.css(
                "p.description::text"
            ).get()

            item["description"] = (
                description.strip() if description else None
            )

            yield item