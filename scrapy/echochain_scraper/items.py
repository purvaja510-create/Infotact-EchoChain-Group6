import scrapy


class MarketplaceListingItem(scrapy.Item):
    listing_id = scrapy.Field()
    brand = scrapy.Field()
    model = scrapy.Field()
    category = scrapy.Field()
    condition = scrapy.Field()
    price = scrapy.Field()
    currency = scrapy.Field()
    seller = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()