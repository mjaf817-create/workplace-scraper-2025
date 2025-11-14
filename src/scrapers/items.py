# scraped item models
import scrapy


class WorkplaceDecisionItem(scrapy.Item):
    """workplace decision data"""
    identifier = scrapy.Field()  # ADJ-00054658
    description = scrapy.Field()  # case description
    ref_no = scrapy.Field()  # reference number
    published_date = scrapy.Field()  # dd/mm/yyyy
    link_to_doc = scrapy.Field()  # doc url
    body = scrapy.Field()  # "Workplace Relations Commission"
    partition_date = scrapy.Field()  # scraping period
    scraped_at = scrapy.Field()  # timestamp
