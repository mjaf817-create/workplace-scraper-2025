BOT_NAME = 'workplace_scraper'
SPIDER_MODULES = ['src.scrapers.spiders']
NEWSPIDER_MODULE = 'src.scrapers.spiders'

ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 4
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
COOKIES_ENABLED = True

DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

ITEM_PIPELINES = {
    'src.scrapers.pipelines.MongoPipeline': 300,
}

# mongo config
MONGODB_URI = 'mongodb://localhost:27017/'
MONGODB_DATABASE = 'workplace_relations'
MONGODB_COLLECTION = 'decisions'

# autothrottle disabled (using fixed delays)
AUTOTHROTTLE_ENABLED = False

# cache for dev
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
FEED_EXPORT_ENCODING = 'utf-8'
