import os

BOT_NAME = "multi_scraper"

SPIDER_MODULES = ["multi_scraper.spiders"]
NEWSPIDER_MODULE = "multi_scraper.spiders"

ZYTE_API_KEY = os.getenv("ZYTE_API_KEY")

ROBOTSTXT_OBEY = False


REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

ZYTE_API_TRANSPARENT_MODE = False
DOWNLOADER_MIDDLEWARES = {
    "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000
}
COOKIES_ENABLED = True
ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED = True

REQUEST_FINGERPRINTER_CLASS = "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter"
SPIDER_MIDDLEWARES = {
    "scrapy_zyte_api.ScrapyZyteAPISpiderMiddleware": 100,
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
DOWNLOAD_HANDLERS = {
    "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
    "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
}
ZYTE_SMARTPROXY_ENABLED =True
DEFAULT_REQUEST_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"
        }
DEFAULT_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}
ZYTE_API_BROWSER_HEADERS = {
    "User-Agent": None  
}

ZYTE_API_BROWSER_CALL = True 
ZYTE_API_TRANSPARENT_MODE = True
ZYTE_API_JS_RENDER = True