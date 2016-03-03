from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

out_file = 'urls_out'

class NetCDF_Spider(CrawlSpider):
    name = 'netcdf'
    allowed_domains = ['uchicago.edu']
    start_urls = [
        'http://users.rcc.uchicago.edu/~davidkelly999/'
    ]
    rules = ( Rule(LinkExtractor(allow=(), restrict_xpaths=('//a',)), callback="parse_item", follow= True), )

    def parse_item(self, response):
        with open(out_file, 'w') as f:
            if response.url.endswith(".nc4") or response.url.endswith(".nc"):
                f.write(response.url + '\n')