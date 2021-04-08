import scrapy
# from bmiscraper.items import BMIscraperItem
from items import BMIscraperItem
from scrapy.loader import ItemLoader


class BMIMeasurementSpider (scrapy.Spider):

    name = "bmimeasurement"
    start_urls = ["https://s.1688.com/company/company_search.htm?keywords=BMI%B2%E2%C1%BF&spm=a26352%2C13672862"]
# Careful: the page is encoded with bytes, we can't extract information

    def parse(self, response):
        for products in response.css('div.company-offer-contain'):
            l = ItemLoader(item = BMIscraperItem(), selector = products)

            l.add_css('name', 'a.company-name')
            l.add_css('experience', 'span.integrity-year')
            l.add_css('link', 'a.company-name::attr(href)')
            l.add_css('price', 'span.price')
            l.add_css('factory', 'span.factory-tag')
            # l.add_css('Labels', 'a.company-name::text')
            # l.add_css('MOQ', 'a.company-name::text')
            # l.add_css('employees', 'a.company-name::text')

            yield l.load_item()

        next_page = response.css('a.fui-next').attrib['href']
        # Careful, on 1688, no href is given for the next page

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)