import scrapy
from scrapy import signals
from pydispatcher import dispatcher
import json
import time


class BMIMeasurementSpider (scrapy.Spider):

    name = "bmimeasurement"
    start_urls = ["https://s.1688.com/company/company_search.htm?keywords=BMI%B2%E2%C1%BF&spm=a26352%2C13672862"]
# Careful: the page is encoded with bytes, we can't extract information
    counter = 0
    results = {}

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def parse(self, response):
    # extract data from the parent page

        for products in response.css('div.company-offer-contain'):
            time.sleep(2)
            yield {
                'name': products.css('a.company-name::text').get(),
                'experience': products.css('span.integrity-year::text').get(),
                'link': products.css('a.company-name').attrib['href'],
                'price': products.css('span.price').replace('¥','').get(),
                'factory': products.css('span.factory-tag::text').get(),
            }

        next_page = response.css('a.fui-next').attrib['href']
        # Careful, on 1688, no href is given for the next page
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)


    def parse2(self, response):
    # extract links of nested webpages from parent page and send them to another parse function
        time.sleep(2)
        for products in response.css("div.company-offer-card > div.company-left-card > div.company-info-contain > div.title-container > a.company-name::attr('href')"):
            yield scrapy.Request(url=products.get(), callback=self.parseNested)

        next_page = response.css('a.fui-next').attrib['href']
        # Careful, on 1688, no href is given for the next page
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse2)


    def parseNested(self, response):
    # extract data from the nested webpages
        time.sleep(2)
        for attr in response.css('div.company-offer-contain'):
            yield {
                'Certifications': attr.css('font.data-spm-anchor-id="a262cb.19918180.khuur57j.i5.6f2d3cb72krorG"::text').get(),
                'MOQ': attr.css('font.data-spm-anchor-id="a262cb.19918180.khuur57j.i0.6f2d3cb79cNLnm"::text').get(),
                'employees': attr.css('font.data-spm-anchor-id="a262cb.19918180.khuur57j.i8.6f2d3cb72krorG"::text').get(),
                'PlantArea': attr.css('font.data-spm-anchor-id="a262cb.19918180.khuur57j.i6.6f2d3cb72krorG"::text').get(),
                'Proofing': attr.css('font.data-spm-anchor-id="a262cb.19918180.khuur57j.i7.6f2d3cb72krorG"::text').get(),
            }

        self.results[self.counter] = {
            'name': name,
            'experience': experience,
            'link': link,
            'price': price,
            'factory': factory,
            'Certifications': certifications,
            'MOQ': MOQ,
            'employees': employees,
            'PlantArea': PlantArea,
            'Proofing': Proofing,
        }

        self.counter += 1

def spider_closed(self, spider):
    with open('results.json', 'w') as fp:
        json.dump(self.results, fp)