import scrapy

class BMIMeasurementSpider (scrapy.Spider):

    name = "bmimeasurement"
    start_urls = ["https://s.1688.com/company/company_search.htm?keywords=BMI%B2%E2%C1%BF&spm=a26352%2C13672862"]
# Careful: the page is encoded with bytes, we can't extract information

    def parse(self, response):
        for products in response.css('div.company-offer-contain'):
            try:
                yield {
                    'name': products.css('a.company-name::text').get(),
                    'experience': products.css('span.integrity-year::text').get(),
                    'link': products.css('a.company-name').attrib['href'],
                    'price' : products.css('span.price::text').get(),
                    'Labels': products.css('a.company-name::text').get(),
                    'MOQ': products.css('a.company-name::text').get(),
                    'employees': products.css('a.company-name::text').get(),
                }
            except:
                yield {
                    'name': products.css('a.company-name::text').get(),
                    'experience': '',
                    'link': products.css('a.company-name').attrib['href'],
                    'price': 'sold out',
                    'Labels': '',
                    'MOQ': '',
                    'employees': '',                }
        next_page = response.css('a.fui-next').attrib['href']
        # Careful, on 1688, no href is given for the next page

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

