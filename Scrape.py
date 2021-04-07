import scrapy

class BMIMeasurementSpider (scrapy.Spider):
    name = "BMI Measurement"
    start_urls = ["https://s.1688.com/company/company_search.htm?keywords=BMI%B2%E2%C1%BF&spm=a26352%2C13672862"]

    def parse(self, response):
        for products in response.css('div.company-offer-contain'):
            try:
                yield {
                    'name': products.css('a.company-name::text').get(),
                    'experience': products.css('span.integrity-year::text').get(),
                    # find a way to get the labels : 'labels': products.css('a.product-item-link').get(),
                    'link': products.css('a.company-name').attrib['href'],
                    'price' : products.css('span.price::text').get(),
                }
            except:
                yield {
                # look if there are exceptions
                }
        next_page = response.css('a.fui-next').attrib['href']
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)


