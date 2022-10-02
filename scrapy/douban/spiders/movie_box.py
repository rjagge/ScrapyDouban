'''
Date: 2022-09-22 16:05:11
LastEditors: Jagger
Description: 
LastEditTime: 2022-09-22 16:05:13
FilePath: /Repository/ScrapyDouban/scrapy/douban/spiders/movie_sell.py
'''
import scrapy
from douban.items import MovieBox


class MovieBoxSpider(scrapy.Spider):
    name = 'movie_box'
    allowed_domains = ['www.boxofficecn.com']

    start_urls = [
        'http://www.boxofficecn.com/boxoffice2022',
    ]
    # for i in range(1994,2023):
    #     start_urls.append('http://www.boxofficecn.com/boxoffice' + str(i))

    def isFloat(self, x):
        try:
            float(x)
            return True
        except:
            return False
    
    def get_release_year_china(self, item):
        if item.xpath('td[2]/text()').get():
            return item.xpath('td[2]/text()').get()
        else:
            return item.xpath('td[2]/span/text()').get()

    def parse(self, response):
        for item in response.xpath('//tr'):
            meta = MovieBox()
            meta['name'] = item.xpath('td[3]/text()').get() if item.xpath(
                'td[3]/a/text()').get() is None else item.xpath('td[3]/a/text()').get()
            meta['release_year_china'] = self.get_release_year_china(item)
            meta['box'] = item.xpath('td[4]/text()').get() if self.isFloat(item.xpath('td[4]/text()').get()) else 0
            yield meta

        # year = int(response.url[-4:])
        # base_url = response.url[:-4]
        # if year < 2022:
        #     yield response.follow(base_url + str(year + 1), callback=self.parse)
