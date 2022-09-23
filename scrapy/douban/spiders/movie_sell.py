'''
Date: 2022-09-22 16:05:11
LastEditors: Jagger
Description: 
LastEditTime: 2022-09-22 16:05:13
FilePath: /Repository/ScrapyDouban/scrapy/douban/spiders/movie_sell.py
'''
import scrapy
from douban.items import MovieMeta


class MovieSellSpider(scrapy.Spider):
    name = 'movie_sell'
    allowed_domains = ['www.boxofficecn.com']

    start_urls = [
        'http://www.boxofficecn.com/boxoffice1995',
    ]
    # for i in range(1994,2023):
    #     start_urls.append('http://www.boxofficecn.com/boxoffice' + str(i))

    def parse(self, response):
        for item in response.xpath('//tr'):
            meta = MovieMeta()
            meta['name'] = item.xpath('td[3]/text()').get() if item.xpath(
                'td[3]/a/text()').get() is None else item.xpath('td[3]/a/text()').get()
            meta['year'] = item.xpath('td[2]/text()').get()
            meta['sell'] = item.xpath('td[4]/text()').get()
            yield meta

        year = int(response.url[-4:])
        base_url = response.url[:-4]
        if year < 2022:
            yield response.follow(base_url + str(year + 1), callback=self.parse)
