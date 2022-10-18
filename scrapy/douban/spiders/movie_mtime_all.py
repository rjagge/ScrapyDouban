'''
Date: 2022-09-14 22:30:22
LastEditors: Jagger
Description: 
LastEditTime: 2022-09-22 16:03:59
FilePath: /Repository/ScrapyDouban/scrapy/douban/spiders/movie_comment_v1.py
'''
from email.header import Header
import json
from urllib.parse import urlencode

import douban.mysql.database as db
from douban.items import Comment, Mtime
from scrapy import Request, Spider

cursor = db.connection.cursor()
# referer = "https://m.douban.com/movie/subject/%s/?from=showing"


class MovieMtimeAllSpider(Spider):
    name = "movie_mtime_all"
    allowed_domains = ["mtime.com"]

    def start_requests(self):
        #   'https://movie.douban.com/subject/26861685/comments?start=20&limit=20&status=P&sort=new_score'
        baseurl = (
            "http://front-gateway.mtime.com/mtime-search/search/unionSearch2"
        )
        # movies = [
        #     # {'douban_id':26861685},
        #     {'douban_id': 35215390},
        # ]
        for year in range(1994,2023):
            yield Request(
                baseurl,
                method="POST",
                body=urlencode({
                    'pageIndex':'1',
                    'pageSize':'50',
                    'year':year,
                    'searchType':'0'
                }),
                headers={'Content-Type':'application/x-www-form-urlencoded'}
            )
            
    def parse(self, response):
        data = json.loads(response.body)
        items = data["data"]["movies"]
        for item in items:
            mtime = Mtime()
            mtime["mtime_id"] = item['movieId']
            mtime["name"] = item['name']
            mtime["name_en"] = item['nameEn']
            mtime["movie_type"] = item['movieType'].replace(' / ',',')
            mtime["rating"] = item['rating']
            mtime["directors"] = ','.join(item['directors']) #list
            mtime["actors"] = ','.join(item['actors']) #list
            mtime["location_name"] = item['locationName']
            mtime["realtime"] = item['realTime']
            mtime["year"] = item['rYear']
            mtime["month"] = item['rMonth']
            mtime["day"] = item['rDay']
            yield mtime

        # 如果没有爬完，就继续爬
        body = response.request.body.decode().split('&')
        page_idx = body[0].split('=')[1]
        page_size = body[1].split('=')[1]
        year = body[2].split('=')[1]
        end_idx = int(page_idx) * int(page_size)
        if end_idx < data['data']['moviesCount']:
            yield Request(
                response.request.url,
                method="POST",
                body= urlencode({
                    'pageIndex':int(page_idx)+1,
                    'pageSize':page_size,
                    'year':year,
                    'searchType':'0'
                }),
                headers={'Content-Type':'application/x-www-form-urlencoded'},
                callback=self.parse,
                dont_filter=True
            )
