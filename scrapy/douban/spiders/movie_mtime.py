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
    name = "movie_mtime"
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
        name_list = [
            '亡命天涯',
            '不能忘却的长征',
            '北京的哥',
            '铁血北疆曲',
            '神兵小将',
            '第一军规',
            '袁崇焕',
            '蝙蝠侠：黑暗骑士崛起',
            '缘分',
            '一家两口',
            '失业生',
            '风吹浪涌',
            '天堂电影院'
        ]
        for name in name_list:
            yield Request(
                baseurl,
                method="POST",
                body=urlencode({
                    'keyword':name,
                    'pageIndex':'1',
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

