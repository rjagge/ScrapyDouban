'''
Date: 2022-09-14 22:30:22
LastEditors: Jagger
Description: 爬mtime的detail.api
LastEditTime: 2022-09-22 16:03:59
FilePath: /Repository/ScrapyDouban/scrapy/douban/spiders/movie_comment_v1.py
'''
from email.header import Header
import json
from urllib.parse import urlencode

import douban.mysql.database as db
from douban.items import Mtime, MtimeJson
from scrapy import Request, Spider

cursor = db.connection.cursor()
# referer = "https://m.douban.com/movie/subject/%s/?from=showing"


class MovieMtimeJsonSpider(Spider):
    name = "movie_mtime_json"
    allowed_domains = ["mtime.com"]

    def start_requests(self):
        #   'https://movie.douban.com/subject/26861685/comments?start=20&limit=20&status=P&sort=new_score'
        baseurl = (
            "http://front-gateway.mtime.com/library/movie/detail.api?movieId=%s&locationId=290"
        )

        sql = "select mtime_id from mtime where mins = ''"
        cursor.execute(sql)
        mtime_ids = cursor.fetchall()

        # mtime_ids = [
        #     {'mtime_id': 12613}
        # ]
            
        
        for mtime_id in mtime_ids:
            yield Request(
                baseurl % mtime_id['mtime_id'],
            )

    def extract_companies(self, companys):
        if companys is None:
            return ''
        comp_list = []
        for company in companys:
            if company['name'] is not None:
                comp_list.append(company['name'])
            elif company['nameEn'] is not None:
                comp_list.append(company['nameEn'])
        return ','.join(comp_list)

    def parse(self, response):
        data = json.loads(response.body)
        mtime_json = MtimeJson()
        mtime = Mtime()
        mtime_json["mtime_id"] = data["data"]['basic']["movieId"]
        mtime_json["meta"] = data["data"]
        mtime["companies"] = self.extract_companies(data["data"]['basic']["companies"])
        mtime["productionCompanies"] = self.extract_companies(data["data"]['basic']["productionCompanies"])
        mtime["mtime_id"] = data["data"]['basic']["movieId"]
        mtime["imageCount"] = data["data"]['basic']["imageCount"]
        mtime["commentCount"] = data["data"]['basic']["commentCount"]
        mtime["longCommentCount"] = data["data"]['basic']["longCommentCount"]
        mtime["shortCommentCount"] = data["data"]['basic']["shortCommentCount"]
        mtime["newsCount"] = data["data"]['basic']["newsCount"]
        mtime["is3D"] = 1 if data["data"]['basic']["is3D"] else 0
        mtime["isIMAX3D"] = 1 if data["data"]['basic']["isIMAX3D"] else 0
        mtime["isDMAX"] = 1 if data["data"]['basic']["isDMAX"] else 0
        mtime["isIMAX"] = 1 if data["data"]['basic']["isIMAX"] else 0
        mtime["mins"] = data["data"]['basic']["mins"]
        yield mtime
        # yield mtime_json, mtime

