'''
Date: 2022-09-14 22:30:22
LastEditors: Jagger
Description: 根据mtime_id来爬mtime的数据
LastEditTime: 2022-09-22 16:03:59
FilePath: /Repository/ScrapyDouban/scrapy/douban/spiders/movie_comment_v1.py
'''
from email.header import Header
import json
from unicodedata import name
from urllib.parse import urlencode

import douban.mysql.database as db
from douban.items import Mtime, MtimeJson
from scrapy import Request, Spider

cursor = db.connection.cursor()
# referer = "https://m.douban.com/movie/subject/%s/?from=showing"


class MovieMtimeIDSpider(Spider):
    name = "movie_mtime_by_id"
    allowed_domains = ["mtime.com"]

    def start_requests(self):
        #   'https://movie.douban.com/subject/26861685/comments?start=20&limit=20&status=P&sort=new_score'
        baseurl = (
            "http://front-gateway.mtime.com/library/movie/detail.api?movieId=%s&locationId=290"
        )

        # 先找movie_box中所有的mtime_id
        sql = "select * from movie_box where mtime_id != 0"
        cursor.execute(sql)
        mtime_ids = cursor.fetchall()

        # mtime_ids = [
        #     {'mtime_id': 25920}
        # ]
            
        # 然后找在mtime表中缺失的mtime_id，然后根据id爬取数据
        for mtime_id in mtime_ids:
            sql = "select mtime_id from mtime where mtime_id = '%s'" % mtime_id['mtime_id']
            cursor.execute(sql)
            mtime_result = cursor.fetchone()
            if mtime_result is None:
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
    
    def extract_directors(self, directors):
        if directors is None:
            return ''
        driector_list = []
        for director in directors:
            if director['name'] is not None:
                driector_list.append(director['name'])
            elif director['nameEn'] is not None:
                driector_list.append(director['nameEn'])
        return ','.join(driector_list)

    def parse(self, response):
        data = json.loads(response.body)
        mtime = Mtime()
        mtime["mtime_id"] = data["data"]['basic']["movieId"]
        mtime["name"] = data["data"]["basic"]["name"]
        mtime["name_en"] = data["data"]["basic"]["nameEn"]
        mtime["movie_type"] = ",".join(data["data"]["basic"]["type"]) if data["data"]["basic"]["type"] else ""
        mtime["rating"] = data["data"]["basic"]["overallRating"]
        mtime["directors"] = self.extract_directors(data["data"]["basic"]["directors"])
        mtime["actors"] = self.extract_directors(data["data"]["basic"]["actors"])
        mtime["location_name"] = data["data"]["basic"]["releaseArea"]
        # mtime["release_area"] = data["data"]["basic"]["releaseArea"]
        mtime["realtime"] = data["data"]["basic"]["releaseDateNew"]
        mtime["year"] = data["data"]["basic"]["releaseDate"][:4] if data["data"]["basic"]["releaseDate"] else 0
        mtime["month"] = data["data"]["basic"]["releaseDate"][4:6] if data["data"]["basic"]["releaseDate"] else 0
        mtime["day"] = data["data"]["basic"]["releaseDate"][6:] if data["data"]["basic"]["releaseDate"] else 0
        mtime["companies"] = self.extract_companies(data["data"]['basic']["companies"])
        mtime["productionCompanies"] = self.extract_companies(data["data"]['basic']["productionCompanies"])
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

