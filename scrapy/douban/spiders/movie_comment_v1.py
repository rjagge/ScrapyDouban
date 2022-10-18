'''
Date: 2022-09-14 22:30:22
LastEditors: Jagger
Description: 原来的豆瓣短评爬虫，已经弃用，详见v2。
LastEditTime: 2022-09-22 16:03:59
FilePath: /Repository/ScrapyDouban/scrapy/douban/spiders/movie_comment_v1.py
'''
import json

import douban.mysql.database as db
from douban.items import Comment
from scrapy import Request, Spider

cursor = db.connection.cursor()
referer = "https://m.douban.com/movie/subject/%s/?from=showing"


class MovieCommentSpider(Spider):
    name = "movie_comment_v1"
    allowed_domains = ["movie.douban.com"]

    def start_requests(self):
        sql = "SELECT douban_id FROM movies WHERE douban_id NOT IN \
            (SELECT douban_id FROM comments GROUP BY douban_id) ORDER BY douban_id DESC"
        cursor.execute(sql)
        movies = cursor.fetchall()
        #   'https://movie.douban.com/subject/26861685/comments?start=20&limit=20&status=P&sort=new_score'
        baseurl = (
            "https://m.douban.com/rexxar/api/v2/movie/%s/interests?count=50&order_by=hot"
        )
        # movies = [
        #     # {'douban_id':26861685},
        #     {'douban_id': 35215390},
        # ]
        for movie in movies:
            yield Request(
                baseurl % movie["douban_id"],
                headers={"Referer": referer % movie["douban_id"]},
            )
            

    def parse(self, response):
        douban_id = response.url.split("/")[-2]
        data = json.loads(response.body)
        items = data["interests"]
        for item in items:
            comment = Comment()
            comment["douban_id"] = douban_id
            comment["douban_comment_id"] = item["id"]
            comment["douban_user_nickname"] = item["user"]["name"]
            comment["douban_user_avatar"] = item["user"]["avatar"]
            comment["douban_user_url"] = item["user"]["url"]
            comment["content"] = item["comment"]
            comment["votes"] = item["vote_count"]
            comment["star"] = 0 if item['rating'] is None else item["rating"]["value"]
            yield comment

        # 如果没有爬完，就继续爬
        end_idx = data['count'] + data['start']
        if end_idx < data['total']:
            yield Request(
                response.urljoin(
                    'interests?count=50&order_by=hot&start=%s' % end_idx),
                headers={"Referer": referer % douban_id},
                callback=self.parse,
                dont_filter=True
            )
