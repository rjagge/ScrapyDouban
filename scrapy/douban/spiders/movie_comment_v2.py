'''
Date: 2022-09-14 22:30:22
LastEditors: Jagger
Description: 
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
    name = "movie_comment_v2"
    allowed_domains = ["movie.douban.com"]
    baseurl = (
            "https://m.douban.com/rexxar/api/v2/movie/%s/interests?&start=%scount=50&order_by=hot"
        )

    def start_requests(self):
        sql = "SELECT douban_id, comments_count FROM movie_box WHERE douban_id != 0 ORDER BY douban_id"
        cursor.execute(sql)
        movies = cursor.fetchall()
        #   'https://movie.douban.com/subject/26861685/comments?start=20&limit=20&status=P&sort=new_score'
        
        # movies = [
        #     # {'douban_id':26861685},
        #     {'douban_id': 1301272,'comments_count': 0},
        # ]
        for movie in movies:
            douban_id = movie["douban_id"]
            start = movie["comments_count"]
            if start < 350:
                yield Request(
                    self.baseurl % (douban_id, start),
                    headers={"Referer": referer % douban_id},
                )
        
            

    def parse(self, response):
        douban_id = response.url.split("/")[-2]
        data = json.loads(response.body)
        items = data["interests"]
        end_idx = min(data['total'], 400)
        # 如果items > 0， 就继续爬
        # 做这个处理的考虑是，短评的实际数量远远小于data['count']显示的数量
        if len(items) > 0 and data['start'] < end_idx:
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

            # 如果当前电影没有爬完，就继续爬
            _idx = data['count'] + data['start']
            if _idx < end_idx:
                yield Request(
                    response.urljoin(
                        'interests?count=50&order_by=hot&start=%s' % _idx),
                    headers={"Referer": referer % douban_id},
                    callback=self.parse,
                    dont_filter=True
                )