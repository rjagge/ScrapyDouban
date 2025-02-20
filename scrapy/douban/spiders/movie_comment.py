import json

import douban.mysql.database as db
from douban.items import Comment
from scrapy import Request, Spider

cursor = db.connection.cursor()


class MovieCommentSpider(Spider):
    name = "movie_comment"
    allowed_domains = ["movie.douban.com"]

    def start_requests(self):
        sql = "SELECT douban_id FROM movies WHERE douban_id NOT IN \
            (SELECT douban_id FROM comments GROUP BY douban_id) ORDER BY douban_id DESC"
        cursor.execute(sql)
        movies = cursor.fetchall()
        baseurl = "https://m.douban.com/rexxar/api/v2/movie/%s/interests?count=5&order_by=hot"
        referer = "https://m.douban.com/movie/subject/%s/?from=showing"
        for movie in movies:
            yield Request(
                baseurl % movie["douban_id"], headers={"Referer": referer % movie["douban_id"]},
            )

    def parse(self, response):
        douban_id = response.url.split("/")[-2]
        items = json.loads(response.body)["interests"]
        for item in items:
            comment = Comment()
            comment["douban_id"] = douban_id
            comment["douban_comment_id"] = item["id"]
            comment["douban_user_nickname"] = item["user"]["name"]
            comment["douban_user_avatar"] = item["user"]["avatar"]
            comment["douban_user_url"] = item["user"]["url"]
            comment["content"] = item["comment"]
            comment["votes"] = item["vote_count"]
            yield comment
