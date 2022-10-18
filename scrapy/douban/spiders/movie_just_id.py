import douban.mysql.database as db
import douban.util as util
import douban.validator as validator
from douban.items import MovieMeta
from scrapy import Spider
import json

cursor = db.connection.cursor()


class MovieMetaSpider(Spider):
    name = "movie_just_id"
    allowed_domains = ["movie.douban.com"]
    user_agent = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko"
    sql = 'SELECT * FROM movies WHERE name is not null AND douban_id = ""'
    cursor.execute(sql)
    movies = cursor.fetchall()
    start_urls = ("https://douban.8610000.xyz/suggest/%s.json" %
                  i["name"] for i in movies)
    # start_urls = ["https://douban.8610000.xyz/suggest/我想吃掉你的胰脏.json"]

    def set_douban_id(self, meta, response):
        meta["douban_id"] = json.loads(response.body)[0]['id']
        return meta

    def set_type(self, meta, response):
        regex = '//text()[preceding-sibling::span[text()="集数:"]][fo\
llowing-sibling::br]'
        match = response.xpath(regex).get()
        if match:
            meta["type"] = "tv"
        else:
            meta["type"] = "movie"
        return meta

    def set_cover(self, meta, response):
        regex = '//img[@rel="v:image"]/@src'
        match = response.xpath(regex).get()
        if match:
            meta["cover"] = match.replace("s_ratio_poster", "l_ratio_poster")
        else:
            meta["cover"] = ""
        return meta

    def set_name(self, meta, response):
        meta["name"] = json.loads(response.body)[0]['title']
        return meta

    def set_slug(self, meta, response):
        meta["slug"] = util.shorturl(meta["douban_id"])
        return meta

    def set_year(self, meta, response):
        regex = '//span[@class="year"]/text()'
        match = response.xpath(regex).get()
        if match:
            meta["year"] = validator.match_year(match)
        return meta

    def set_directors(self, meta, response):
        regex = '//a[@rel="v:directedBy"]/text()'
        matches = response.xpath(regex).getall()
        meta["directors"] = validator.process_slash_str("/".join(matches))
        return meta

    def set_writers(self, meta, response):
        regex = '//span[preceding-sibling::span[text()="编剧"]]/a/text()'
        matches = response.xpath(regex).getall()
        meta["writers"] = validator.process_slash_str("/".join(matches))
        return meta

    def set_actors(self, meta, response):
        regex = '//a[@rel="v:starring"]/text()'
        matches = response.xpath(regex).getall()
        meta["actors"] = validator.process_slash_str("/".join(matches))
        return meta

    def set_genres(self, meta, response):
        regex = '//span[@property="v:genre"]/text()'
        matches = response.xpath(regex).getall()
        meta["genres"] = "/".join(matches)
        return meta

    def set_official_site(self, meta, response):
        regex = '//a[preceding-sibling::span[text()="官方网站:"]][following-si\
bling::br]/@href'
        match = response.xpath(regex).get()
        if match:
            meta["official_site"] = validator.process_url(match)
        return meta

    def set_regions(self, meta, response):
        regex = '//text()[preceding-sibling::span[text()="制片国家/地区:"]][fo\
llowing-sibling::br]'
        match = response.xpath(regex).get()
        if match:
            meta["regions"] = match
        return meta

    def set_languages(self, meta, response):
        regex = '//text()[preceding-sibling::span[text()="语言:"]][following-s\
ibling::br]'
        match = response.xpath(regex).get()
        if match:
            meta["languages"] = match
        return meta

    def set_release_date(self, meta, response):
        regex = '//span[@property="v:initialReleaseDate"]/@content'
        match = response.xpath(regex).get()
        if match:
            release_date = validator.str_to_date(validator.match_date(match))
            if release_date:
                meta["release_date"] = release_date
        return meta

    def set_runtime(self, meta, response):
        regex = '//span[@property="v:runtime"]/@content'
        match = response.xpath(regex).get()
        if match:
            meta["mins"] = match
        return meta

    def set_alias(self, meta, response):
        regex = '//text()[preceding-sibling::span[text()="又名:"]][following-s\
ibling::br]'
        match = response.xpath(regex).get()
        if match:
            meta["alias"] = validator.process_slash_str(match)
        return meta

    def set_imdb_id(self, meta, response):
        regex = '//a[preceding-sibling::span[text()="IMDb链接:"]][following-si\
bling::br]/@href'
        match = response.xpath(regex).get()
        if match:
            meta["imdb_id"] = match.strip().split("?")[0][27:]
        return meta

    def set_score(self, meta, response):
        regex = '//strong[@property="v:average"]/text()'
        match = response.xpath(regex).get()
        if match:
            meta["douban_score"] = match
        return meta

    def set_votes(self, meta, response):
        regex = '//span[@property="v:votes"]/text()'
        match = response.xpath(regex).get()
        if match:
            meta["douban_votes"] = match
        return meta

    def set_tags(self, meta, response):
        regex = '//div[@class="tags-body"]/a/text()'
        matches = response.xpath(regex).getall()
        meta["tags"] = "/".join(matches)
        return meta

    def set_storyline(self, meta, response):
        regex = '//span[@class="all hidden"]/text()'
        matches = response.xpath(regex).getall()
        if matches:
            meta["storyline"] = "<br>".join([item.strip() for item in matches])
        else:
            regex = '//span[@property="v:summary"]/text()'
            matches = response.xpath(regex).getall()
            if matches:
                meta["storyline"] = "<br>".join(
                    [item.strip() for item in matches])
        return meta

    def set_comments(self, meta, response):
        regex = '//div[@class="comment"]/p/text()'
        matches = response.xpath(regex).getall()
        meta["comments"] = "/".join((i.strip() for i in matches))
        return meta

    def parse(self, response):
        meta = MovieMeta()
        self.set_douban_id(meta, response)
        self.set_name(meta, response)
        return meta
