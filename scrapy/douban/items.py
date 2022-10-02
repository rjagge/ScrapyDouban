'''
Date: 2022-09-14 22:30:22
LastEditors: Jagger
Description: 
LastEditTime: 2022-09-22 16:03:21
FilePath: /Repository/ScrapyDouban/scrapy/douban/items.py
'''
from scrapy import Field, Item


class Subject(Item):
    douban_id = Field()
    type = Field()


class MovieMeta(Item):
    douban_id = Field()
    type = Field()
    cover = Field()
    name = Field()
    slug = Field()
    year = Field()
    release_year_china = Field()
    directors = Field()
    writers = Field()
    actors = Field()
    genres = Field()
    official_site = Field()
    regions = Field()
    languages = Field()
    release_date = Field()
    mins = Field()
    alias = Field()
    imdb_id = Field()
    douban_id = Field()
    douban_score = Field()
    douban_votes = Field()
    tags = Field()
    storyline = Field()
    sell = Field()

class MovieBox(Item):
    name = Field()
    release_year_china = Field()
    box = Field()


class BookMeta(Item):
    douban_id = Field()
    slug = Field()
    name = Field()
    sub_name = Field()
    alt_name = Field()
    cover = Field()
    summary = Field()
    authors = Field()
    author_intro = Field()
    translators = Field()
    series = Field()
    publisher = Field()
    publish_date = Field()
    pages = Field()
    price = Field()
    binding = Field()
    isbn = Field()
    douban_id = Field()
    douban_score = Field()
    douban_votes = Field()
    tags = Field()


class Comment(Item):
    douban_id = Field()
    douban_comment_id = Field()
    douban_user_nickname = Field()
    douban_user_avatar = Field()
    douban_user_url = Field()
    content = Field()
    votes = Field()
    star = Field()

class Mtime(Item):
    mtime_id = Field()
    name = Field()
    name_en = Field()
    movie_type = Field()
    rating = Field()
    directors = Field()
    actors = Field()
    location_name = Field()
    realtime = Field()
    year = Field()
    month = Field()
    day = Field()
