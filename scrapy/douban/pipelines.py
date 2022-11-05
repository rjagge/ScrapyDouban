import hashlib
import logging

from scrapy import Request
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import to_bytes
from twisted.internet.defer import DeferredList
import json

import douban.mysql.database as db
from douban.items import BookMeta, Comment, MovieMeta, MovieBox, Subject, Mtime, MtimeJson

cursor = db.connection.cursor()


class DoubanPipeline(object):
    def get_subject(self, item):
        sql = "SELECT id FROM subjects WHERE douban_id=%s" % item["douban_id"]
        cursor.execute(sql)
        return cursor.fetchone()

    def save_subject(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ",".join(keys)
        temp = ",".join(["%s"] * len(keys))
        sql = "INSERT INTO subjects (%s) VALUES (%s)" % (fields, temp)
        cursor.execute(sql, values)
        return db.connection.commit()

    def get_movie_box_meta(self, item):
        sql = "SELECT id FROM movie_box WHERE name='%s' and release_year_china = '%s'" % (item["name"], item['release_year_china'])
        cursor.execute(sql)
        return cursor.fetchone()

    def save_movie_box_meta(self, item):
        if "（重映）" not in item['name']:
            keys = item.keys()
            values = tuple(item.values())
            fields = ",".join(keys)
            temp = ",".join(["%s"] * len(keys))
            sql = "INSERT INTO movie_box (%s) VALUES (%s)" % (fields, temp)
            cursor.execute(sql, tuple(str(i).strip() for i in values))
            return db.connection.commit()
        else:
            logging.warn('%s is replayed, aborting...' % item['name'])

    def get_mtime_meta(self, item):
        sql = "SELECT id FROM mtime WHERE mtime_id='%s'" % item["mtime_id"]
        cursor.execute(sql)
        return cursor.fetchone()

    def save_mtime_meta(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ",".join(keys)
        temp = ",".join(["%s"] * len(keys))
        sql = "INSERT INTO mtime (%s) VALUES (%s)" % (fields, temp)
        cursor.execute(sql, tuple(str(i).strip() for i in values))
        return db.connection.commit()

    def update_mtime_meta(self, item):
        douban_id = item.pop("mtime_id")
        keys = item.keys()
        values = list(item.values())
        values.append(douban_id)
        fields = ['%s=' % i + '%s' for i in keys]
        sql = "UPDATE mtime SET %s WHERE mtime_id=%s" % (
            ",".join(fields), "%s")
        cursor.execute(sql, tuple(str(i).strip() for i in values))
        return db.connection.commit()
    
    def get_mtime_json_meta(self, item):
        sql = "SELECT id FROM mtime_json WHERE mtime_id='%s'" % item['meta']['basic']['movieId']
        cursor.execute(sql)
        return cursor.fetchone()

    def save_mtime_json_meta(self, item):
        mtime_id = item['meta']['basic']['movieId']
        sql = "INSERT INTO mtime_json (mtime_id, jsondoc) VALUES (%s, %s)" % (mtime_id, json.dumps(item['meta']))
        cursor.execute(sql)
        return db.connection.commit()

    def update_mtime_json_meta(self, item):
        mtime_id = item['meta']['basic']['movieId']
        sql = "UPDATE mtime_json SET jsondoc = %s WHERE mtime_id=%s" % (mtime_id, json.dumps(item['meta']))
        cursor.execute(sql)
        return db.connection.commit()

    def get_movie_meta(self, item):
        if 'douban_id' not in item:
            sql = "SELECT id FROM movies WHERE name='%s'" % item["name"]
        else:
            sql = "SELECT id FROM movies WHERE douban_id='%s'" % item["douban_id"]
        cursor.execute(sql)
        return cursor.fetchone()

    def save_movie_meta(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ",".join(keys)
        temp = ",".join(["%s"] * len(keys))
        sql = "INSERT INTO movies (%s) VALUES (%s)" % (fields, temp)
        cursor.execute(sql, tuple(i.strip() for i in values))
        return db.connection.commit()

    def update_movie_meta(self, item):
        douban_id = item.pop("douban_id")
        keys = item.keys()
        values = list(item.values())
        values.append(douban_id)
        fields = ['%s=' % i + '%s' for i in keys]
        sql = "UPDATE movies SET %s WHERE douban_id=%s" % (
            ",".join(fields), "%s")
        cursor.execute(sql, tuple(i.strip() for i in values))
        return db.connection.commit()

    def update_movie_meta_by_name(self, item):
        douban_id = item.pop("douban_id")
        name = item.pop("name")
        sql = "UPDATE movies SET douban_id = %s WHERE name='%s'" % (
            douban_id, name)
        cursor.execute(sql)
        return db.connection.commit()
    
    def update_movie_meta_by_name_sell(self, item):
        name = item.pop("name")
        year_china = item.pop("release_year_china")
        sell = item.pop("sell")
        sql = "UPDATE movies SET release_year_china = %s WHERE name='%s' and sell='%s'" % (
            year_china, name, sell)
        cursor.execute(sql)
        return db.connection.commit()

    def get_book_meta(self, item):
        sql = "SELECT id FROM books WHERE douban_id=%s" % item["douban_id"]
        cursor.execute(sql)
        return cursor.fetchone()

    def save_book_meta(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ",".join(keys)
        temp = ",".join(["%s"] * len(keys))
        sql = "INSERT INTO books (%s) VALUES (%s)" % (fields, temp)
        cursor.execute(sql, tuple(i.strip() for i in values))
        return db.connection.commit()

    def update_book_meta(self, item):
        douban_id = item.pop("douban_id")
        keys = item.keys()
        values = tuple(item.values())
        values.append(douban_id)
        fields = ["%s=" % i + "%s" for i in keys]
        sql = "UPDATE books SET %s WHERE douban_id=%s" % (
            ",".join(fields), "%s")
        cursor.execute(sql, values)
        return db.connection.commit()

    def get_comment(self, item):
        sql = "SELECT * FROM comments WHERE douban_comment_id=%s" % item["douban_comment_id"]
        cursor.execute(sql)
        return cursor.fetchone()

    def save_comment(self, item):
        keys = item.keys()
        values = tuple(item.values())
        fields = ",".join(keys)
        temp = ",".join(["%s"] * len(keys))
        sql = "INSERT INTO comments (%s) VALUES (%s)" % (fields, temp)
        cursor.execute(sql, values)
        return db.connection.commit()

    def process_item(self, item, spider):
        try:
            if isinstance(item, Subject):
                """
                subject
                """
                exist = self.get_subject(item)
                if not exist:
                    self.save_subject(item)
            elif isinstance(item, MovieMeta):
                """
                meta
                """
                if spider.name == 'movie_sell':
                    exist = self.get_movie_meta(item)
                    if not exist:
                        self.save_movie_meta(item)
                    else:
                        self.update_movie_meta_by_name_sell(item)
                else:
                    exist = self.get_movie_meta(item)
                    if not exist:
                        self.save_movie_meta(item)
                    else:
                        if len(item) > 2:
                            self.update_movie_meta(item)
                        else:
                            self.update_movie_meta_by_name(item)
            elif isinstance(item, BookMeta):
                """
                meta
                """
                exist = self.get_book_meta(item)
                if not exist:
                    self.save_book_meta(item)
                else:
                    self.update_book_meta(item)
            elif isinstance(item, Comment):
                """
                comment
                """
                exist = self.get_comment(item)
                if not exist:
                    self.save_comment(item)
            elif isinstance(item, Mtime):
                """
                Mtime
                """
                exist = self.get_mtime_meta(item)
                if not exist:
                    self.save_mtime_meta(item)
                else:
                    self.update_mtime_meta(item)
            elif isinstance(item, MtimeJson):
                """
                MtimeJson
                """
                exist = self.get_mtime_json_meta(item)
                if not exist:
                    self.save_mtime_json_meta(item)
                else:
                    self.update_mtime_json_meta(item)
            elif isinstance(item, MovieBox):
                """
                MovieBox
                """
                exist = self.get_movie_box_meta(item)
                if not exist:
                    self.save_movie_box_meta(item)
                else:
                    logging.info("item already existed %s" % item['name'])
        except Exception as e:
            logging.warn(item)
            logging.error(e)
        return item


class CoverPipeline(ImagesPipeline):
    def process_item(self, item, spider):
        return item
        if "meta" not in spider.name:
            return item
        info = self.spiderinfo
        requests = arg_to_iter(self.get_media_requests(item, info))
        dlist = [self._process_request(r, info, item) for r in requests]
        dfd = DeferredList(dlist, consumeErrors=1)
        return dfd.addCallback(self.item_completed, item, info)

    def file_path(self, request, response=None, info=None, *, item=None):
        guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        return "%s%s/%s%s/%s.jpg" % (guid[9], guid[19], guid[29], guid[39], guid)

    def get_media_requests(self, item, info):
        if item["cover"]:
            return Request(item["cover"])

    def item_completed(self, results, item, info):
        image_paths = [x["path"] for ok, x in results if ok]
        if image_paths:
            item["cover"] = image_paths[0]
        else:
            item["cover"] = ""
        return item
