import logging
import database as db

cursor = db.connection.cursor()

sql = "select a.release_year_china, a.box, b.directors from movie_box a, movies b where a.douban_id = b.douban_id"
cursor.execute(sql)
box_list = cursor.fetchall()

for movie in box_list:
    box = movie['box']
    year = movie['release_year_china']
    directors = movie['directors'].split("/") if movie['directors'] else []
    if len(directors) > 0:
        for director in directors:
            sql = "select id from directors where name = '%s' and year = %s" % (director, year)
            cursor.execute(sql)
            exist = cursor.fetchone()
            # 如果当年当演员没有box，就新建一条数据
            if not exist:
                sql = "insert into directors (name, year, box, movie_count) VALUES ('%s', %s, %s, 1)" % (director, year, box)
                cursor.execute(sql)
            # 如果当年当演员有box，那么就累加
            else:
                sql = "update directors set box = box + %s, movie_count = movie_count + 1 where name = '%s' and year = %s" % (box, director, year) 
                cursor.execute(sql)
            db.connection.commit()
            

    db.connection.commit()
    