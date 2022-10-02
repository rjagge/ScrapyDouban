import logging
import database as db

cursor = db.connection.cursor()

sql = "select * from movie_box where douban_id = 0"
cursor.execute(sql)
box_list = cursor.fetchall()

for movie in box_list:
    sql = "select * from movies where name = '%s'" % movie['name']
    cursor.execute(sql)
    douban_result = cursor.fetchall()
    if len(douban_result) == 1:
        sql = "update movie_box set douban_id = %s where id = %s" % (douban_result[0]['douban_id'], movie['id'])
        cursor.execute(sql)
    else:
        for douban_movie in douban_result:
            sql = "update movies set hit = 1 where id = %s" % douban_movie['id']
            cursor.execute(sql)
    db.connection.commit()
    