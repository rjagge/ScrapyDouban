import logging
import database as db

cursor = db.connection.cursor()

sql = "select * from movie_box where mtime_id = 0"
cursor.execute(sql)
box_list = cursor.fetchall()

for movie in box_list:
    sql = "select * from mtime where name = '%s'" % movie['name']
    cursor.execute(sql)
    mtime_result = cursor.fetchall()
    if len(mtime_result) == 1:
        sql = "update movie_box set mtime_id = %s where id = %s" % (mtime_result[0]['mtime_id'], movie['id'])
        cursor.execute(sql)
    else:
        for mtime_movie in mtime_result:
            sql = "update mtime set hit = 1 where id = %s" % mtime_movie['id']
            cursor.execute(sql)
    db.connection.commit()
    