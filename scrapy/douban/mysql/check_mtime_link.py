import logging
import database as db

cursor = db.connection.cursor()

sql = "select * from movie_box where mtime_id != 0"
# sql = "select * from movie_box where mtime_id != 0 and comments_count >= 200"
cursor.execute(sql)
box_list = cursor.fetchall()

empty_list = []
for movie in box_list:
    sql = "select mtime_id from mtime where mtime_id = '%s'" % movie['mtime_id']
    cursor.execute(sql)
    mtime_result = cursor.fetchone()
    if mtime_result is None:
        empty_list.append(movie['mtime_id'])

print(len(empty_list))
    