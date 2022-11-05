import logging
import database as db

cursor = db.connection.cursor()

# sql = "select * from movie_box where douban_id != 0 and comments_count >= 200"
sql = "select * from movie_box where douban_id != 0"
cursor.execute(sql)
box_list = cursor.fetchall()

empty_list = []
for movie in box_list:
    sql = "select douban_id from movies where douban_id = '%s'" % movie['douban_id']
    cursor.execute(sql)
    douban_result = cursor.fetchone()
    if douban_result is None:
        empty_list.append(movie['douban_id'])

print(len(empty_list))
    