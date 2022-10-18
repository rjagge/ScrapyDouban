import logging
import database as db

cursor = db.connection.cursor()

sql = "select douban_id from movie_box"
cursor.execute(sql)
box_list = cursor.fetchall()

for douban_id in box_list:
    sql = "select count('id') from comments where douban_id = %s" % douban_id['douban_id']
    cursor.execute(sql)
    comments_count = cursor.fetchone()["count('id')"]
    sql = "update movie_box set comments_count = %s where douban_id = %s" % (comments_count, douban_id['douban_id'])
    cursor.execute(sql)
    db.connection.commit()
    