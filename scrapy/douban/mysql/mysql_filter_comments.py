'''
Date: 2022-10-10 22:21:44
LastEditors: Jagger
Description: 用来筛选短评
LastEditTime: 2022-10-07 22:21:57
FilePath: /zuhui/2022_10_05/sentence_piece.py
'''
from distutils.util import execute
import sys
sys.path.append('..')
import logging
import database as db

#类型筛选： （1）剔除儿童片，因为儿童片的观众并不是豆瓣电影评论的主要撰写者，因此无法用儿童电影的电影评论测算儿童片观众群体对于该电影的真实性感知。
#时间筛选：  1994年1月1日——2022年6月30日，注：因为一些7、8月份的电影还在上映，票房数据仍在变动，因此剔除最近三个月上映的电影。
#数量筛选：  筛选短评数量大于200的电影

#导出数据： a.大于200的电影的所有短评   b.大于200的电影的前200的热门短评

cursor = db.connection.cursor()

def write_file(data,file_dir):
    file = open(file_dir,'w',encoding="utf-8")
    # write header
    keys = ','.join(data[0].keys())
    file.writelines(keys+'\n')
    # write content
    data = [process_comment(i) for i in data]
    data = [i + '\n' for i in data]
    file.writelines(data)
    file.close()

def process_comment(comment):
    for column in comment:
        if column == 'content':
            # 替换回车,消除空格
            comment[column] = comment[column].replace('\n',' ').replace('\r',' ').replace(',','，').strip()
            # 增加引号
            comment[column] = "'%s'" % comment[column]
        else:
            comment[column] = str(comment[column])
    
    return ','.join(comment.values())

# 筛选儿童片类型
# 先从mtime中找出类型为儿童
cursor.execute("select `mtime_id` from `mtime` where `movie_type` LIKE '%儿童%'")
mtime_ids_children = [str(i['mtime_id']) for i in cursor.fetchall()]
# 再从movies中找出release_date 大于2022-6-30的数据
cursor.execute("select `mtime_id` from `mtime` where `year` = 2022 and `month` > 6")
mtime_ids_above_date = [str(i['mtime_id']) for i in cursor.fetchall()]
# 然后再从moive_box中剔除这些id, 并且选择短评数量大于200的电影
cursor.execute("select `douban_id` from `movie_box` where `mtime_id` not in (%s) and comments_count >= 200" % ','.join(mtime_ids_children + mtime_ids_above_date))
douban_ids = [str(i['douban_id']) for i in cursor.fetchall()]
logging.info("%s movies are valid, let's grab their comments." % len(douban_ids))
# 最后再拿出来所有的数据

# douban_ids = douban_ids[:5] 
# 拿大于200电影的所有短评
# comments_all = []
comments_top200 = []
for douban_id in douban_ids:
    cursor.execute("select id, douban_id, douban_comment_id, content, votes, star from `comments` where `douban_id` =  %s order by `votes` desc limit 200" % douban_id)    
    comments = cursor.fetchall()
    # comments_all += comments
    comments_top200 += comments[:200]
# logging.info("The length of comments_all is: %s." % len(comments_all))
logging.info("The length of comments_top200 is: %s." % len(comments_top200))



# write_file(comments_all,'/srv/ScrapyDouban/comments_all.csv')
write_file(comments_top200,'/srv/ScrapyDouban/comments_top200.csv')


    