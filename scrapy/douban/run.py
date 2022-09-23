'''
Date: 2022-09-17 10:30:40
LastEditors: Jagger
Description: 
LastEditTime: 2022-09-22 16:02:56
FilePath: /Repository/ScrapyDouban/scrapy/douban/run.py
'''
import os
from scrapy.cmdline import execute

os.chdir(os.path.dirname(os.path.realpath(__file__)))

try:
    execute(
        [
            'scrapy',
            'runspider',
            '/srv/ScrapyDouban/scrapy/douban/spiders/movie_meta.py',
            '-o',
            'out.json',
        ]
    )
except SystemExit:
    pass
