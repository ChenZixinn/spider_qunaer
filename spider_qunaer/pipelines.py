# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import pandas as pd
import pymysql
from sqlalchemy import create_engine, NVARCHAR, VARCHAR, TEXT, DATETIME, FLOAT, INTEGER
from itemadapter import ItemAdapter
from spider_qunaer.items import *

class SpiderQunaerPipeline:
    db_name = "qunaer"
    table_name = "scenery"

    df_scenery = pd.DataFrame()
    df_evaluate = pd.DataFrame()

    pymysql.install_as_MySQLdb()
    DB_STRING = 'mysql+mysqldb://root:Cat010320__@47.113.185.181/qunaer?charset=utf8'
    engine = None
    dtype_scenery = {
        "scenery_name": NVARCHAR(length=255),  # id
        "people_percent": FLOAT,  # 多少人去过
        "rank": INTEGER,  # 排名
        "content": TEXT,  # 评论内容
        "score": FLOAT,  # 评分
        "play_time": NVARCHAR(length=32),  # 建议游玩时间
    }

    def process_item(self, item, spider):
        if isinstance(item, SpiderSceneryItem):
            # 如果是景点item
            df = pd.DataFrame.from_dict([item])
            self.df_scenery = pd.concat([self.df_scenery, df])

        elif isinstance(item, SpiderEvaluteItem):
            # 如果是评论item
            df = pd.DataFrame.from_dict([item])
            self.df_evaluate = pd.concat([self.df_evaluate, df])
        return item

    def open_spider(self, spider):
        # 开启爬虫时先填写好列名
        self.engine = create_engine(self.DB_STRING)
        # try:
        #     self.engine.execute("DROP TABLE scenery;")
        # except Exception as e:
        #     print(f"sql error : {e}")
        # self.data

    def close_spider(self, spider):
        print(self.df_scenery)

        print(self.df_evaluate)
        # 关闭时保存
        print("close")
        # self.df.to_sql(self.table_name, con=self.engine, chunksize=10000, if_exists="replace", index=False,
        #           dtype=self.dtype_scenery)
        # # 增加主键
        # with self.engine.connect() as con:
        #     con.execute("""ALTER TABLE `{}`.`{}` \
        #             ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT FIRST, \
        #             ADD PRIMARY KEY (`id`);"""
        #                 .format(self.db_name, self.table_name))
        # try:
        #     self.df.dropna(axis=0, how='any', inplace=True)
        # except Exception as e:
        #     print(f"pipline error:{e}")