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

    df_scenery = pd.DataFrame()
    df_evaluate = pd.DataFrame()

    pymysql.install_as_MySQLdb()

    # 数据库名和表名字
    db_name = "qunaer"
    table_name = "scenery"
    db_username = 'root'
    db_password = ''
    db_ip = "127.0.0.1"
    DB_STRING = f'mysql+mysqldb://{db_username}:{db_password}@{db_ip}/{db_name}?charset=utf8'
    engine = None
    dtype_scenery = {
        "scenery_name": NVARCHAR(length=255),  # id
        "people_percent": VARCHAR(length=32),  # 多少人去过
        "rank": INTEGER,  # 排名
        "score": VARCHAR(length=32),  # 评分
        "play_time": NVARCHAR(length=32),  # 建议游玩时间
    }

    """
    yield 的数据会到这里
    """
    def process_item(self, item, spider):
        if isinstance(item, SpiderSceneryItem):
            # 如果是景点item
            df = pd.DataFrame.from_dict([item])
            self.df_scenery = pd.concat([self.df_scenery, df])
            # print(self.df_scenery)

        elif isinstance(item, SpiderEvaluteItem):
            # 如果是评论item
            df = pd.DataFrame.from_dict([item])
            self.df_evaluate = pd.concat([self.df_evaluate, df])
            # print(self.df_evaluate)
        return item

    def open_spider(self, spider):
        # 链接数据库
        self.engine = create_engine(self.DB_STRING)

    def close_spider(self, spider):
        # 关闭时保存
        print("close")
        # 转化为
        self.df_evaluate.to_csv("./df_evaluate.csv")
        scenery_list = set(self.df_evaluate["scenery_name"])
        # 根据景点名写入到文件
        for scenery in scenery_list:
            self.df_evaluate[self.df_evaluate["scenery_name"] == scenery].to_csv(f"./evalute_data/{scenery}.csv", index=False)
        # 写入数据库，写入失败则写入到csv
        try:
            self.df_scenery.to_sql(self.table_name, con=self.engine, chunksize=10000, if_exists="replace", index=False,
                      dtype=self.dtype_scenery)
            # # 增加主键
            with self.engine.connect() as con:
                con.execute(
                    f"""ALTER TABLE {self.table_name} ADD COLUMN id int(11)  NOT NULL AUTO_INCREMENT first,ADD primary KEY(id);""")
        except Exception as e:
            print(e)
            self.df_scenery.to_csv("./scenery.csv", index=False)