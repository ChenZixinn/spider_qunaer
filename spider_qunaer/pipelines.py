# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymysql
from sqlalchemy import create_engine
from itemadapter import ItemAdapter
from spider_qunaer.items import *

class SpiderQunaerPipeline:

    pymysql.install_as_MySQLdb()
    DB_STRING = 'mysql+mysqldb://root:Cat010320__@47.113.185.181/qunaer?charset=utf8'
    def process_item(self, item, spider):
        if isinstance(item, SpiderSceneryItem):
            # 如果是景点item
            pass

        elif isinstance(item, SpiderEvaluteItem):
            # 如果是评论item
            pass
        return item

    def open_spider(self, spider):
        # 开启爬虫时先填写好列名
        self.engine = create_engine(self.DB_STRING)
        # self.df.to_sql('news', con=self.engine, chunksize=10000, if_exists="replace", index=False,
        #           dtype=self.dtype_dict)
        try:
            self.engine.execute("DROP TABLE news;")
        except Exception as e:
            print(f"sql error : {e}")
        # self.data

    def close_spider(self, spider):
        # 关闭时保存
        print("close")
        # try:
        #     self.df.dropna(axis=0, how='any', inplace=True)
        # except Exception as e:
        #     print(f"pipline error:{e}")