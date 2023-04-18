from copy import deepcopy

import scrapy
from lxml import etree
from scrapy import Selector
from spider_qunaer import items

class QunaerSpider(scrapy.Spider):
    name = 'qunaer'
    # allowed_domains = ['travel.qunar.com']
    start_urls = ['http://travel.qunar.com/p-cs299979-chongqing-jingdian']
    page_num = 1
    id = 0

    def parse(self, response):

        # item_evalute = items.SpiderEvaluteItem
        item_scenery = items.SpiderSceneryItem
        # 使用 XPath 表达式来定位和提取数据
        scenery_list = response.xpath("//ul[@class='list_item clrfix']/li")
        print("scenery_list")
        print(scenery_list)
        # 拿到每个景点
        for i in scenery_list:
            self.id += 1
            item_scenery.id = self.id
            item_scenery.scenery_name = i.xpath("./div/div/a/span/text()").extract_first()
            item_scenery.rank = i.xpath("./div/div/div/span[2]/span/text()").extract_first()
            item_scenery.people_percent = i.xpath("./div/div[2]/span/span/text()").extract_first()

            detail_url = i.xpath("./a/@href").extract_first()
            print("detail_url:", detail_url)
            # meta={"item": deepcopy(item), "process": 0, "count": 0}
            yield scrapy.Request(detail_url, callback=self.get_detail, encoding="utf-8",dont_filter=True ,
                                 meta={"item_scenery": deepcopy(item_scenery)})

        # 下一页
        # if self.page_num < 100:
        #     self.page_num += 1
        #     yield scrapy.Request(url=f"{self.start_urls[0]}-1-{self.page_num}", callback=self.parse)


    def get_detail(self, response):
        item_scenery = response.meta["item_scenery"]
        item_scenery.score = response.xpath('//*[@id="js_mainleft"]/div[4]/div/div[2]/div[1]/div[1]/span[1]/text()').extract_first()
        play_time = response.xpath('//div[@class="time"]/text()').extract_first()
        if play_time:
            item_scenery.play_time = play_time.split("：")[1]
        else:
            item_scenery.play_time =None
        # yield item_scenery

        # 第一页评论
        self.get_evalute(response)

        i = 0
        # 第2-5页评论
        for path in response.xpath("//div[@class='b_paging']/a"):
            if i >= 4:
                break
            evalute_path = path.xpath("./@href").extract_first()
            i += 1
            print("evalute_path:", evalute_path)
            yield scrapy.Request(evalute_path, callback=self.get_evalute, encoding="utf-8", dont_filter=True,
                                 meta={"item_scenery": deepcopy(item_scenery)})
            break

    def get_evalute(self, response):
        print("1111111")
        item_scenery = response.meta["item_scenery"]
        evalute_list = response.xpath("//ul[@id='comment_box']/li")
        for evalute in evalute_list:
            item_evalute = items.SpiderEvaluteItem
            item_evalute.content = evalute.xpath("./div[1]/div[1]/div[@class='e_comment_content']").xpath('string(.)').extract()[0].replace("阅读全部", "")  # 内容
            item_evalute.send_time = evalute.xpath("./div[1]/div[1]/div[5]/ul/li[1]/text()").extract_first()  # 评论时间
            item_evalute.user_name = evalute.xpath("./div[2]/div[2]/a/text()").extract_first()  # 用户名
            score = evalute.xpath("./div[1]/div[1]/div[2]/span/span/@class").extract_first()
            if score:
                item_evalute.score = score.split("star_")[-1]   # 评分
            else:
                item_evalute.score = 0
            item_evalute.scenery_name = item_scenery.scenery_name  # 景点名

            print("item: ")
            print(item_evalute.content)
            print(item_evalute.send_time)
            print(item_evalute.user_name)
            print(item_evalute.score)
            print(item_evalute.scenery_name)
            break
            # yield item_evalute


