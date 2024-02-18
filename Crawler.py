import pandas as pd        #保存文件
import numpy as np
import requests            #获取网页
from lxml import etree     #解析文档
from faker import Factory# 生成不同的user-agent
import time
import random 
from multiprocessing.dummy import Pool as ThreadPool  # 线程池


class crawler_url_list():
    def __init__(self,stock,start_page, end_page, IP_pool, cookie , text_type):
        """ 
        stock：股票代码
        start_page/end_page：起始页码
        IP_pool：ip池
        """
        self.stock = stock
        self.start_page = start_page
        self.end_page = end_page
        self.IP_pool = IP_pool
        self.cookie = cookie
        self.text_type = text_type

    def get_page_url_list(self):  ## 获取 网页列表， 如 https://guba.eastmoney.com/list,zssh000016_1.html
        page_url_list = []
        page_list = np.arange(self.start_page,self.end_page+1).tolist()
        random.shuffle(page_list)
        for i in page_list:
            if self.text_type == "评论":
                page_url_list.append(f"http://guba.eastmoney.com/list,{self.stock},f_{i}.html")  ## 评论网页
            elif self.text_type == "资讯":
                page_url_list.append(f"http://guba.eastmoney.com/list,{self.stock},1,f_{i}.html")  ## 资讯网页
            elif self.text_type == "研报":
                page_url_list.append(f"http://guba.eastmoney.com/list,{self.stock},2,f_{i}.html")  ## 研报网页
            else:
                print("暂未设置",self.text_type,"类型")
        return(page_url_list,page_list)        

    def get_crawler_url_list(self,arg): ## 获取 需要爬虫网页列表， 如 'https://guba.eastmoney.com/news,zssh000016,1263001326.html'
        url, page_num = arg
        ## 生成随机user-agent
        fc = Factory.create() 
        header = {'User-Agent': fc.user_agent(),
               "Cookie":self.cookie}
        ## 获取网页
        response = requests.get(url = url,headers=header,proxies=random.choice(self.IP_pool)) 
        #print("正在爬取:", url, "状态:",response)
        ## 解析文档
        root = etree.HTML(response.text) 
        
        ## 评论/资讯
        if self.text_type == "评论" or self.text_type == "资讯":
            readers = root.xpath("//tr[@class = 'listitem']//div[@class = 'read']/text()")  ## 阅读数
            reply = root.xpath("//tr[@class = 'listitem']//div[@class = 'reply']/text()")   ## 评论数
            title = root.xpath("//tr[@class = 'listitem']//div[@class = 'title']/a/text()")   ## 标题内容
            temp_times = root.xpath("//tr[@class = 'listitem']//div[contains(@class,'update')]/text()")   ## 时间   不完整
            temp_url = root.xpath("//tr[@class = 'listitem']//div[@class = 'title']/a/@href")   ## 文本链接 不完整
        elif self.text_type == "研报":
            readers = root.xpath("//tr[@class = 'listitem report_item']//div[@class = 'read']/text()")  ## 阅读数
            reply = root.xpath("//tr[@class = 'listitem report_item']//div[@class = 'reply']/text()")   ## 评论数
            title = root.xpath("//tr[@class = 'listitem report_item']//div[@class = 'title']/a/text()")   ## 标题内容
            temp_times = root.xpath("//tr[@class = 'listitem report_item']//div[contains(@class,'update')]/text()")   ## 时间   不完整
            temp_url = root.xpath("//tr[@class = 'listitem report_item']//div[@class = 'title']/a/@href")   ## 文本链接 不完整
        else:
            print("暂未设置【",self.text_type,"】类型")
        
        times = temp_times #= []
        url_list = []
        ## 时间  文本链接  不完整处理
        for i in range(len(temp_url)):    
            if temp_url[i][1:5] == "news":
                temp_url1 = "https://guba.eastmoney.com" + temp_url[i]
                url_list.append(temp_url1)
            else:
                url_list.append("nan")
        ## 加入页码，页内顺序
        page = [page_num]*len(title)
        order = np.arange(len(title)).tolist()
        ## 设置睡眠，反爬虫
        t = random.uniform(3, 5)
        time.sleep(t)
        return(readers,reply,title,times,url_list,page,order)   
    
    def run_multithreading(self,threading_num, sleep_nums, sleep_time=30):
        """
        threading_num：线程个数
        sleep_nums：爬取多少个后睡眠一段时间
        sleep_time：爬完sleep_nums个后睡眠多久时间
        """
        start = time.time()
        get_crawler_url_list = self.get_crawler_url_list
        page_url_list, page_list = self.get_page_url_list()
        
        nums = len(page_url_list)
        if nums<sleep_nums:print("ERROR:sleep_nums大于nums")
        temp_index = np.arange(0,nums,sleep_nums).tolist()  ## 顺序相同   
        temp_index.append(nums)

        all_data = []
        for i in range(len(temp_index)-1):
            temp_page_url_list = page_url_list[temp_index[i]:temp_index[i+1]]
            temp_page_list = page_list[temp_index[i]:temp_index[i+1]]
            input_args = [(temp_page_url_list[i],temp_page_list[i]) for i in range(len(temp_page_list))]

            ## 多线程
            p = ThreadPool(threading_num)
            all_data += p.map(get_crawler_url_list,input_args)   ## map 非阻塞方法--线程并发执行,  imap 阻塞方法--线程顺序执行
            #print("****    正在睡眠    ****")
            time.sleep(sleep_time)
        
        ## 数据输出
        print("正在输出数据")
        page_nums = len(all_data)
        all_readers = sum([all_data[i][0] for i in range(page_nums)],[])
        all_reply = sum([all_data[i][1] for i in range(page_nums)],[])
        all_title = sum([all_data[i][2] for i in range(page_nums)],[])
        all_time = sum([all_data[i][3] for i in range(page_nums)],[])
        all_url = sum([all_data[i][4] for i in range(page_nums)],[])
        all_page = sum([all_data[i][5] for i in range(page_nums)],[])
        all_order = sum([all_data[i][6] for i in range(page_nums)],[])                                                           
        output_data = np.array((all_readers,all_reply,all_title,all_time,all_url,all_page,all_order),dtype=object)
        
        end = time.time()
        print("运行时间：",(end-start))
        return(output_data)
    
    
    