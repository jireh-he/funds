#coding:utf-8
'''
Created on 2016年5月15日

@author: jireh
'''
import os
import sys
sys.path.append(os.path.abspath(".."))
from bs4 import BeautifulSoup
from html_downloader import HtmlDownloader
import csv
import threading
from multiprocessing.dummy import Pool as ThreadPool

#基金历史净值采集API
FundHistoryAPI=u'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&page=1'


#获取基金列表和历史净值,数据来自东方财富网

class FundGet(object):
    def __init__(self):
        self.down=HtmlDownloader()
    
    def getFundHistory(self,fundcode):
        try:
            fundapiurl=FundHistoryAPI+'&per=10000&code='+fundcode
            htmldoc=self.down.download(fundapiurl)
            if htmldoc is None:
                print 'Error'
                return None
            #<table class="w782 comm lsjz">..        
            soup=BeautifulSoup(htmldoc,'html.parser',from_encoding='gbk')
            trlist=soup.find('table').find_all('tr')
            res=[]
            for tr in trlist[1:]:
                tds=tr.find_all('td')
                res.append(map(lambda t:t.text.strip(),tds))
            return res
        except Exception,error:
            print error
    
            
        
