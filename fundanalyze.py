# coding:utf-8
#基金分析
import sys
import csv

from fund.AnalyzeFunds import AnalyzeFunds
from fund.FundStore import FundStore 
import threading
from multiprocessing.dummy import Pool as ThreadPool

reload(sys)
sys.setdefaultencoding("utf8")

class AnalyzeMain(object):
    def __init__(self):
        self.infile=open('fundlist.csv','rb')
        self.af=AnalyzeFunds()
        self.lock=threading.Lock()
        self.store=FundStore()
    def fundtest(self):
        #self.af.getAllTop()
        #self.af.allDateEarn()
        #self.af.patchTop()
        self.af.unload_Alltop()
        #print self.af.getTopBydate('2015-02-27')
        '''
        print self.af.startFund('2014-10-31', 6000)
        for i in range(11):
            print self.af.nextMonthFund()
        print '年化'+str((self.af.myfund['amount']*0.995-6000)*100/6000)+'%'
'''

    
    def getFundDays(self):
        csvfile=open('./fundhistory/000001.csv','rb')
        csvreader=csv.reader(csvfile)
        dayls=[]
        for line in csvreader:
            if line[0]<'2016-07-10':
                break
            dayls.append(line[0])   
        return dayls[1:]
    
    
    
    #回收资源   
    def destroy(self):
        self.infile.close()

if __name__ == "__main__":
    am=AnalyzeMain()
    am.fundtest()
    am.destroy()