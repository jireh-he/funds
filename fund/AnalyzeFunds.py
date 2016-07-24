#coding:utf-8
'''
Created on 2016年7月21日

@author: 何以勒
'''
import datetime,os
import config
import json
from db.Sqlite3Client import Sqlite3Client



class AnalyzeFunds(object):
    '''
     分析每一天基金的月涨幅数据
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.sqlDir = os.path.join(config.FUNDSTORE_DIR,'funds.db')
        self.s3c=Sqlite3Client(self.sqlDir)      
        self.datels=[d[0] for d in self.s3c.query('select jzrq from fundrq')]
        self.myfund={}
        self.portion=[5,2,1]
    '''
    datestr 日期字符串 'yyyy-mm-dd'
    
    '''
    def dateminus(self,datestr,days=30):
        if datestr<'2010-01-01':
            return None
        d1=datetime.datetime.strptime(datestr,'%Y-%m-%d')
        d2=d1-datetime.timedelta(days=days)
        return d2.strftime('%Y-%m-%d')
    
    def dateadd(self,datestr,days=30):
        if datestr>'2016-07-20':
            return None
        d1=datetime.datetime.strptime(datestr,'%Y-%m-%d')
        d2=d1+datetime.timedelta(days=days)
        return d2.strftime('%Y-%m-%d')

    def datedelta(self,datestr1,datestr2):
        d1=datetime.datetime.strptime(datestr1,'%Y-%m-%d')
        d2=datetime.datetime.strptime(datestr2,'%Y-%m-%d')
        return abs((d1-d2).days)
      
    
    def getMonthRise(self,datestr,fundcode):
        pastdate=self.dateminus(datestr)
        sqlstr='select * from fundshistory where fundcode=? and jzrq>=? limit 0,1'
        fundvalue=self.s3c.query(sqlstr,[fundcode,datestr])
        #print fundvalue
        if len(fundvalue)>0:fundvalue=fundvalue[0] 
        else:
            return []
        if self.datedelta(datestr, fundvalue[1])>10:
            return []
        pastfund=self.s3c.query(sqlstr,[fundcode,pastdate])
        if len(pastfund)>0:pastfund=pastfund[0] 
        else:
            return []
        if len(fundvalue)>2 and len(pastfund)>2:
            rise=round((fundvalue[2]-pastfund[2])*100/pastfund[2],2)
            return [fundvalue[0],rise,fundvalue[1],fundvalue[2],pastfund[1],pastfund[2]]
        else:
            return []
        
    def getTopMonthRise(self,datestr):
        pastdate=self.dateminus(datestr)
        pastdate2=self.dateminus(pastdate)
        if pastdate is None or pastdate2 is None:
            return []        
        sqlstr1='(select fundcode,max(jzrq) as jzrq,dwjz from fundshistory where jzrq<=? and jzrq>? and sgzt=\'开放申购\' and shzt=\'开放赎回\' group by fundcode) a'
        sqlstr2='(select fundcode,max(jzrq) as jzrq,dwjz from fundshistory where jzrq<=? and jzrq>?  group by fundcode) b'
        sqlstr3='select a.fundcode,round((a.dwjz-b.dwjz)*100/b.dwjz,2) as rise,a.jzrq as jzrq1,a.dwjz as dwjz1,b.jzrq as jzrq2,b.dwjz as dwjz2 from '+sqlstr1+' join '+sqlstr2+' on a.fundcode=b.fundcode order by rise desc limit 0,5'
        fundls=self.s3c.query(sqlstr3, [datestr,pastdate,pastdate,pastdate2])
        return fundls
    
    def getAllTop(self):
        self.s3c.execute('create table if not exists alltopfund (fundcode text, zhangfu real,jzrq1 text, dwjz1 real,jzrq2 text,dwjz2 real,primary key(fundcode,jzrq1))')
        maxdate=self.s3c.query('select max(jzrq1) from alltopfund')[0][0]
        fundrows=[]
        for d in self.datels:
            if d<maxdate:
                continue
            print d
            fundrows+=self.getTopMonthRise(d)
            if len(fundrows)>100:
                self.s3c.insertbatchdata('alltopfund', fundrows)
                print u'成功插入100多条记录'
                fundrows=[]
        self.s3c.insertbatchdata('alltopfund', fundrows)
        print u'完成所有导入'    
    def patchTop(self):
        riqi=['2011-01-30','2011-04-22','2011-04-25','2011-09-05','2011-12-26','2013-02-15','2013-06-24','2013-06-25','2013-06-26','2014-05-04','2015-06-19','2015-10-02','2015-10-05','2015-12-20','2016-01-04']
        fundrows=[]
        for d in riqi:
            print d
            fundrows+=self.getTopMonthRise(d)
        self.s3c.insertbatchdata('alltopfund', fundrows)    
    #根据日期获得排名前5的基金    
    def getTopBydate(self,datestr):
        return self.s3c.query('select * from alltopfund where jzrq1<=? and dwjz1>0.5 order by jzrq1 desc limit 0,5',[datestr])
    
    #选择日期开始建仓
    def startFund(self,datestr,amount):
        self.myfund['date']=datestr
        self.myfund['fundls']=self.getTopBydate(datestr)[:3]
        self.myfund['portion']=[round(amount*self.portion[i]/(sum(self.portion)*p[3]),2) for (i,p) in enumerate(self.myfund['fundls'])] 
        self.myfund['amount']=amount
        self.myfund['cost']=amount
        return self.myfund
    
    #下一月的基金池状态
    def nextMonthFund(self):
        self.myfund['date']=self.dateadd(self.myfund['date'])
        #if (self.myfund['amount']-self.myfund['cost'])/self.myfund['cost']>=0.1:
        #    return self.startFund(self.myfund['date'], self.myfund['amount']*0.99)
        
        if self.myfund['date']>'2016-07-20':
            exit()
        
        self.myfund['fundls']=[self.getMonthRise(self.myfund['date'], f[0]) for f in self.myfund['fundls']]
        #print self.myfund['date']
        self.myfund['date']=self.myfund['fundls'][0][2]
        try:
            order=sorted(self.myfund['fundls'],key=lambda f:f[1],reverse=True)
        except Exception,error:
            print error,self.myfund['fundls']
                   
        appendfund=self.getTopBydate(self.myfund['date'])[0]
        del(order[2])
        order.append(appendfund)
        #print self.myfund['date'],appendfund[0],order[2][0]
        #newportion=[1,1,1]
        '''
        for i,f in enumerate(self.myfund['fundls']):
             if order[0][0] is f[0]:
                 newportion[0]=self.myfund['portion'][i]
             if order[1][0] is f[0]:
                 newportion[1]=self.myfund['portion'][i] 
             if order[2][0] is f[0]:
                 #print order[2],appendfund                 
                 del order[2]
                 newportion[2]=round(f[3]*self.myfund['portion'][i]*0.99/appendfund[3],2)
                 order.append(appendfund)
        '''      
        self.myfund['portion']=[round(self.myfund['amount']*0.995*self.portion[i]/(sum(self.portion)*p[3]),2) for (i,p) in enumerate(self.myfund['fundls'])] 
        self.myfund['fundls']=order
        #print self.myfund['fundls']
        self.myfund['amount']=sum([self.myfund['portion'][i]*f[3] for (i,f) in enumerate(self.myfund['fundls'])])   
        return self.myfund
    
    def allDateEarn(self):
        startmoney=6000
        for d in self.datels:
            self.startFund(d, startmoney)
            for i in range(11):
                self.nextMonthFund()
            print d,'年化'+str((self.myfund['amount']*0.995-startmoney)*100/startmoney)+'%'
            
    
    def unload_Alltop(self):
        alltop=self.s3c.query('select * from alltopfund')
        funds={f[2]:[] for f in alltop}
        for f in alltop:
            funds[f[2]].append(f)
        jsondir=os.path.join(config.FUNDHISTORY,'alltop.json')
        alltopjson=open(jsondir,'wb')
        alltopjson.writelines(json.dumps(funds))
        alltopjson.close()
        print u'成功导出%s条记录'%str(len(funds))
                  