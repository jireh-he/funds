#coding:utf-8
'''
Created on 2016年7月22日

@author: jireh
'''
import os,csv
import config
from db.Sqlite3Client import Sqlite3Client
from itertools import islice


class FundStore(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.load_sql_storage('funds')
        self.s3c=Sqlite3Client(self.sqlDir)
        
    def load_sql_storage(self,dbname):
        self.sqlDir = os.path.join(config.FUNDSTORE_DIR,'%s.db'%dbname)
        with Sqlite3Client(self.sqlDir) as s3c:
            s3c.execute('create table if not exists fundshistory (fundcode text, jzrq text, dwjz real, ljjz real,sgzt text,shzt text,fhsp text,primary key(fundcode,jzrq))')
            s3c.execute('create table if not exists fundslist (xuhao integer,fundcode text PRIMARY KEY,jjmc text)')

    
    def insert_update(self,tablename,rows=[]):
        for row in rows:
            try:
                self.s3c.insert_data(tablename,row)
            except Exception,error:
                continue
                tableinfo=self.s3c.query('PRAGMA table_info(fundshistory)')
                fields=[f[1] for f in tableinfo]
                formatsql='update %s set %s=? where %s=%s and %s=%s'%(tablename,'=?,'.join(fields),fields[0],row[0],fields[1],row[1])
                self.s3c.execute(formatsql,row)
                continue
        print u'更新表%s成功'%tablename        
    def batchinsert(self,tablename,rows):
        try:
            self.s3c.insertbatchdata(tablename, rows)
        except Exception,error:
            print error
        print u'插入基金%s成功'%rows[0][0]        
    def load_fundhistory(self,fundcode):
        fundcsv=open(os.path.join(config.FUNDHISTORY,'%s.csv'%fundcode),'rb')
        csvreader=csv.reader(fundcsv)
        funds=[]
        line=[]
        try:
            for line in islice(csvreader,1,None):
                fund=[]
                fund.append(fundcode)
                fund.append(unicode(line[0]))
                fund.append(float(line[1]) if len(line[1].strip())>0 else 1)
                fund.append(float(line[2]) if len(line[2].strip())>0 else fund[2])    
                fund.append(unicode(line[4]))
                fund.append(unicode(line[5]))
                fund.append(unicode(line[6]))
                funds.append(fund)
            self.batchinsert('fundshistory', funds)
        except Exception,error:
            print '基金%s出错:%s'%(fundcode,error)
            print line
        fundcsv.close()
    
    def load_fundlist(self):
        fundcsv=open('./fundlist.csv','rb')
        csvreader=csv.reader(fundcsv)
        funds=[[unicode(l) for l in line] for line in islice(csvreader,1,None)]
        self.insert_update('fundslist', funds)
    
    def loadAllFunds(self):
        fundls=self.s3c.query('select fundcode from fundslist')
        fundls=[f[0] for f in fundls]
        map(self.load_fundhistory,fundls)
                