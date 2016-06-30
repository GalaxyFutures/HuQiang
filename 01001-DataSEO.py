# -*- coding: utf-8 -*-
# SEO(seasoned equity offering) 股权再融资
'''
一、发行方式(S_FELLOW_ISSUETYPE)
1  董事会预案 
2  股东大会通过 
3  实施 
4  未通过 
5  证监会批准 
8  国资委批准 
12  停止实施 
20  发审委通过 
21  发审委未通过 
22  股东大会未通过 
二、发行方式类型(s_fellow_issuetype)
439006000:定向 
439010000:公开 
三、定向增发定价方式代码 (pricingmode )
竞价:275001000 
定价:275002000 
'''
import os
import shutil
import gzip
import pandas as pd
import numpy as np

'''
from pandas import to_datetime
from pandas import DataFrame
from pandas import read_csv
from datetime import datetime
from pandas import pivot_table
'''

dirBase = os.path.dirname(__file__) #获取当前文件夹的绝对路径
#readFile='D:\\Jens\\Model\\Excel-TPAM\\201601-Python\\AShareSEO-H10Y_Y.txt.gz'  #python 2
#readFile='D:\\Jens\\Model\\Excel-TPAM\\201601-Python\\201604-定增\\AShareSEO-H10Y_Y.txt.gz'
readFile=dirBase+'\\A-DataOrg\\AShareSEO-H10Y_Y.txt.gz'
unzipFile=readFile[:-3]
SEOFile =dirBase+'\\B-DataDerived\\AShareSEO.hd5' 

# fileNameSplit = unzipFile.split('\\')
# unzipFile = fileNameSplit[len(fileNameSplit)-1]

g = gzip.GzipFile(mode='rb',fileobj=open(readFile,'rb'))
open(unzipFile,'wb').write(g.read())
#lines = open(unzipFile).readlines() #open file, read every row  #python 2
lines = open(unzipFile,encoding= 'utf-8').readlines() #open file, read every row
fp = open(unzipFile,'wb')
for s in lines:
    #fp.write(s.replace("&",",").replace("|\r\n","&").replace("\r\n",""))  #python 2
    fp.write(s.replace("&",",").replace("|\r\n","&").replace("\r\n","").encode('utf-8'))
fp.close()

df1=pd.read_csv(unzipFile,sep="|",usecols=['S_INFO_WINDCODE', 'S_FELLOW_PROGRESS', 'S_FELLOW_ISSUETYPE','S_FELLOW_PRICE', 'S_FELLOW_LISTDATE', 'S_FELLOW_INSTLISTDATE','S_FELLOW_SMTGANNCEDATE', 'S_FELLOW_PASSDATE', 'S_FELLOW_APPROVEDDATE','S_SEO_HOLDERSUBSRATE','ANN_DT', 'PRICINGMODE','S_FELLOW_DATE'],parse_dates=['S_FELLOW_LISTDATE', 'S_FELLOW_INSTLISTDATE','S_FELLOW_SMTGANNCEDATE', 'S_FELLOW_PASSDATE', 'S_FELLOW_APPROVEDDATE','ANN_DT','S_FELLOW_DATE'],date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),encoding='utf-8')   #原文件读取，并转换日期格式
df2=df1.sort_values(by='ANN_DT') # 无法进行索引排序，且最新公告日非唯一，故不用于索引
#df2.index=df2['ANN_DT'] 
#df2.index.name='最新公告日'
#del df2['ANN_DT']
#df1[S_FELLOW_ISSUETYPE].replace('439006000','定向')
df2.columns='股票代码','方案进度','发行方式','发行价','公众股上市日','机构股上市日','股东大会公告日','发审委通过日','证监会通过日','大股东认购比例','最新公告日','定价方式','定增发行日'
#df1.sort_values(by='最新公告日').to_hdf(SEOFile,index=False)
df2.to_hdf(SEOFile,'tables',mode='w', complib='blosc',complevel=9 )
#df=pd.read_hdf(SEOFile,'tables').head()
print("完成！")
