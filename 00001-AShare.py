# -*- coding: utf-8 -*-
'''
S_DQ_TRADESTATUS状态：
交易-1
    是否涨跌停：未知-10，未涨跌停-11，涨停-12，跌停-13
停牌-2
N（新股）-3
XD（除息）-4
DR（除权除息）-5
XR（除权）-6
'''

import pandas as pd
import numpy as np

'''
from pandas import to_datetime
from datetime import datetime
from pandas import DataFrame
from pandas import read_csv
from pandas import read_hdf
from pandas import pivot_table
'''
import os
dirBase = os.path.dirname(__file__) # 获取当前文件夹的绝对路径
dirDataOrg=dirBase+'\\A-DataOrg'
dirDataDerived=dirBase+'\\B-DataDerived'
readFile1=dirDataOrg+'\\AShareEODPrices-H10Y_Y.txt.gz'  # 原始交易数据文件(10年前)
readFile2=dirDataOrg+'\\AShareEODPrices-H5Y_Y.txt.gz'  # 原始交易数据文件(5年前)
tradeFile=dirDataDerived+'\\AShareEODPrices.hd5'   # 处理后交易数据文件
FAAFile=dirDataDerived+'\\AShareFAAPrices.hd5'  # 前复权价矩阵文件（Forward_Answer_Authority）
TStatusFile=dirDataDerived+'\\AShareTStatus.hd5'  # 交易状态文件（TradeStatus）
time1=pd.datetime.now()
print('正在读取原文件...')
#df=pd.read_csv(readFile2,sep="|",encoding='utf-8')   # 原文件读取
#df1=pd.read_csv(readFile2,sep="|",usecols=["TRADE_DT","S_INFO_WINDCODE","S_DQ_CLOSE","S_DQ_ADJFACTOR"],encoding='utf-8')   # 原文件读取指定列
df1A=pd.read_csv(readFile1,sep="|",usecols=["TRADE_DT","S_INFO_WINDCODE","S_DQ_CLOSE","S_DQ_ADJFACTOR",'S_DQ_TRADESTATUS'],parse_dates="TRADE_DT",date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),index_col='TRADE_DT',encoding='utf-8')   #原文件读取，并转换日期格式，并指定日期索引
df1B=pd.read_csv(readFile2,sep="|",usecols=["TRADE_DT","S_INFO_WINDCODE","S_DQ_CLOSE","S_DQ_ADJFACTOR",'S_DQ_TRADESTATUS'],parse_dates="TRADE_DT",date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),index_col='TRADE_DT',encoding='utf-8')   #原文件读取，并转换日期格式，并指定日期索引
time2=pd.datetime.now()
print('读取原文件完成，耗时' +str(round((time2-time1).seconds))+'秒。正在合并文件...')
df1=pd.concat([df1A,df1B])
#print('合并文件完成，耗时' +str(round((time2-time1).seconds))+'秒。正在建立日期索引...')
#df1.index=df1["TRADE_DT"] #df1['2010-10-01':'2010-12-30']
#df1=pd.read_csv(readFile2,sep="|",usecols=["TRADE_DT","S_INFO_WINDCODE","S_DQ_CLOSE","S_DQ_ADJFACTOR"],parse_dates="TRADE_DT",index_col="TRADE_DT",encoding='utf-8')   #原文件读取，并转换日期格式，并指定日期索引，未测试通过
time3=pd.datetime.now()
print('合并文件完成，耗时' +str(round((time3-time2).seconds))+'秒。正在计算前复权价格...')
df1["Forward_Answer_Authority"]=(df1["S_DQ_CLOSE"]/df1["S_DQ_ADJFACTOR"]).round(2) #前复权，后复权为 backward answer authority
time4=pd.datetime.now()
print('计算前复权价格完成，耗时' +str(round((time4-time3).seconds))+'秒。正在将交易信息写入文件...')
df1.to_hdf(tradeFile,'tables',mode='w', complib='blosc',complevel=9)
time5=pd.datetime.now()
print('交易信息写入文件完成！耗时' +str(round((time5-time4).seconds))+'秒。正在提取前复权收盘价...')
df2=df1.pivot(columns='S_INFO_WINDCODE',values='Forward_Answer_Authority') 
time6=pd.datetime.now()
print('提取前复权收盘价完成！耗时'+str(round((time6-time5).seconds))+'秒。正在将前复权收盘价写入文件...')
df2.index.name='dateTD' 
df2.to_hdf(FAAFile,'tables',mode='w', complib='blosc',complevel=9)
time7=pd.datetime.now()
print('将前复权收盘价写入文件完成！耗时' +str(round((time7-time6).seconds))+'秒。正在提取交易状态信息...')
df1['TRADESTATUS']=df1['S_DQ_TRADESTATUS'].replace(('交易','停牌','N','XD','DR','XR'),('1','2','3','4','5','6'))
df3=df1.pivot(columns='S_INFO_WINDCODE',values= 'TRADESTATUS')
time8=pd.datetime.now()
print('提取前复权收盘价完成！耗时'+str(round((time8-time7).seconds))+'秒。正在将交易状态写入文件...')
df3.to_hdf(TStatusFile,'tables',mode='w', complib='blosc',complevel=9)
time9=pd.datetime.now()
print('将交易状态写入文件完成！耗时'+str(round((time9-time8).seconds))+'秒！')
print('全部完成！耗时'+str(round((time9-time1).seconds))+'秒！')

