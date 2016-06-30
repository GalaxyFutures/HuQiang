
# -*- coding: utf-8 -*-
# SEO(seasoned equity offering) 股权再融资
import pandas as pd
import numpy as np
from pandas import to_datetime
from pandas import DataFrame
from pandas import read_csv
from datetime import datetime
from pandas import pivot_table
import os
dirBase = os.path.dirname(__file__) # 获取当前文件夹的绝对路径
dirDataOrg=dirBase+'\\A-DataOrg'
dirDataDerived=dirBase+'\\B-DataDerived'
tradeFile=dirDataDerived+'\\AShareEODPrices.hd5'   # 处理后交易数据文件
FAAFile=dirDataDerived+'\\AShareFAAPrices.hd5'  # 前复权价矩阵文件（Forward_Answer_Authority）
TStatusFile=dirDataDerived+'\\AShareTStatus.hd5'  # 交易状态文件（TradeStatus）
SEOFile =dirBase+'\\B-DataDerived\\AShareSEO.hd5'  #定增文件
signalFile=dirBase+'\\B-DataDerived\\交易信号.xlsx'  #交易信号文件

#dfSEO1=pd.read_csv(SEOFile,usecols=['股票代码','方案进度','发行方式','发行价','机构股上市日','最新公告日'],parse_dates=['机构股上市日','最新公告日'],date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),encoding='gbk') #读取定增数据
#dfSEO1=pd.read_csv(SEOFile,usecols=['S_INFO_WINDCODE','S_FELLOW_PROGRESS','S_FELLOW_ISSUETYPE','S_FELLOW_PRICE','S_FELLOW_INSTLISTDATE','ANN_DT'],parse_dates=['S_FELLOW_INSTLISTDATE','ANN_DT'],date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),encoding='utf-8') #读取定增数据：股票代码','方案进度','发行方式','发行价','机构股上市日','最新公告日'
dfSEO1=pd.read_hdf(SEOFile) #读取定增数据
dfSEO2=dfSEO1[dfSEO1.方案进度==3].copy() 
dfSEO3=dfSEO1[dfSEO1.方案进度==12].copy() 

#dfFAA=pd.read_csv(FAAFile,parse_dates=['dateTD'],date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),encoding='gbk') #读取前复权收盘价
dfFAA=pd.read_hdf(FAAFile) #读取前复权收盘价
#dfTS=pd.read_csv(TStatusFile,encoding='gbk')  #读取交易状态
dfTS=pd.read_hdf(TStatusFile)  #读取交易状态
#dfTrade=pd.read_csv(tradeFile,parse_dates=['TRADE_DT'],date_parser = lambda x : pd.to_datetime(x, format="%Y%m%d"),encoding='gbk') #读取累计复权因子
dfTrade=pd.read_hdf(tradeFile)  #读取累计复权因子
dfTrade2=dfTrade.pivot(columns='S_INFO_WINDCODE',values='S_DQ_ADJFACTOR')

del dfSEO2['方案进度']
del dfSEO2['发行方式']
dfSEO2['信号日期']=0
dfSEO2['前复权发行价']=0
for i in dfSEO2.index: 
    SEOCode=dfSEO2.at[i,'股票代码']
    fellowPrice=dfSEO2.at[i,'发行价'] 
    orgListdate=dfSEO2.at[i,'机构股上市日'] 
    monitorDate=orgListdate- pd.tseries.offsets.DateOffset(months=3)
    AnnDate=dfSEO2.at[i,'最新公告日'] 
    FAA=dfFAA[SEOCode] #前复权价     
    count=0   
    for i1 in dfFAA.index: #用于定位公告日至交易日期（非交易日则后推）
        if count==0:
            temp=i1
            count=count+1
        else:  
            if  AnnDate==i1:
                break
            else:
                if AnnDate>temp and AnnDate<i1:
                    AnnDate=i1
                    break
#print('i:'+i.astype(str))
    #if i>=1042:
      #  print(i)
       # break

    ADJFACTOR=dfTrade2.at[AnnDate,SEOCode]
    
    dfSEO2.at[i,'前复权发行价']=float(fellowPrice)/ADJFACTOR
    FAA2=FAA[monitorDate:orgListdate] #前复权价切片
    for i2 in FAA2.index:
          #print(i2)
        if FAA2[i2]<dfSEO2.at[i,'前复权发行价'] : #破发
            dfSEO2.at[i,'信号日期']=to_datetime(i2,format='%Y/%m/%d')
dfSEO3=dfSEO2[dfSEO2.信号日期!=0] #.copy()             
dfSEO3.to_excel(signalFile, sheet_name='信号')
print("完成！")

   #SEOOrgListDate=dfFAA[dfSEO1.iat[c,4]] #机构股上市日期
   #SEOOrgListDate2=dfFAA[dfSEO1.iat[c,4]] #机构股上市日期
   # for r in  c
  #print(c)
   # print(dfSEO1[r])
    #dfFAA2=dfSEO2[dfSEO2.股票代码=='000778.SZ']
      #  dfFAA2=dfFAA['000778.SZ']
    

