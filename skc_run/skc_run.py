#!/usr/bin/python
#-*-coding : UTF-8 -*-
#import base64
#import hashlib
#import pytz
#import operator
#import json
#import requests
#import pymysql
import pandas as pd
import numpy as np
import datetime
import time
from pandasql import sqldf
from scipy import stats as sts
from scipy.stats import chi2_contingency
from scipy.stats import ttest_ind
#import xlwt
import sys
#import smtplib
import os.path
#读取execel使用(支持07)  
#from openpyxl import Workbook  
#写入excel使用(支持07)  
#from openpyxl import load_workbook 
#from openpyxl.workbook import Workbook
#from openpyxl.writer.excel import ExcelWriter 
#from openpyxl.chart import BarChart, Series, Reference, BarChart3D   
#from openpyxl.styles import Font, colors, Alignment,PatternFill,Border,Side,numbers
#import xlsxwriter as xlw
#from io import BytesIO 
#from urllib.request import urlopen
#import pickle as p 
#from email.mime.multipart import MIMEMultipart
#from email.mime.text import MIMEText
#from email.mime.application import MIMEApplication
#from email import encoders
#from sqlalchemy import create_engine
from skc_run_utils import select_result
from skc_run_utils import get_orderid
from skc_run_utils import orderimportsku
from skc_run_utils import save_to_database
#from sqlalchemy import create_engine
#pymysql.install_as_MySQLdb()


#Data
##读取算法所需的SQL
f = open('./data/skc_run_sql.txt','r')
a = f.read()
skc_run_sql = eval(a)
##价位转化依赖表
c2pr = pd.read_csv('./data/pricenode.csv',sep = ',')
##查询所需数据
skc = select_result(skc_run_sql['sql_skc'])
boxdetail = select_result(skc_run_sql['boxdetail_sql'])
skcchange = select_result(skc_run_sql['skcchange_sql'])
skc_stop = select_result(skc_run_sql['sql_skc_stop'],type = 3)
skcchange['datetime'] = pd.to_datetime(skcchange['datetime'],format="%Y-%m-%d %H:%M:%S")
boxdetail['datetime'] = pd.to_datetime(boxdetail['datetime'],format="%Y-%m-%d %H:%M:%S")

#生成划分界限
##转化价位
cates =  list(set(skc['cate']))
for i in range(len(cates)):
    c2pr_c2 = c2pr[c2pr.cate == cates[i]]
    if(c2pr_c2.empty):
        continue
    skc.price[(skc.cate == cates[i]) & (skc.price <= int(c2pr_c2.price[0:1]))] = 1
    for j in reversed(range(len(c2pr_c2)-1)):
        skc.price[(skc.cate == cates[i]) & (skc.price > int(c2pr_c2.price[j:j+1]))] = j+2
##提取已经验证的SKC池
skc_normal = skc[(skc.sent_cnt >= 30) & (~skc.skc.isin(list(skc_stop.SKCID)))]
skc_cnt = sqldf("select cate,price,sum(sent_cnt) as sent_cnt from skc_normal group by cate,price")
cates =  list(set(skc_normal['cate']))
skc_1 = pd.DataFrame(columns = ( 'cate' ,'price' ,'rank' ,'tj'))
skc_2 = pd.DataFrame(columns = ( 'cate' ,'price' ,'rank' ,'tj'))
##计算界限
for cate  in cates :
    cate_tj_mean = np.mean(skc_normal.tj[skc_normal.cate == cate])
    cate_tj_mean_5sd = np.mean(skc_normal.tj[skc_normal.cate == cate]) - np.std(skc_normal.tj[skc_normal.cate == cate]) * 0.5
    cate_tj_mean_sd = np.mean(skc_normal.tj[skc_normal.cate == cate]) - np.std(skc_normal.tj[skc_normal.cate == cate])
    price_temp = len(c2pr.price[c2pr.cate == cate])
    for  i in range(1 ,price_temp + 1) : 
        skc_q3 = 0 if skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)].empty else np.percentile(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)],75)
        cate_price_tj_mean = 0 if np.isnan(np.mean(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)])) else np.mean(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)])
        cate_price_tj_mean_5sd = 0 if np.isnan(np.mean(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)]) - np.std(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i) ]) * 0.5) else np.mean(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)]) - np.std(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)]) * 0.5
        cate_price_tj_mean_sd = 0 if np.isnan(np.mean(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)]) - np.std(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)])) else np.mean(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)]) - np.std(skc_normal.tj[(skc_normal.cate == cate) & (skc_normal.price == i)])
        if len(skc_normal[(skc_normal.cate == cate) & (skc_normal.price == i)]) >= 4:
            if skc_q3 > cate_tj_mean:
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'S' , 'tj' : skc_q3}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'A' , 'tj' : cate_price_tj_mean}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'B' , 'tj' : cate_price_tj_mean_5sd}
            elif cate_price_tj_mean > cate_tj_mean :
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'S_c_p_mean' , 'tj' : cate_price_tj_mean}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'A_c_p_mean-.5sd' , 'tj' : cate_price_tj_mean_5sd}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'B_c_p_mean_sd' , 'tj' : cate_price_tj_mean_sd}
            else:
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'S_mean' , 'tj' : cate_tj_mean}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'A_c_mean-.5sd' , 'tj' : cate_tj_mean_5sd}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'B_c_mean_sd' , 'tj' : cate_tj_mean_sd}
        elif (len(skc_normal[(skc_normal.cate == cate) & (skc_normal.price == i)]) < 4) & (len(skc_normal[(skc_normal.cate == cate) & (skc_normal.price == i)]) > 1):
            if cate_price_tj_mean > cate_tj_mean:
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'S_c_p_mean' , 'tj' : cate_price_tj_mean}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'A_c_p_mean-.5sd' , 'tj' : cate_price_tj_mean_5sd}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'B_c_p_mean_sd' , 'tj' : cate_price_tj_mean_sd}
            else:
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'S_mean' , 'tj' : cate_tj_mean}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'A_c_mean-.5sd' , 'tj' : cate_tj_mean_5sd}
                skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'B_c_mean_sd' , 'tj' : cate_tj_mean_sd}
        elif len(skc_normal[(skc_normal.cate == cate) & (skc_normal.price == i)]) <= 1:
            skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'S_mean' , 'tj' : cate_tj_mean}
            skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'A_c_mean-.5sd' , 'tj' : cate_tj_mean_5sd}
            skc_1.loc[skc_1.shape[0]+1]  = {'cate' : cate , 'price' : int(i) ,'rank' : 'B_c_mean_sd' , 'tj' : cate_tj_mean_sd}
        if (i in range(3,len(c2pr.price[c2pr.cate == cate])+1)) & (len(c2pr.price[c2pr.cate == cate]) >= 3):
            if((skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-2)].empty != 1)&(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-1)].empty != 1)&(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i)].empty != 1)):
                x =  np.array([[(skc_1.iloc[skc_1.shape[0]-9,3] * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-2)])),(skc_1.iloc[skc_1.shape[0]-6,3] * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-2)]))],[((1-skc_1.iloc[skc_1.shape[0]-9,3]) * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-1)])),((1-skc_1.iloc[skc_1.shape[0]-6,3]) * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-1)]))]])
                re_x = chi2_contingency(x)
                y =  np.array([[(skc_1.iloc[skc_1.shape[0]-6,3] * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-1)])),(skc_1.iloc[skc_1.shape[0]-3,3] * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i-1)]))],[((1-skc_1.iloc[skc_1.shape[0]-9,3]) * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i)])),((1-skc_1.iloc[skc_1.shape[0]-6,3]) * float(skc_cnt.sent_cnt[(skc_cnt.cate == cate)&(skc_cnt.price == i)]))]])
                re_y = chi2_contingency(x)
                if((re_y[1] < 0.05) &(re_x[1] < 0.05 )& ((skc_1.iloc[skc_1.shape[0]-9,3] - skc_1.iloc[skc_1.shape[0]-6,3]) >= 0.03 )& ((skc_1.iloc[skc_1.shape[0]-3,3] - skc_1.iloc[skc_1.shape[0]-6,3]) > 0 )):
                    print(cate)
                    skc_1.iloc[skc_1.shape[0]-6,3] = cate_tj_mean + np.std(skc_normal.tj[skc_normal.cate == cate]) * 0.5
                    skc_1.iloc[skc_1.shape[0]-5,3] = cate_tj_mean
                    skc_1.iloc[skc_1.shape[0]-4,3] = cate_tj_mean - np.std(skc_normal.tj[skc_normal.cate == cate])
skc_1 = skc_1.sort_values(by = ['cate','price'])

#判断SKC对应等级
##已经验证商品
skc_normal['R'] = 'C'
for cate in cates :
    price_temp = len(c2pr.price[c2pr.cate == cate])
    for  i in range(1 ,price_temp + 1) : 
        rank_standard = skc_1[(skc_1.cate == cate) & (skc_1.price == i )]
        if rank_standard.shape[0] == 0:
            continue
        else :
            skc_normal.R[(skc_normal.cate == cate) & (skc_normal.price == i) & (skc_normal.tj >= float(rank_standard.iloc[0,-1])) ]  = rank_standard.iloc[0,-2]
            for j in range(1,rank_standard.shape[0]):
                skc_normal.R[(skc_normal.cate == cate) & (skc_normal.price == i) & (skc_normal.tj >= float(rank_standard.iloc[j,-1]))& (skc_normal.tj < float(rank_standard.iloc[j-1,-1]))] = rank_standard.iloc[j,-2]
##验证不足商品
skc_new = skc[(skc.sent_cnt <= 30) & (skc.sent_cnt >=10) ]
skc_new['R'] = ''
for i in range(skc_new.shape[0]):
    rank_standard_new = skc_1[(skc_1.cate == skc_new.iloc[i,1]) & (skc_1.price == skc_new.iloc[i,2])]
    if rank_standard_new.empty :
        break
    rank_vector = np.concatenate((np.tile([1],int(round(rank_standard_new.iloc[0,3] * int(30)))),np.tile([0],int(round((1-rank_standard_new.iloc[0,3]) * int(30))))))
    skc_vector = np.concatenate((np.tile([1],int(round(skc_new.iloc[i,3] * skc_new.iloc[i,4]))),np.tile([0],int(round((1- skc_new.iloc[i,3]) * skc_new.iloc[i,4])))))
    re = ttest_ind(rank_vector,skc_vector,equal_var=False)
    if (skc_new.iloc[i,3] > rank_standard_new.iloc[0,3]) & (re[1] <= 0.1) :
        skc_new.iloc[i,5] = 'S'

##生成最后S级SKC列表
skc_S = skc_normal[skc_normal.R.str.contains("S")].append(skc_new[skc_new.R.str.contains("S")])

##保存每日的S级在库情况
skc_s_record = list(skc_S.skc)
skcrecord = select_result(skc_run_sql['sql_s_skc_record'],skc_s_record,1)
save_to_database(skcrecord,'analysis',"skcrecord")
#yconnect = create_engine('mysql://analysis:sI5A2BU4p7NobtbG@rm-bp12n2u9o7432w52b.mysql.rds.aliyuncs.com:3306/analysis')
#pd.io.sql.to_sql(skcrecord,'skcrecord', yconnect, schema='analysis', if_exists='append',index = False)

#计算S级SKC与新品的流通速度
newskc = select_result(skc_run_sql['sql_newskc'])
new_skc = list(newskc.skc)
skc_sid = list(skc_S.skc)
boxdetail_skc = list(set(boxdetail.skc))
liquidity_id = list(set(skc_sid).union(new_skc).intersection(set(boxdetail_skc)))
skc_liquidity = pd.DataFrame(columns=("cate_1","price","skc","date","skc_cnt","box_cnt"))
for skc in liquidity_id:
    skcchange_tep = skcchange[skcchange.skc == skc].copy()
    if skcchange_tep.empty:
        continue
    dates = []
    a = []
    for i in range(1,len(skcchange_tep)):
        if skcchange_tep.iloc[i,1] ==  skcchange_tep.iloc[i-1,1]:
            a.append(i)
    skcchange_tep.drop(skcchange_tep.index[a],inplace = True)
    try:
        c = range(0,list(skcchange_tep.type).index(1))
        skcchange_tep.drop(skcchange_tep.index[c],inplace = True)
    except:
        continue
    if len(skcchange_tep)%2 == 1:
        for j in range(0,len(skcchange_tep)-1,2):
            dates.append(pd.to_datetime(skcchange_tep.iloc[j,2]))
            if skcchange_tep.iloc[j+1,2].date() == skcchange_tep.iloc[j,2].date():
                dates.append(pd.to_datetime(skcchange_tep.iloc[j+1,2]))
                continue
            for k in range(1,((boxdetail.datetime[(boxdetail.skc == skc)  &  (boxdetail.datetime <= skcchange_tep.iloc[j+1,2].date()+ datetime.timedelta(days=1))].max().date() + datetime.timedelta(days=1))- skcchange_tep.iloc[j,2].date()).days):
                date_1 = skcchange_tep.iloc[j,2] + datetime.timedelta(days=k)
                date_1 = date_1.date()
                date_1 = pd.to_datetime(date_1)
                dates.append(date_1)
                dates.append(date_1)
            dates.append(pd.to_datetime((boxdetail.datetime[(boxdetail.skc == skc)  &  (boxdetail.datetime <= skcchange_tep.iloc[j+1,2].date()+ datetime.timedelta(days=1))].max())))
        dates.append(pd.to_datetime(skcchange_tep.iloc[-1,2]))
        for  z in range(1,(datetime.datetime.now().date() - skcchange_tep.iloc[-1,2].date()).days):
            date_2 = skcchange_tep.iloc[-1,2] + datetime.timedelta(days=z)
            date_2 = date_2.date()
            date_2 = pd.to_datetime(date_2)
            dates.append(date_2)
            dates.append(date_2)
        dates.append(pd.to_datetime((datetime.datetime.now()).date().strftime('%Y-%m-%d %H:%M:%S'))) 
    elif len(skcchange_tep)%2 == 0:
        for j in range(0,len(skcchange_tep)-1,2):
            dates.append(pd.to_datetime(skcchange_tep.iloc[j,2]))
            if skcchange_tep.iloc[j+1,2].date() == skcchange_tep.iloc[j,2].date():
                dates.append(pd.to_datetime(skcchange_tep.iloc[j+1,2]))
                continue
            for k in range(1,((boxdetail.datetime[(boxdetail.skc == skc)  &  (boxdetail.datetime <= skcchange_tep.iloc[j+1,2].date()+ datetime.timedelta(days=1))].max().date() + datetime.timedelta(days=1))- skcchange_tep.iloc[j,2].date()).days):
                date_1 = skcchange_tep.iloc[j,2] + datetime.timedelta(days=k)
                date_1 = date_1.date()
                date_1 = pd.to_datetime(date_1)
                dates.append(date_1)
                dates.append(date_1)
            dates.append(pd.to_datetime((boxdetail.datetime[(boxdetail.skc == skc)  &  (boxdetail.datetime <= skcchange_tep.iloc[j+1,2].date()+ datetime.timedelta(days=1))].max())))
    for m in range(0,len(dates),2):
        skc_liquidity.loc[skc_liquidity.shape[0]+1] = {"cate_1":int(list(boxdetail.cate_1[boxdetail.skc == skc])[0]) ,"price":float(list(boxdetail.price[boxdetail.skc == skc])[0]) ,"skc" : skc,"date" : dates[m].date().strftime('%Y-%m-%d'),"skc_cnt" : len(boxdetail[(boxdetail.datetime >= dates[m]) & (boxdetail.datetime < dates[m+1]) & (boxdetail.skc == skc)]),"box_cnt" : len(list(set(boxdetail.BoxID[(boxdetail.datetime >= dates[m]) & (boxdetail.datetime < dates[m+1])]))) }
skc_liquidity = sqldf("select skc,cate_1,price,date,sum(skc_cnt) as cnt,sum(box_cnt) as box  from skc_liquidity  group by skc,date")
skc_liquidity['liquidity'] = skc_liquidity.cnt /skc_liquidity.box
skc_liquidity.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
skc_liquidity['date'] = pd.to_datetime(skc_liquidity['date'],format="%Y-%m-%d")
b = []
for i in range(1,len(skc_liquidity)):
    if skc_liquidity.iloc[i,3].weekday() in range(5,7):
        b.append(i)
skc_liquidity.drop(skc_liquidity.index[b],inplace = True)

#提取近期流通的SKC
skc_circulate = list(set(skc_liquidity['skc']))
skc_liquidity_1 = pd.DataFrame(columns = ('cate_1','price','skc' ,'liquidity'))
for skc in skc_circulate :
    skc_liquidity_temp = skc_liquidity.liquidity[skc_liquidity.skc == skc ]
    liquidity = skc_liquidity_temp.iloc[0]
    for i in range(1,skc_liquidity_temp.shape[0]):
        liquidity = liquidity * 0.4 + skc_liquidity_temp.iloc[i] * 0.6
    skc_liquidity_1.loc[skc_liquidity_1.shape[0]+1] = {'cate_1': int((list(skc_liquidity.cate_1[skc_liquidity.skc == skc ])[0])) ,'price':float((list(skc_liquidity.price[skc_liquidity.skc == skc ])[0])) ,'skc' : skc , 'liquidity' : liquidity}
skc_liquidity_1['price_range']= str('')
cates = list(set(skc_liquidity_1['cate_1']))
for i in range(len(cates)):
    c2pr_c2 = c2pr[c2pr.cate == cates[i]]
    if(c2pr_c2.empty):
        continue
    skc_liquidity_1.price_range[(skc_liquidity_1.cate_1 == cates[i]) & (skc_liquidity_1.price <= int(c2pr_c2.price[0:1]))] = '%s%s' %(str('<='),str(int(c2pr_c2.price[0:1])))
    for j in range(len(c2pr_c2)-2):
        skc_liquidity_1.price_range[(skc_liquidity_1.cate_1 == cates[i]) & (skc_liquidity_1.price > int(c2pr_c2.price[j:j+1]))] = '%s%s%s' %(str(int(c2pr_c2.price[j:j+1] + 1)),str("-"),str(int(c2pr_c2.price[j+1:j+2])))
    skc_liquidity_1.price_range[(skc_liquidity_1.cate_1 == cates[i]) & (skc_liquidity_1.price >= int(c2pr_c2.price[-1:]))] = '%s%s' %(str(int(c2pr_c2.price[-1:])),str("+"))
skc_liquidity_1['price'] = skc_liquidity_1['price_range']
del skc_liquidity_1['price_range']
#计算Cate*Price下平均流通速度
cp_liquidity = sqldf("select cate_1,price,avg(liquidity) as avg_liquidity from skc_liquidity_1 group by cate_1,price")
skc_liquidity_1.skc = round(skc_liquidity_1.skc)

#提取流通的S级SKC
sql_s_skc = "select * from skc_liquidity_1 as a where a.skc in (%s) "% ','.join(["'%s'" % item for item in skc_sid])
skc_s_liquidity = sqldf(sql_s_skc)
skc_S_circulate = list(set(skc_s_liquidity.skc))
skc_s_stock = select_result(skc_run_sql['sql_stock'],skc_S_circulate,1)
box_cnt = select_result(skc_run_sql['sql_boxcnt'])
skc_s_liquidity['stock'] = int(0)
skc_s_liquidity['boxcnt'] = int(0)
skc_s_liquidity['day'] = int(0)
for i in range(len(skc_s_stock)):
    skc_s_liquidity.stock[skc_s_liquidity.skc == skc_s_stock.iloc[i,0]] = skc_s_stock.iloc[i,1]
skc_s_liquidity['boxcnt'] = skc_s_liquidity['stock'] / skc_s_liquidity['liquidity']
skc_s_liquidity['day'] = skc_s_liquidity['boxcnt'] / box_cnt.iloc[0,0]
#提取需补S级SKC
skc_s_need = skc_s_liquidity[skc_s_liquidity.day <= 7]
skc_S_need = list(skc_s_need.skc)
for skc in list(skc_s_need.skc[skc_s_need.skc.isin(list(skc_stop.SKCID))]):
    skc_S_need.remove(skc)
skc_s_need[skc_s_need.skc.isin(list(skc_S_need))]
skc_s_need['need'] = round(skc_s_need['liquidity'] * box_cnt.iloc[0,0] * 60)
skc_s_need = skc_s_need[skc_s_need.need >= 10]
skc_s_pic = list(set(skc_s_need['skc']))
skc_s_item = select_result(skc_run_sql['sql_skc_item'],skc_S_need,1)
skc_S_1 = pd.merge(skc_s_item,skc_s_need[['skc','need']], how='inner',on = ['skc'])
for i  in range(len(skc_S_1)):
    skc_S_1.iloc[i,7] = '%.2f%%' %(skc_S_1.iloc[i,7] * 100)
skc_S_1_id = list(set(skc_S_1.skc))
#构建SKC自动下单所需信息
RecommendSizeBottom_1 = select_result(skc_run_sql['RecommendSizeBottom_1'])
RecommendSizeTop_1 = select_result(skc_run_sql['RecommendSizeTop_1'])
RecommendSizeShoes_1 = select_result(skc_run_sql['RecommendSizeShoes_1'])
RecommendSizeSuit_1 = select_result(skc_run_sql['RecommendSizeSuit_1'])
skc_s_need_size = select_result(skc_run_sql['sql_sid_size'],skc_S_1_id,1)
skc_order_cnt = select_result(skc_run_sql['sql_order_cnt'])
x_cate = np.array([1,2,3,5])
np.repeat(x_cate, [4,6,9,5], axis=0)
size_dict  = {"cate" : np.repeat(x_cate, [4,6,9,5], axis=0),
 "size1" : ['S','M','L','XL',29,30,31,32,33,34,40,40.5,41,41.5,42,42.5,43,43.5,44,"S","M","L","XL","XXL"]
}
newusersize = pd.DataFrame.from_dict(size_dict, orient='columns')
newusersize['rate'] = float(0.)
#构建尺码基础表
size_dict_top = {}
for recommend in RecommendSizeTop_1.cate1:
    recommend.replace(' ', '')
    try :
        size_a,rate_a = recommend.split(',')
        size_a = size_a.split('/')
        rate_a = rate_a.split('/')
        rate_a = list(map(lambda x: float(x.strip('%'))/ 100 if not x == '1' else x , rate_a))
        size_tep = dict(zip(size_a,rate_a))
    except :
        continue     
    for size in size_tep.keys():
        if size in size_dict_top.keys():
            size_dict_top[size] = float(size_dict_top[size]) + float(size_tep[size])
        else :
            size_dict_top[size] = size_tep[size]
size_top = list(size_dict_top.keys())
size_top_cnt = list(size_dict_top.values())
size_dict_top_1 = {"cate" : 1,"rate" : size_top_cnt,"size1" : size_top} 
top_size = pd.DataFrame(size_dict_top_1)
top_size.rate = top_size.rate/sum(top_size.rate)
size_dict_Bottom = {}
for recommend in RecommendSizeBottom_1.cate2:
    recommend.replace(' ', '')
    try :
        size_a,rate_a = recommend.split(',')
        size_a = size_a.split('/')
        rate_a = rate_a.split('/')
        rate_a = list(map(lambda x: float(x.strip('%'))/ 100 if not x == '1' else x , rate_a))
        size_tep = dict(zip(size_a,rate_a))
    except :
        continue     
    for size in size_tep.keys():
        if size in size_dict_Bottom.keys():
            size_dict_Bottom[size] = float(size_dict_Bottom[size]) + float(size_tep[size])
        else :
            size_dict_Bottom[size] = size_tep[size]
size_Bottom = list(size_dict_Bottom.keys())
size_Bottom_cnt = list(size_dict_Bottom.values())
size_dict_Bottom_1 = {"cate" : 2,"rate" : size_Bottom_cnt,"size1" : size_Bottom} 
Bottom_size = pd.DataFrame(size_dict_Bottom_1)
Bottom_size.rate = Bottom_size.rate/sum(Bottom_size.rate)

RecommendSizeShoes_1.rate = RecommendSizeShoes_1.rate/RecommendSizeShoes_1.rate.sum()
RecommendSizeSuit_1.rate = RecommendSizeSuit_1.rate/RecommendSizeSuit_1.rate.sum()
newusersize_1 = pd.concat([top_size,Bottom_size,RecommendSizeShoes_1,RecommendSizeSuit_1])
newusersize.size1 = newusersize.size1.astype('str')
for i in range(len(newusersize_1)):
    newusersize.rate[(newusersize.cate == newusersize_1.iloc[i,0]) & (newusersize.size1 == str(newusersize_1.iloc[i,2]))] =  newusersize_1.iloc[i,1]
#S级SKC备份
skc_S_1_copy = skc_S_1.copy()
skc_order_record = pd.DataFrame(columns = ("Brand","Orderid","success"))
#-------------生成订单----------------
for Brand in list(set(skc_S_1.品牌)):
    skc_S_order = pd.DataFrame(columns=  select_result(skc_run_sql['sql_aim_skc'],[4],1).columns.values.tolist())  
    for skc in list(set(skc_S_1.skc[skc_S_1.品牌 == Brand])):
        stock_cnt = skc_s_need_size.cnt[skc_s_need_size.skc == skc].sum()
        need_cnt = skc_S_1.need[skc_S_1.skc == skc]
        total_cnt = stock_cnt + need_cnt
        skc_size_trans = select_result(skc_run_sql['sql_size_trans'],(skc,skc),2)
        skc_aim = select_result(skc_run_sql['sql_aim_skc'],[skc],1)
        cate_rate = newusersize[newusersize.cate == int(skc_S_1.cate[skc_S_1.skc == skc])].copy()
        skc_size_trans['rate'] = float(1.)
        if skc_aim.loc[0,"Cate1"] in (1,2,3,5):
            for i in range(skc_size_trans.shape[0]):
                skc_size_trans.iloc[i,7] =   skc_size_trans.iloc[i,6] / skc_size_trans.sale_rate[skc_size_trans.size_a == skc_size_trans.iloc[i,2]].sum()
                if (np.isnan(skc_size_trans.iloc[i,7])):
                    skc_size_trans.iloc[i,7] = float(cate_rate.rate[cate_rate.size1 == skc_size_trans.iloc[i,2]])
                else : 
                    skc_size_trans.iloc[i,7] = skc_size_trans.iloc[i,7] * float(cate_rate.rate[cate_rate.size1 == skc_size_trans.iloc[i,2]])
            for j in range(0,skc_size_trans.shape[0]):
                skc_aim.loc[j] =  skc_aim.loc[0]
                skc_aim.iloc[j,3] =  skc_size_trans.iloc[j-1,0]
                if skc_order_cnt.order_cnt[skc_order_cnt.SKUID == skc_size_trans.iloc[j-1,0]].empty:
                    skc_aim.iloc[j,7] = int(np.round(float(int(total_cnt) * float(skc_size_trans.iloc[j-1,7]) / float(skc_size_trans.rate.sum())),0))
                else : 
                    skc_aim.iloc[j,7] = int(np.round(float(int(total_cnt) * float(skc_size_trans.iloc[j-1,7]) / float(skc_size_trans.rate.sum())) - int(skc_order_cnt.order_cnt[skc_order_cnt.SKUID == skc_size_trans.iloc[j-1,0]]),0))
            skc_S_order = skc_S_order.append(skc_aim)
        
        else:
            skc_size_trans = select_result(skc_run_sql['sql_size_trans'],(skc,skc),2)
            skc_size_trans['rate'] = float(1.)
            skc_size_trans.rate = skc_size_trans.sale_rate / skc_size_trans.sale_rate.sum()
            for j in range(0,skc_size_trans.shape[0]):
                skc_aim.loc[j] =  skc_aim.loc[0]
                skc_aim.iloc[j,3] =  skc_size_trans.iloc[j-1,0]
                if skc_order_cnt.order_cnt[skc_order_cnt.SKUID == skc_size_trans.iloc[j-1,0]].empty:
                    skc_aim.iloc[j,7] = int(np.round(float(int(total_cnt) * float(skc_size_trans.iloc[j-1,7]))))
                else:
                    skc_aim.iloc[j,7] = int(np.round(float(int(total_cnt) * float(skc_size_trans.iloc[j-1,7]))) - int(skc_order_cnt.order_cnt[skc_order_cnt.SKUID == skc_size_trans.iloc[j-1,0]]),0)
            skc_S_order = skc_S_order.append(skc_aim)
    del skc_S_order['Cate1']
    skc_S_order = skc_S_order[skc_S_order.PurchaseNum > 0].copy()
    skc_S_order = skc_S_order.reset_index(drop = True)
    skulist = skc_S_order.to_dict('records')
    orderid_api_res = get_orderid(key = '@y7M&M@7vk!SUtqb', supplierid = int(10) ,handleby = int(list(set(skc_S_1.负责人[skc_S_1.品牌 == Brand]))[0]))
    orderid = orderid_api_res["data"]["orderID"]
    res_api = orderimportsku(orderid = orderid, key = '@y7M&M@7vk!SUtqb',skulist =skulist)
    skc_order_record.loc[skc_order_record.shape[0]+1] = {"Brand" : Brand,"Orderid" : orderid,"success" : res_api}
skc_order_record.to_excel('./Daily_record/自动下单监控_' + time.strftime("%Y-%m-%d") + '.xlsx',index = False)
