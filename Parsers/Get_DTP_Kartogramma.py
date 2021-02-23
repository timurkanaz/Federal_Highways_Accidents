#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import geopandas as gpd
import time
import requests as r
import numpy as np
from json import dumps,loads
from numpy import random,array_split
from requests.packages.urllib3.exceptions import InsecureRequestWarning
r.packages.urllib3.disable_warnings(InsecureRequestWarning)
from os import system
from multiprocessing.pool import ThreadPool
desktop_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']





def Get_OKTMOS():
    regions_dict = {"maptype":1,"region":"877","date":"[\"11.2020\"]","pok":"1"}
    k=1
    while k==1:
        try:
            raw_data = r.post("http://stat.gibdd.ru/map/getMainMapData", json=regions_dict)
            k=0
        except:
            time.sleep(10)
    raw_reg_js=loads(raw_data.content)
    raw_reg_js=loads(loads(raw_reg_js["metabase"])[0]["maps"])
    oktmos=[(i["id"],i["name"]) for i in raw_reg_js]
    region=[]
    region_oktmo=[]
    district=[]
    district_oktmo=[]
    for oktmo in oktmos:
        subregions_dict = {"maptype":1,"region":"877","date":"[\"11.2020\"]","pok":"1","region":oktmo[0]}
        raw_subdata=r.post("http://stat.gibdd.ru/map/getMainMapData", json=subregions_dict)
        raw_subreg_js=loads(loads(loads(raw_subdata.content)["metabase"])[0]["maps"])
        for js in raw_subreg_js:
            region.append(oktmo[1])
            region_oktmo.append(oktmo[0])
            district.append(js["name"])
            district_oktmo.append(js["id"])

    df=pd.DataFrame([region,district,region_oktmo,district_oktmo]).T
    df.columns=["Region","District","Region_OKTMO","District_OKTMO"]
    return df

inc=50
df1=Get_OKTMOS()
y_2015=['MONTHS:1.2015','MONTHS:2.2015','MONTHS:3.2015','MONTHS:4.2015','MONTHS:5.2015','MONTHS:6.2015','MONTHS:7.2015','MONTHS:8.2015','MONTHS:9.2015','MONTHS:10.2015','MONTHS:11.2015','MONTHS:12.2015']
y_2016=['MONTHS:1.2016','MONTHS:2.2016','MONTHS:3.2016','MONTHS:4.2016','MONTHS:5.2016','MONTHS:6.2016','MONTHS:7.2016','MONTHS:8.2016','MONTHS:9.2016','MONTHS:10.2016','MONTHS:11.2016','MONTHS:12.2016']
y_2017=['MONTHS:1.2017','MONTHS:2.2017','MONTHS:3.2017','MONTHS:4.2017','MONTHS:5.2017','MONTHS:6.2017','MONTHS:7.2017','MONTHS:8.2017','MONTHS:9.2017','MONTHS:10.2017','MONTHS:11.2017','MONTHS:12.2017']
y_2018=['MONTHS:1.2018','MONTHS:2.2018','MONTHS:3.2018','MONTHS:4.2018','MONTHS:5.2018','MONTHS:6.2018','MONTHS:7.2018','MONTHS:8.2018','MONTHS:9.2018','MONTHS:10.2018','MONTHS:11.2018','MONTHS:12.2018']
y_2019=['MONTHS:1.2019','MONTHS:2.2019','MONTHS:3.2019','MONTHS:4.2019','MONTHS:5.2019','MONTHS:6.2019','MONTHS:7.2019','MONTHS:8.2019','MONTHS:9.2019','MONTHS:10.2019','MONTHS:11.2019','MONTHS:12.2019']
y_2020=['MONTHS:1.2020','MONTHS:2.2020','MONTHS:3.2020','MONTHS:4.2020','MONTHS:5.2020','MONTHS:6.2020','MONTHS:7.2020','MONTHS:8.2020','MONTHS:9.2020','MONTHS:10.2020','MONTHS:11.2020']
ys=[y_2015,y_2016,y_2017,y_2018,y_2019,y_2020]


# In[2]:


def divide_rows():
    regs=list(df1.Region.unique())
    rows_divided=list(array_split(regs,4))
    nums=[1,2,3,4]
    rows_divided=list(zip(nums,rows_divided))
    return rows_divided


# In[3]:


def do_post(js,f,t1,t2):
    u=0
    while u==0:
        try:
            post_content=f.post("http://stat.gibdd.ru/map/getDTPCardData",json=js,headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            u=1
        except:
            time.sleep(10)
            print("Таймаут {},{}".format(t1,t2))
    return post_content


# In[4]:


def get_dtp(rows_part):
    alerts=[]
    content=[]
    num,rows_part=rows_part
    with r.Session() as f:
        for ind,part in enumerate(rows_part):
            indexes=list(df1[df1.Region==part].index)
            for index in indexes:
                x=0
                tup=tuple(df1.iloc[index,:])
                for y_packed in ys:
                        st=1
                        j=True
                        iter_num=0
                        while j:
                            dicts={"data":{"date":y_packed,"ParReg":str(tup[2]),"order":{"type":"1","fieldName":"dat"},"reg":str(tup[3]),"ind":"1","st":str(st),"en":str(st+inc)}}
                            js={}
                            js["data"]=dumps(dicts["data"],separators=(',', ':')).encode("utf8").decode('unicode-escape')
                            raw_data= do_post(js,f,tup[0],tup[1])
                            try:
                                data=loads(loads(raw_data.content)["data"])["tab"]
                                st=st+inc+1
                                x+=len(data)
                                for i in data:
                                    LATS1=i['infoDtp']['COORD_W']
                                    LONS1=i['infoDtp']["COORD_L"]
                                    regions1=tup[0]
                                    districts1=tup[1]
                                    try:
                                        type1=i['infoDtp']['dor_z']
                                    except:
                                        type1=np.nan
                                    content.append((LATS1,LONS1,regions1,districts1,type1))
                                    iter_num+=1
                   
                            
                        
                            except:
                                if (x==0) and (iter_num==0) and (raw_data.content!=b'{"data":""}'):
                                    print(f"Alert {tup[1]}")
                                    alerts.append((tup[0],tup[1],tup[2],tup[3]))
                                    j=False
                                else:
                                    j=False
            
                print("{},{},{}".format(tup[0],tup[1],x))
            print('{} поток обработал {}/{} '.format(num,ind+1,len(rows_part)),flush=True,end="\r")
    return (content,alerts)
    
            


# In[5]:


def run_parsing(tuplex):
    pool=ThreadPool(4)
    l=pool.map(get_dtp,tuplex)
    return l


# In[6]:


def work_with_tuples(l):
    tuples=[]
    al=[]
    for part in l:
        t=part[0]
        a=part[1]
        for part_part in t:
            tuples.append(part_part)
        for a_a in a:
            al.append(a_a)
    print(f"После успешной обработки получено событий:{len(tuples)}")
    print(f"Однако,количество районов при обработке которых возникла ошибка:{len(al)}")
    print("Эти районы")
    for a in al:
        print(a[0],a[1],a[2],a[3])
    LAT=[i[0] for i in tuples]
    LON=[i[1] for i in tuples]
    regions=[i[2] for i in tuples]
    districts=[i[3] for i in tuples]
    road_type=[i[4] for i in tuples]
    acc_df=pd.DataFrame([regions,districts,road_type,LAT,LON]).T
    acc_df.columns=["Region","District","Road_Type","LAT","LON"]
    acc_df.to_excel("accidents.xlsx",index=False)


# In[7]:


def gibdd_stat_parser():
    v=divide_rows()
    l1=run_parsing(v)
    work_with_tuples(l1)
    

