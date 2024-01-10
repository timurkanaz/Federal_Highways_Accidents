#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests as r
from json import loads,dumps
import pandas as pd
from numpy import random, array_split
from multiprocessing.pool import ThreadPool
import time
import re
import os

months=['MONTHS:12.2015','MONTHS:12.2016','MONTHS:12.2017','MONTHS:12.2018','MONTHS:12.2019','MONTHS:12.2020','MONTHS:12.2021','MONTHS:12.2022','MONTHS:11.2023']



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
inc=50


# In[2]:


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
    df.to_excel("OKTMOS.xlsx",index=False)
Get_OKTMOS()


# In[3]:


def Get_Road_Info():
    raw_data=r.post("http://stat.gibdd.ru/road/getMainMapData")
    data=loads(loads(raw_data.content)["metabase"])["features"]
    ids=[]
    for i in data:
        ids.append((i["properties"]["id"],i["properties"]["name"]))
    ids=ids[:85]
    mega_roads=[]
    for id_ in ids:
        roads=[]
        raw_data1=r.post("http://stat.gibdd.ru/road/getRegionDorCombo",json={"reg":id_[0]})
        try:
            raw_data1=loads(raw_data1.content)
            for rd in raw_data1:
                if ("ИСКЛЮЧЕНА" in rd["text"]) or ("исключена" in rd["text"]):
                    pass
                else:
                    roads.append((rd["value"],rd["text"]))
        except:
            roads.append(("0","0"))
        mega_roads.append(roads)
    mega_tuples=[]
    for ind,id_ in enumerate(ids):
        if (mega_roads[ind]==[("0","0")]) or (mega_roads[ind]==[]):
            pass
        else:
            mega_tuples.append([id_,mega_roads[ind]])
    reg_numbers=[]
    reg_names=[]
    road_numbers=[]
    road_names=[]
    for tup in mega_tuples:
        reg_info,roads=tup
        for road in roads:
            reg_numbers.append(reg_info[0])
            reg_names.append(reg_info[1])
            road_value,road_text=road
            road_numbers.append(road_value)
            road_names.append(road_text)
    df=pd.DataFrame([reg_numbers,reg_names,road_numbers,road_names]).T
    df.columns=["Region_Number","Region","Road_Number","Road"]
    df.to_excel("Roads.xlsx",index=False)
    return df
df1=Get_Road_Info()


# In[4]:


def do_post(js,f0,t1,t2):
    u=0
    while u==0:
        try:
            post_content=f0.post("http://stat.gibdd.ru/road/getDorKMData",json=js,headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            u=1
        except:
            time.sleep(10)
            print("Таймаут {},{}".format(t1,t2))
    return post_content


# In[5]:


def get_km_months(rows_part):
    num,rows_part=rows_part
    km_and_months=[]
    with r.Session() as f:
        for ind,part in enumerate(rows_part):
            indexes=list(df1[df1.Region==part].index)
            for index in indexes:
                x=0
                tup=tuple(df1.iloc[index,:])
                for month in months:
                    dicts={"dor":str(tup[2]),"reg":str(tup[0]),"date":month}
                    raw_data=do_post(dicts,f,tup[1],tup[3])
                    raw_data=raw_data.json()["data"]
                    x+=len(raw_data)
                    for d in raw_data:
                        km_and_months.append((tup[3],tup[1],tup[2],d["name"],month))

    return km_and_months


# In[6]:


def make_km_frame():
    regs=list(df1.Region.unique())
    rows_divided=list(array_split(regs,4))
    nums=[1,2,3,4]
    rows_divided=list(zip(nums,rows_divided))
    pool=ThreadPool(4)
    l=pool.map(get_km_months,rows_divided)
    res=[]
    for l_l in l:
        for l_l_l in l_l:
            res.append(l_l_l)
    names=[i[0] for i in res]
    regs=[i[1] for i in res]
    road_nums=[i[2] for i in res]
    km_nums=[i[3] for i in res]
    months=[i[4] for i in res]
    df=pd.DataFrame([names,regs,road_nums,km_nums,months]).T
    df.columns=["Name","Region","Road_Num","KM","Month"]
    df.to_excel("Stats_by_region_roads.xlsx",index=False)
    return df


# In[7]:


def do_post_card(js,f0,t1,t2):
    u=0
    while u==0:
        try:
            post_content=f0.post("http://stat.gibdd.ru/road/getDorKMKardList",json=js,headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            u=1
        except:
            time.sleep(10)
            print("Таймаут {},{}".format(t1,t2))
    return post_content


# In[8]:


def get_dtp_cards(rows_divided):
    num,rows_part,df2=rows_divided
    kard_data=[]
    with r.Session() as fk:
        for ind,part in enumerate(rows_part):
            indexes=list(df2[df2.Region==part].index)
            x=0
            for index in indexes:
                tup=tuple(df2.iloc[index,:])
                st=1
                j=True
                while j:
                    dicts={"data":{"date":[tup[-1]],"order":{"type":"1","fieldName":"dat"},"dor":str(tup[2]),"km":str(tup[3]),"st":str(st),"en":str(st+inc)}}
                    js={}
                    js["data"]=dumps(dicts["data"],separators=(',', ':')).encode("utf8").decode('unicode-escape')
                    raw_data= do_post_card(js,fk,tup[1],tup[0])
                    try:
                        data=loads(loads(raw_data.content)["data"])["tab"]
                        st=st+inc+1
                        x+=len(data)
                        for i in data:
                            road=i["infoDtp"]["dor"]
                            date=i["date"]
                            district=i["District"]
                            reg=part
                            curr_km=tup[3]
                            type_acc=i["DTP_V"]
                            timing=i['Time']
                            identif=i['KartId']
                            deaths=i['POG']
                            wounded=i['RAN']
                            partic=i['K_UCH']
                            nofcars=i['K_TS']
                            street_type=i['infoDtp']['k_ul']
                            factor=" "
                            for f in i['infoDtp']['factor']:
                                factor+=f+" "
                            weather=" "
                            for f in i['infoDtp']['s_pog']:
                                weather+=f+" "
                            spch=i['infoDtp']['s_pch']
                            brightn=i['infoDtp']['osv']
                            change_motion=i['infoDtp']['change_org_motion']
                            nedostatk=" "
                            for f in i['infoDtp']['ndu']:
                                nedostatk+=f+" "
                            UDS_NA_DTP=" "
                            for f in i['infoDtp']['sdor']:
                                UDS_NA_DTP+=f+" "
                        
                            UDS_NEAR_DTP=" "
                            for f in i['infoDtp']['OBJ_DTP']:
                                UDS_NEAR_DTP+=f+" "
                            kard_data.append((road,date,district,reg,curr_km,type_acc,timing,identif,deaths,wounded,partic,nofcars,street_type,factor,
                                              weather,spch,brightn,change_motion,nedostatk,UDS_NA_DTP,UDS_NEAR_DTP))
                    except:
                        j=False
            print(part,x)
            time.sleep(20)
    return kard_data


# In[10]:


def Federal_Highways_DTP_Parser():
    df2=make_km_frame()
    regs=list(df2.Region.unique())
    rows_divided=list(array_split(regs,4))
    nums=[1,2,3,4]
    rd=list(zip(nums,rows_divided))
    rd_n=[]
    for r1 in rd:
        rd_n.append((r1[0],r1[1],df2))
    pool=ThreadPool(4)
    l=pool.map(get_dtp_cards,rd_n)
    tuples=[]
    for l_l in l:
        for l_l_l in l_l:
            tuples.append(l_l_l)
    print("Парсинг закончен.Работаю с данными")
    roads=[i[0] for i in tuples]
    dates=[i[1] for i in tuples]
    districts=[i[2] for i in tuples]
    regs=[i[3] for i in tuples]
    kms=[i[4] for i in tuples]
    types=[i[5] for i in tuples]
    timings=[i[6] for i in tuples]
    ids=[i[7] for i in tuples]
    deaths=[i[8] for i in tuples]
    woundeds=[i[9] for i in tuples]
    participants=[i[10] for i in tuples]
    nofcars=[i[11] for i in tuples]
    street_types=[i[12] for i in tuples]
    factors=[i[13] for i in tuples]
    weathers=[i[14] for i in tuples]
    spchs=[i[15] for i in tuples]
    brightns=[i[16] for i in tuples]
    change_motion=[i[17] for i in tuples]
    nedostatk=[i[18] for i in tuples]
    uds_na_dtp=[i[19] for i in tuples]
    uds_near_dtp=[i[20] for i in tuples]
    df_f=pd.DataFrame([ids,roads,dates,timings,districts,regs,kms,street_types,types,nofcars,participants,deaths,woundeds,factors,
                      weathers,spchs,brightns,change_motion,nedostatk,uds_na_dtp,uds_near_dtp]).T
    df_f.columns=['Accident ID',"Road","Date","Time","District","Region","KM","Street type","Accident type","#,cars","#,participants","#,deaths","#,wounded","Accident factor"
                 ,"Weather","Road condition","Lighting","Changes in dr.mode","Disadv.of road network","UDS on place","UDS near place"]
    df_f["Road_Abbr"]=df_f["Road"].map(lambda x:re.findall(r"[АМР]-\d{1,4}\s?",x))
    df_f["Road_Abbr"]=[i[0] if len(i)>0 else "" for i in df_f.Road_Abbr]
    df_f.loc[df_f.Road=="Крым Москва - Тула - Орел - Курск - Белгород - граница с Украиной, подъезд к заповеднику Прохоровское поле","Road_Abbr"]="М-2"
    df_f.loc[df_f.Road=="Самара – Пугачёв – Энгельс - Волгоград","Road_Abbr"]="Р-226"
    df_f.loc[df_f.Road=="Чекшино - Тотьма - Котлас - Куратово","Road_Abbr"]="А-123"
    df_f.loc[df_f.Road=="Обход г. Нижнего Новгорода","Road_Abbr"]="М-7"
    df_f.loc[df_f.Road=="Кочубей-Зеленокумск через Нефтекумск","Road_Abbr"]="А-167"
    df_f.loc[df_f.Road=="Объезд г. Бийска","Road_Abbr"]="Р-256"
    df_f.loc[df_f.Road=="Барнаул - Павловск - граница с Республикой Казахстан","Road_Abbr"]="А-321"
    df_f.loc[df_f.Road=="1Р 418  Иркутск - Усть-Ордынский","Road_Abbr"]="Р-418"
    df_f.loc[df_f.Road=="Косолаповы – Новоурожайная – Наймушины (объезд г. Котельнича в составе а/д «Вятка»)","Road_Abbr"]="Р-176"
    df_f.loc[df_f.Road.isin(["Внешняя сторона","Внутренняя сторона"]),"Road_Abbr"]="МКАД"
    df_f.loc[df_f.Road=="«Таврида» Керчь – Симферополь - Севастополь","Road_Abbr"]="А-291"
    df_f.loc[df_f.Road=="Южно-Сахалинск - Оха","Road_Abbr"]="А-393"
    df_f.Road_Abbr=df_f.Road_Abbr.str.rstrip().str.lstrip()
    df_f["KM"]=df_f["KM"].astype("category")
    df_f["Accident type"]=df_f["Accident type"].astype("category")
    df_f["Date"]=pd.to_datetime(df_f["Date"],format="%d.%m.%Y")
    print("Сохраняю")
    df_f.to_excel("Federal_Highways_Accidents.xlsx",index=False)


# In[11]:


Federal_Highways_DTP_Parser()

