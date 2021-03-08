import requests as r
from json import loads,dumps
import pandas as pd
from numpy import random, array_split
from multiprocessing.pool import ThreadPool
import time
import re
import os

months=['MONTHS:1.2015','MONTHS:2.2015','MONTHS:3.2015','MONTHS:4.2015','MONTHS:5.2015','MONTHS:6.2015','MONTHS:7.2015','MONTHS:8.2015','MONTHS:9.2015','MONTHS:10.2015','MONTHS:11.2015','MONTHS:12.2015',        'MONTHS:1.2016','MONTHS:2.2016','MONTHS:3.2016','MONTHS:4.2016','MONTHS:5.2016','MONTHS:6.2016','MONTHS:7.2016','MONTHS:8.2016','MONTHS:9.2016','MONTHS:10.2016','MONTHS:11.2016','MONTHS:12.2016',
        'MONTHS:1.2017','MONTHS:2.2017','MONTHS:3.2017','MONTHS:4.2017','MONTHS:5.2017','MONTHS:6.2017','MONTHS:7.2017','MONTHS:8.2017','MONTHS:9.2017','MONTHS:10.2017','MONTHS:11.2017','MONTHS:12.2017',
        'MONTHS:1.2018','MONTHS:2.2018','MONTHS:3.2018','MONTHS:4.2018','MONTHS:5.2018','MONTHS:6.2018','MONTHS:7.2018','MONTHS:8.2018','MONTHS:9.2018','MONTHS:10.2018','MONTHS:11.2018','MONTHS:12.2018',
        'MONTHS:1.2019','MONTHS:2.2019','MONTHS:3.2019','MONTHS:4.2019','MONTHS:5.2019','MONTHS:6.2019','MONTHS:7.2019','MONTHS:8.2019','MONTHS:9.2019','MONTHS:10.2019','MONTHS:11.2019','MONTHS:12.2019',
        'MONTHS:1.2020','MONTHS:2.2020','MONTHS:3.2020','MONTHS:4.2020','MONTHS:5.2020','MONTHS:6.2020','MONTHS:7.2020','MONTHS:8.2020','MONTHS:9.2020','MONTHS:10.2020','MONTHS:11.2020','MONTHS:12.2020',
        'MONTHS:1.2021']


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


def Get_Road_Info():
    raw_data=r.post("http://stat.gibdd.ru/dor/getMainMapData")
    data=loads(loads(raw_data.content)["metabase"])["features"]
    ids=[]
    for i in data:
        ids.append((i["properties"]["id"],i["properties"]["name"]))
    ids=ids[:85]
    mega_roads=[]
    for id_ in ids:
        roads=[]
        raw_data1=r.post("http://stat.gibdd.ru/dor/getRegionDorCombo",json={"reg":id_[0]})
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
    return df

df1=Get_Road_Info()




def do_post(js,f0,t1,t2):
    u=0
    while u==0:
        try:
            post_content=f0.post("http://stat.gibdd.ru/dor/getDorKMData",json=js,headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            u=1
        except:
            time.sleep(10)
            print("Таймаут {},{}".format(t1,t2))
    return post_content





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
                print(tup[1],tup[2],tup[3],x)
    return km_and_months





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
    return df



def do_post_card(js,f0,t1,t2):
    u=0
    while u==0:
        try:
            post_content=f0.post("http://stat.gibdd.ru/dor/getDorKMKardList",json=js,headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            u=1
        except:
            time.sleep(10)
            print("Таймаут {},{}".format(t1,t2))
    return post_content




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
                        st=st+inc
                        x+=len(data)
                        for i in data:
                            road=i["infoDtp"]["dor"]
                            date=i["date"]
                            district=i["District"]
                            reg=part
                            curr_km=tup[3]
                            type_acc=i["DTP_V"]
                            LAT=i["infoDtp"]['COORD_W']
                            LON=i["infoDtp"]['COORD_L']
                            kard_data.append((road,date,district,reg,curr_km,type_acc,LAT,LON))
                    except:
                        j=False
            print(part,x)
            time.sleep(20)
    return kard_data







def Federal_Highways_DTP_Parser():
    df2=make_km_frame()
    os.system("cls")
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
    LATS=[i[6] for i in tuples]
    LONS=[i[7] for i in tuples]
    df_f=pd.DataFrame([roads,dates,districts,regs,kms,types,LATS,LONS]).T
    df_f.columns=["Road","Date","District","Region","KM","Type","LAT","LON"]
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
    df_f["Type"]=df_f["Type"].astype("category")
    df_f["Date"]=pd.to_datetime(df_f["Date"],format="%d.%m.%Y")
    print("Сохраняю")
    df_f.to_excel("Federal_Highways_Accidents.xlsx",index=False)

