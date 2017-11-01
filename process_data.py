
# coding: utf-8
import pymysql
import pandas as pd

def process_data():

    conn = pymysql.connect(host='42.159.145.138', port=3306,user='root',passwd='Alldo807',db='ahgs',charset='UTF8')
    cur = conn.cursor()
    #仅选择2017-8-14以及之后的事故数据
    cur.execute("select uuid,btime,lxname,direction,stake1,longitude1,latitude1 from gaosu_event where eventtype=1")
    cluster_rawdata=cur.fetchall()
    cur.close()
    conn.close()


    roadstake_data=pd.read_excel('D:\\roadstake_bayes.xlsx')


    conn = pymysql.connect(host='42.159.145.138', port=3306,user='root',passwd='Alldo807',db='ahgs',charset='UTF8')
    cur = conn.cursor()
    cur.execute("select lxname,stake,offset from bridgedata")
    bridge_rawdata=cur.fetchall()
    cur.close()
    conn.close()



    lxname=[]
    stake=[]
    offset=[]
    for i in range(len(bridge_rawdata)):
        lxname.append(bridge_rawdata[i][0])
        stake.append(bridge_rawdata[i][1])
        offset.append(bridge_rawdata[i][2])
    bridge_rawdata=pd.DataFrame({'lxname':lxname,'stake':stake,'offset':offset})



    conn = pymysql.connect(host='42.159.145.138', port=3306,user='root',passwd='Alldo807',db='ahgs',charset='UTF8')
    cur = conn.cursor()
    cur.execute("select lxname,stake,offset from tunneldata")
    tunnel_rawdata=cur.fetchall()
    cur.close()
    conn.close()


    lxname=[]
    stake=[]
    offset=[]
    for i in range(len(tunnel_rawdata)):
        lxname.append(tunnel_rawdata[i][0])
        stake.append(tunnel_rawdata[i][1])
        offset.append(tunnel_rawdata[i][2])
    tunnel_rawdata=pd.DataFrame({'lxname':lxname,'stake':stake,'offset':offset})




    tunnel=[0 for i in range(len(roadstake_data))]
    bridge=[0 for i in range(len(roadstake_data))]
    for i in range(len(roadstake_data)):
        l=roadstake_data['roadstake'][i].split('_')
        lxname=l[0]
        start=int(l[1])
        end=int(l[2])
        tunnel_data=tunnel_rawdata[tunnel_rawdata['lxname']==lxname]
        tunnel_data.index=range(len(tunnel_data))
        bridge_data=bridge_rawdata[bridge_rawdata['lxname']==lxname]
        bridge_data.index=range(len(bridge_data))
        for stake in tunnel_data['stake']:
            if start<=stake and end>stake:
                tunnel[i]=1
        for stake in bridge_data['stake']:
            if start<=stake and end>stake:
                bridge[i]=1


    roadstake_data['tunnel']=tunnel
    roadstake_data['bridge']=bridge

    uuid=[]
    time=[]
    lxname=[]
    direction=[]
    stake=[]
    longtitude=[]
    latitude=[]
    for i in range(len(cluster_rawdata)):
        uuid.append(cluster_rawdata[i][0])
        time.append(cluster_rawdata[i][1])
        lxname.append(cluster_rawdata[i][2])
        direction.append(cluster_rawdata[i][3])
        stake.append(cluster_rawdata[i][4])
        longtitude.append(float(cluster_rawdata[i][5]))
        latitude.append(float(cluster_rawdata[i][6]))
    cluster_data=pd.DataFrame({'uuid':uuid,'time':time,'lxname':lxname,'direction':direction,'stake':stake,'longititude':longtitude,'latitude':latitude})


    cluster_data['date']=[cluster_data['time'][i].split(' ')[0] for i in range(len(cluster_data))]
    cluster_data['hour']=[cluster_data['time'][i].split(' ')[1][0:2] for i in range(len(cluster_data))]
    cluster_data['month']=[cluster_data['time'][i].split(' ')[0][5:7] for i in range(len(cluster_data))]



    roadstake=[]
    tunnel=[]
    bridge=[]
    date=[]
    hour=[]
    dateset=list(set(cluster_data['date']))
    hourset=list(set(cluster_data['hour']))
    print(len(dateset),len(hourset))
    for i in range(len(roadstake_data['roadstake'])):
        for d in dateset:
            for h in hourset:
                roadstake.append(roadstake_data['roadstake'][i])
                tunnel.append(roadstake_data['tunnel'][i])
                bridge.append(roadstake_data['bridge'][i])
                date.append(d)
                hour.append(h)


    dataset=pd.DataFrame({'roadstake':roadstake,'tunnel':tunnel,'bridge':bridge,'date':date,'hour':hour})


    dataset.to_csv('E:\dataset_tunnel_bridge.csv')

    #在某天某时刻某个路段是否发生了事故
    event_number=[0 for i in range(len(dataset)) ]
    for i in range(len(dataset)):
        l=dataset['roadstake'][i].split('_')
        lxname=l[0]
        start=l[1]
        end=l[2]
        #找到事故数据中那一天那一时刻那一道路上发生的事故
        road_event=cluster_data[cluster_data['date']==dataset['date'][i]]
        road_event=road_event[road_event['hour']==dataset['hour'][i]]
        road_event=road_event[road_event['lxname']==lxname]
        road_event.index=range(len(road_event))
        #该路段是否有事故发生了
        if len(road_event)==0:
            continue
        else:
            for j in range(len(road_event)):
                stake_end=road_event['stake'][j].index('+')
                if start<=road_event['stake'][j][1:stake_end] and end>road_event['stake'][j][1:stake_end]:
                    event_number[i]=event_number[i]+1


    dataset['event_number']=event_number
    dataset.to_csv('E:\dataset_eventnumber.csv')


    conn = pymysql.connect(host='42.159.145.138', port=3306,user='root',passwd='Alldo807',db='ahgs',charset='UTF8')
    cur = conn.cursor()
    cur.execute("select btime,lxname,stake1,stake2,guanzhi_reasonname,guanzhi_levelname from gaosu_event where eventtype=2")
    guanzhi_data=cur.fetchall()
    cur.close()
    conn.close()



    btime=[]
    lxname=[]
    stake1=[]
    stake2=[]
    guanzhi_reasonname=[]
    guanzhi_levelname=[]
    for i in range(len(guanzhi_data)):
        btime.append(guanzhi_data[i][0])
        lxname.append(guanzhi_data[i][1])
        stake1.append(guanzhi_data[i][2])
        stake2.append(guanzhi_data[i][3])
        guanzhi_reasonname.append(guanzhi_data[i][4])
        guanzhi_levelname.append(guanzhi_data[i][5])
    guanzhi_data=pd.DataFrame({'btime':btime,'lxname':lxname,'stake1':stake1,'stake2':stake2,'guanzhi_reasonname':guanzhi_reasonname,'guanzhi_levelname':guanzhi_levelname})




    guanzhi_data['date']=[guanzhi_data['btime'][i].split(' ')[0] for i in range(len(guanzhi_data))]
    guanzhi_data['hour']=[guanzhi_data['btime'][i].split(' ')[1][0:2] for i in range(len(guanzhi_data))]
    guanzhi_data['month']=[guanzhi_data['btime'][i].split(' ')[0][5:7] for i in range(len(guanzhi_data))]


    conn = pymysql.connect(host='42.159.145.138', port=3306,user='root',passwd='Alldo807',db='ahgs',charset='UTF8')
    cur = conn.cursor()
    cur.execute("select btime,lxname,stake1,stake2,shigong_basename from gaosu_event where eventtype=3")
    shigong_data=cur.fetchall()
    cur.close()
    conn.close()


    btime=[]
    lxname=[]
    stake1=[]
    stake2=[]
    shigong_basename=[]
    for i in range(len(shigong_data)):
        btime.append(shigong_data[i][0])
        lxname.append(shigong_data[i][1])
        stake1.append(shigong_data[i][2])
        stake2.append(shigong_data[i][3])
        shigong_basename.append(shigong_data[i][4])
    shigong_data=pd.DataFrame({'btime':btime,'lxname':lxname,'stake1':stake1,'stake2':stake2,'shigong_basename':shigong_basename})



    shigong_data['date']=[shigong_data['btime'][i].split(' ')[0] for i in range(len(shigong_data))]
    shigong_data['hour']=[shigong_data['btime'][i].split(' ')[1][0:2] for i in range(len(shigong_data))]
    shigong_data['month']=[shigong_data['btime'][i].split(' ')[0][5:7] for i in range(len(shigong_data))]




    is_guanzhi=[0 for i in range(len(dataset))]
    guanzhi_reason=[' ' for i in range(len(dataset))]
    guanzhi_level=[' ' for i in range(len(dataset))]
    is_shigong=[0 for i in range(len(dataset))]
    for i in range(len(dataset)):
        l=dataset['roadstake'][i].split('_')
        lxname=l[0]
        start=l[1]
        end=l[2]
        #找到管制数据中那一天那一时刻那一道路上发生的管制
        road_guanzhi=guanzhi_data[guanzhi_data['date']==dataset['date'][i]]
        road_guanzhi=road_guanzhi[road_guanzhi['hour']==dataset['hour'][i]]
        road_guanzhi=road_guanzhi[road_guanzhi['lxname']==lxname]
        road_guanzhi.index=range(len(road_guanzhi))
        #找到施工数据中那一天那一时刻那一道路上发生的施工
        road_shigong=shigong_data[shigong_data['date']==dataset['date'][i]]
        road_shigong=road_shigong[road_shigong['hour']==dataset['hour'][i]]
        road_shigong=road_shigong[road_shigong['lxname']==lxname]
        road_shigong.index=range(len(road_shigong))
        if len(road_guanzhi)==0 and len(road_shigong)==0:
            continue
        elif len(road_guanzhi)>0:
            for j in range(len(road_guanzhi)):
                stake1_end=road_guanzhi['stake1'][j].index('+')
                stake2_end=road_guanzhi['stake2'][j].index('+')
                if start<=road_guanzhi['stake1'][j][1:stake1_end] and end>road_guanzhi['stake2'][j][1:stake2_end]:
                    is_guanzhi[i]=1
                    guanzhi_reason[i]=road_guanzhi['guanzhi_reasonname'][j]
                    guanzhi_level[i]=road_guanzhi['guanzhi_levelname'][j]
        elif len(road_shigong)>0:
            for j in range(len(road_shigong)):
                stake1_end=road_shigong['stake1'][j].index('+')
                stake2_end=road_shigong['stake2'][j].index('+')
                if start<=road_shigong['stake1'][j][1:stake1_end] and end>road_shigong['stake2'][j][1:stake2_end]:
                    is_shigong[i]=1


    dataset['is_guanzhi']=is_guanzhi
    dataset['guanzhi_reason']=guanzhi_reason
    dataset['guanzhi_level']=guanzhi_level
    dataset['is_shigong']=is_shigong

    # 更新一个字段，即该时刻之前，该道路是否发生了事故
    for i in range(len(dataset)):
        # 对于每一行记录，找出这个路段这个时刻之前的所有记录
        df = dataset[dataset['roadstake'] == dataset['roadstake'][i]]
        df = df[df['hour'] == dataset['hour'][i]]
        df = df[df['date'] <= dataset['date'][i]]
        # 是否发生过事故
        length = len(df[df['event_number'] > 0])
        if length == 0:
            continue
        else:
            dataset['event_happened'][i] =dataset['event_happened'][i] + length


    dataset.to_csv('E:\dataset_feature.csv')




