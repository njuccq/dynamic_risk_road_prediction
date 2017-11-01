import pymysql
import pandas as pd
import datetime


def process_feature():
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='ahgs', charset='UTF8')
    print('database conneted ')
    cur = conn.cursor()
    # 读取事故发生数据
    cur.execute(
        "select uuid,btime,lxname,direction,stake1,longitude1,latitude1 from gaosu_event where eventtype=1")
    rawdata = cur.fetchall()
    cur.close()
    conn.close()

    roadstake_data = pd.read_excel(r'inputdata/roadstake_bayes.xlsx')

    # 读取桥梁数据并转换为dataframe格式
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='ahgs', charset='UTF8')
    cur = conn.cursor()
    cur.execute("select lxname,stake,offset from bridgedata")
    bridge_rawdata = cur.fetchall()
    cur.close()
    conn.close()

    lxname = []
    stake = []
    offset = []
    for i in range(len(bridge_rawdata)):
        lxname.append(bridge_rawdata[i][0])
        stake.append(bridge_rawdata[i][1])
        offset.append(bridge_rawdata[i][2])
    bridge_rawdata = pd.DataFrame({'lxname': lxname, 'stake': stake, 'offset': offset})

    # 读取隧道数据并转换为dataframe格式
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='ahgs', charset='UTF8')
    cur = conn.cursor()
    cur.execute("select lxname,stake,offset from tunneldata")
    tunnel_rawdata = cur.fetchall()
    cur.close()
    conn.close()

    lxname = []
    stake = []
    offset = []
    for i in range(len(tunnel_rawdata)):
        lxname.append(tunnel_rawdata[i][0])
        stake.append(tunnel_rawdata[i][1])
        offset.append(tunnel_rawdata[i][2])
    tunnel_rawdata = pd.DataFrame({'lxname': lxname, 'stake': stake, 'offset': offset})

    # 将桥梁与隧道整合到2公里的道路中
    tunnel = [0 for i in range(len(roadstake_data))]
    bridge = [0 for i in range(len(roadstake_data))]
    for i in range(len(roadstake_data)):
        l = roadstake_data['roadstake'][i].split('_')
        lxname = l[0]
        start = int(l[1])
        end = int(l[2])
        tunnel_data = tunnel_rawdata[tunnel_rawdata['lxname'] == lxname]
        tunnel_data.index = range(len(tunnel_data))
        bridge_data = bridge_rawdata[bridge_rawdata['lxname'] == lxname]
        bridge_data.index = range(len(bridge_data))
        for stake in tunnel_data['stake']:
            if start <= stake and end > stake:
                tunnel[i] = 1
        for stake in bridge_data['stake']:
            if start <= stake and end > stake:
                bridge[i] = 1

    roadstake_data['tunnel'] = tunnel
    roadstake_data['bridge'] = bridge

    roadstake_data.to_csv(r'outputdata/roadstake_tunnel_bridge.csv')
    print('--------------------------------save roadstake tunnel bridge data ok --------------------------')
    print("now : " + str(datetime.datetime.now()))

    # 将事故发生数据转换成dataframe格式
    uuid = []
    time = []
    lxname = []
    direction = []
    stake = []
    longtitude = []
    latitude = []
    for i in range(len(rawdata)):
        uuid.append(rawdata[i][0])
        time.append(rawdata[i][1])
        lxname.append(rawdata[i][2])
        direction.append(rawdata[i][3])
        stake.append(rawdata[i][4])
        longtitude.append(float(rawdata[i][5]))
        latitude.append(float(rawdata[i][6]))
    data = pd.DataFrame({'uuid': uuid, 'time': time, 'lxname': lxname, 'direction': direction, 'stake': stake,
                         'longititude': longtitude, 'latitude': latitude})

    # 构造日期、时刻信息、道路、桥梁隧道数据集
    data['date'] = [data['time'][i].split(' ')[0] for i in range(len(data))]
    data['hour'] = [data['time'][i].split(' ')[1][0:2] for i in range(len(data))]
    data['month'] = [data['time'][i].split(' ')[0][5:7] for i in range(len(data))]

    roadstake = []
    tunnel = []
    bridge = []
    date = []
    hour = []
    dateset = list(set(data['date']))
    hourset = list(set(data['hour']))
    print(len(dateset), len(hourset))
    for i in range(len(roadstake_data['roadstake'])):
        for d in dateset:
            for h in hourset:
                roadstake.append(roadstake_data['roadstake'][i])
                tunnel.append(roadstake_data['tunnel'][i])
                bridge.append(roadstake_data['bridge'][i])
                date.append(d)
                hour.append(h)

    dataset = pd.DataFrame({'roadstake': roadstake, 'tunnel': tunnel, 'bridge': bridge, 'date': date, 'hour': hour})

    dataset.to_csv(r'outputdata/dataset_tunnel_bridge_0925.csv')
    print('--------------------------------save date time roadstake tunnel bridge data ok --------------------------')
    print("now : " + str(datetime.datetime.now()))

    # 在某天某时刻某个路段是否发生了事故
    event_number = [0 for i in range(len(dataset))]
    for i in range(len(dataset)):
        l = dataset['roadstake'][i].split('_')
        lxname = l[0]
        start = int(l[1])
        end = int(l[2])
        # 找到事故数据中那一天那一时刻那一道路上发生的事故
        road_event = data[(data['date'] == dataset['date'][i]) & (data['hour'] == dataset['hour'][i]) & (data['lxname']==lxname)]
        #road_event = road_event[road_event['hour'] == dataset['hour'][i]]
        #road_event = road_event[road_event['lxname'] == lxname]
        road_event.index = range(len(road_event))
        # 该路段是否有事故发生了
        if len(road_event) == 0:
            continue
        else:
            for j in range(len(road_event)):
                stake_end = road_event['stake'][j].index('+')
                if start <= int(road_event['stake'][j][1:stake_end]) and end > int(road_event['stake'][j][1:stake_end]):
                    event_number[i] = event_number[i] + 1

    dataset['event_number'] = event_number
    dataset.to_csv(r'D:\陈超群\botgo项目数据\outputdata\dataset_eventnumber_0925.csv')
    print('--------------------------------save event data ok --------------------------')

if __name__ == '__main__':
    process_feature()