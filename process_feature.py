import pymysql
import pandas as pd
import datetime


def process_feature():
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='ahgs', charset='UTF8')
    print('database conneted ')
    cur = conn.cursor()
    # 读取事故发生数据
    # 仅选择2017-8-15以及之后的数据
    cur.execute(
        "select uuid,btime,lxname,direction,stake1,longitude1,latitude1 from gaosu_event where eventtype=1'")
    rawdata = cur.fetchall()
    cur.close()
    conn.close()

    roadstake_data = pd.read_excel(r'D:\陈超群\botgo项目数据\inputdata\roadstake_bayes.xlsx')

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

    roadstake_data.to_csv(r'D:\陈超群\botgo项目数据\outputdata\roadstake_tunnel_bridge.csv')
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

    dataset.to_csv(r'D:\陈超群\botgo项目数据\outputdata\dataset_tunnel_bridge_0815.csv')
    print('--------------------------------save date time roadstake tunnel bridge data ok --------------------------')
    print("now : " + str(datetime.datetime.now()))

    # 在某天某时刻某个路段是否发生了事故
    event_number = [0 for i in range(len(dataset))]
    for i in range(len(dataset)):
        l = dataset['roadstake'][i].split('_')
        lxname = l[0]
        start = l[1]
        end = l[2]
        # 找到事故数据中那一天那一时刻那一道路上发生的事故
        road_event = data[data['date'] == dataset['date'][i]]
        road_event = road_event[road_event['hour'] == dataset['hour'][i]]
        road_event = road_event[road_event['lxname'] == lxname]
        road_event.index = range(len(road_event))
        # 该路段是否有事故发生了
        if len(road_event) == 0:
            continue
        else:
            for j in range(len(road_event)):
                stake_end = road_event['stake'][j].index('+')
                if start <= road_event['stake'][j][1:stake_end] and end > road_event['stake'][j][1:stake_end]:
                    event_number[i] = event_number[i] + 1

    dataset['event_number'] = event_number
    dataset.to_csv(r'D:\陈超群\botgo项目数据\outputdata\dataset_eventnumber_0815.csv')
    print('--------------------------------save event data ok --------------------------')
    print("now : " + str(datetime.datetime.now()))

    # 读取数据库中的道路管制信息并整合成dataframe格式
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='ahgs', charset='UTF8')
    cur = conn.cursor()
    cur.execute(
        "select btime,lxname,stake1,stake2,guanzhi_reasonname,guanzhi_levelname from gaosu_event where eventtype=2 and btime>'2017-08-15'")
    guanzhi_data = cur.fetchall()
    cur.close()
    conn.close()

    btime = []
    lxname = []
    stake1 = []
    stake2 = []
    guanzhi_reasonname = []
    guanzhi_levelname = []
    for i in range(len(guanzhi_data)):
        btime.append(guanzhi_data[i][0])
        lxname.append(guanzhi_data[i][1])
        stake1.append(guanzhi_data[i][2])
        stake2.append(guanzhi_data[i][3])
        guanzhi_reasonname.append(guanzhi_data[i][4])
        guanzhi_levelname.append(guanzhi_data[i][5])
    guanzhi_data = pd.DataFrame(
        {'btime': btime, 'lxname': lxname, 'stake1': stake1, 'stake2': stake2, 'guanzhi_reasonname': guanzhi_reasonname,
         'guanzhi_levelname': guanzhi_levelname})

    guanzhi_data['date'] = [guanzhi_data['btime'][i].split(' ')[0] for i in range(len(guanzhi_data))]
    guanzhi_data['hour'] = [guanzhi_data['btime'][i].split(' ')[1][0:2] for i in range(len(guanzhi_data))]
    guanzhi_data['month'] = [guanzhi_data['btime'][i].split(' ')[0][5:7] for i in range(len(guanzhi_data))]

    # 读取数据库中的道路施工信息并整合成dataframe格式
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='ahgs', charset='UTF8')
    cur = conn.cursor()
    cur.execute(
        "select btime,lxname,stake1,stake2,shigong_basename from gaosu_event where eventtype=3 and btime>'2017-08-15'")
    shigong_data = cur.fetchall()
    cur.close()
    conn.close()

    btime = []
    lxname = []
    stake1 = []
    stake2 = []
    shigong_basename = []
    for i in range(len(shigong_data)):
        btime.append(shigong_data[i][0])
        lxname.append(shigong_data[i][1])
        stake1.append(shigong_data[i][2])
        stake2.append(shigong_data[i][3])
        shigong_basename.append(shigong_data[i][4])
    shigong_data = pd.DataFrame(
        {'btime': btime, 'lxname': lxname, 'stake1': stake1, 'stake2': stake2, 'shigong_basename': shigong_basename})

    shigong_data['date'] = [shigong_data['btime'][i].split(' ')[0] for i in range(len(shigong_data))]
    shigong_data['hour'] = [shigong_data['btime'][i].split(' ')[1][0:2] for i in range(len(shigong_data))]
    shigong_data['month'] = [shigong_data['btime'][i].split(' ')[0][5:7] for i in range(len(shigong_data))]

    # 将道路管制、施工信息整合到dataset（日期、时刻、道路、桥梁、隧道、事故）中
    is_guanzhi = [0 for i in range(len(dataset))]
    guanzhi_reason = [' ' for i in range(len(dataset))]
    guanzhi_level = [' ' for i in range(len(dataset))]
    is_shigong = [0 for i in range(len(dataset))]
    for i in range(len(dataset)):
        l = dataset['roadstake'][i].split('_')
        lxname = l[0]
        start = l[1]
        end = l[2]
        # 找到管制数据中那一天那一时刻那一道路上发生的管制
        road_guanzhi = guanzhi_data[guanzhi_data['date'] == dataset['date'][i]]
        print(str(dataset['date'][i]) + '-----------------length of road_guanzhi---------------------------')
        print(len(road_guanzhi))
        road_guanzhi = road_guanzhi[road_guanzhi['hour'] == dataset['hour'][i]]
        road_guanzhi = road_guanzhi[road_guanzhi['lxname'] == lxname]
        road_guanzhi.index = range(len(road_guanzhi))
        # 找到施工数据中那一天那一时刻那一道路上发生的施工
        road_shigong = shigong_data[shigong_data['date'] == dataset['date'][i]]
        print(str(dataset['date'][i]) + '-----------------length of road_shigong---------------------------')
        print(len(road_shigong))
        road_shigong = road_shigong[road_shigong['hour'] == dataset['hour'][i]]
        road_shigong = road_shigong[road_shigong['lxname'] == lxname]
        road_shigong.index = range(len(road_shigong))
        if len(road_guanzhi) == 0 and len(road_shigong) == 0:
            continue
        elif len(road_guanzhi) > 0 and len(road_shigong) == 0:
            for j in range(len(road_guanzhi)):
                stake1_end = road_guanzhi['stake1'][j].index('+')
                stake2_end = road_guanzhi['stake2'][j].index('+')
                if start <= road_guanzhi['stake1'][j][1:stake1_end] and end > road_guanzhi['stake2'][j][1:stake2_end]:
                    is_guanzhi[i] = 1
                    guanzhi_reason[i] = road_guanzhi['guanzhi_reasonname'][j]
                    guanzhi_level[i] = road_guanzhi['guanzhi_levelname'][j]
                break
        elif len(road_shigong) > 0 and len(road_guanzhi) == 0:
            for j in range(len(road_shigong)):
                stake1_end = road_shigong['stake1'][j].index('+')
                stake2_end = road_shigong['stake2'][j].index('+')
                if start <= road_shigong['stake1'][j][1:stake1_end] and end > road_shigong['stake2'][j][1:stake2_end]:
                    is_shigong[i] = 1
                break
        elif len(road_guanzhi) > 0 and len(road_shigong) > 0:
            for j in range(len(road_guanzhi)):
                stake1_end = road_guanzhi['stake1'][j].index('+')
                stake2_end = road_guanzhi['stake2'][j].index('+')
                if start <= road_guanzhi['stake1'][j][1:stake1_end] and end > road_guanzhi['stake2'][j][1:stake2_end]:
                    is_guanzhi[i] = 1
                    guanzhi_reason[i] = road_guanzhi['guanzhi_reasonname'][j]
                    guanzhi_level[i] = road_guanzhi['guanzhi_levelname'][j]
                break
            for j in range(len(road_shigong)):
                stake1_end = road_shigong['stake1'][j].index('+')
                stake2_end = road_shigong['stake2'][j].index('+')
                if start <= road_shigong['stake1'][j][1:stake1_end] and end > road_shigong['stake2'][j][1:stake2_end]:
                    is_shigong[i] = 1
                break

    dataset['is_guanzhi'] = is_guanzhi
    dataset['guanzhi_reason'] = guanzhi_reason
    dataset['guanzhi_level'] = guanzhi_level
    dataset['is_shigong'] = is_shigong

    dataset.to_csv(r'D:\陈超群\botgo项目数据\outputdata\dataset_guanzhi_shigong_20170815.csv')
    print('----------------------save guanzhi_shigong_data ok-------------------------')

    # 整合天气数据
    # 读取天气数据
    conn = pymysql.connect(host='101.37.29.99', port=3306, user='root', passwd='Alldo807!', db='meteorological',
                           charset='UTF8')
    cur = conn.cursor()
    # 仅选择2017-8-15以及之后的数据
    cur.execute("select * from weather where time>='20170815000000'")
    weather_data = cur.fetchall()
    cur.close()
    conn.close()

    station = []
    time = []
    temp = []
    humidity = []
    visinility = []
    wd = []
    ws = []
    for i in range(len(weather_data)):
        station.append(weather_data[i][0])
        time.append(weather_data[i][1])
        temp.append(weather_data[i][2])
        humidity.append(weather_data[i][3])
        visinility.append(weather_data[i][4])
        wd.append(weather_data[i][5])
        ws.append(weather_data[i][6])
    weather = pd.DataFrame(
        {'station': station, 'time': time, 'temp': temp, 'humidity': humidity, 'visinility': visinility, 'wd': wd,
         'ws': ws})

    # 删除time为999999的数据
    weather = weather[weather['time'] != '999999']
    # 去掉脏数据
    weather = weather[weather['time'] != 20]

    print('---------- read weather data ok ---------------------------------')

    # 读取安徽省站点对应的经纬度数据
    station_data = pd.read_csv(r'D:\陈超群\botgo项目数据\inputdata\anhui_station.csv', encoding='utf-8')
    print('---------- load station ok -------------------')

    # 将安微省的天气提取出来
    anhui_station = list(set(station_data['station']))
    weather = weather[weather['station'].isin(anhui_station)]

    weather.to_csv(r'D:\陈超群\botgo项目数据\outputdata\weather_20170815.csv')

    # 将天气数据整合到dataset中
    dataset['temp'] = [0 for i in range(len(dataset))]
    dataset['humidity'] = [0 for i in range(len(dataset))]
    dataset['visinility'] = [0 for i in range(len(dataset))]
    dataset['wd'] = [0 for i in range(len(dataset))]
    dataset['ws'] = [0 for i in range(len(dataset))]
    print(' -------------------------- set ok --------------------------------------------------')

    # print(' ------------------------ weather date is ----------------'+str(set(weather['date'])))
    # test = str(15) in set(weather['date'])
    # print(' -------------------- d is in set(weather[data])) -------------------'+str(test) )
    test = set(weather['date'])

    print('----------------------------- dataset[date] -----------------' + str(set(dataset['date'])))
    print(' ------------------------------ dataset length -----------' + str(len(dataset)))
    dataset = dataset.sort_values(by='date', ascending=False)
    dataset.index = range(len(dataset))
    for i in range(len(dataset)):
        l = dataset['date'][i].split('-')
        y = l[0]
        m = l[1]
        d = l[2]
        # print(' --------------------- y, m, d is -----------'+y,m,d)
        road = dataset['roadstake'][i].split('_')
        lxname = road[0]
        start = int(road[1])
        end = int(road[2])
        if y not in set(weather['year']):
            continue
        elif m not in set(weather['month']):
            continue
        elif d not in set(weather['date']):
            print(' ----------------- weather date set is ------------' + str(test))
            print('----------------- d is --------------------' + str(d))
            continue
        elif dataset['hour'][i] not in set(weather['hour']):
            print(' ----------------- weather hour set is ------------' + str(set(weather['hour'])))
            print('----------------- hour is --------------------' + str(dataset['hour'][i]))
            continue
        else:
            # 找出该天的所有道路的天气数据
            weather_data = weather[weather['year'] == y]
            weather_data = weather_data[weather_data['month'] == m]
            weather_data = weather_data[weather_data['date'] == d]
            # 找出该时刻的所有道路的天气数据
            weather_data = weather_data[weather_data['hour'] == dataset['hour'][i]]

            df1 = weather_data[weather_data['lxname1'] == lxname]
            df2 = weather_data[weather_data['lxname2'] == lxname]
            df1.index = range(len(df1))
            df2.index = range(len(df2))
            if len(df1) != 0:
                for j in range(len(df1)):
                    if df1['stake1'][j] <= start and df1['stake2'][j] >= end:
                        dataset['temp'][i] = df1['temp'][j]
                        dataset['humidity'][i] = df1['humidity'][j]
                        dataset['visinility'][i] = df1['visinility'][j]
                        dataset['wd'][i] = df1['wd'][j]
                        dataset['ws'][i] = df1['ws'][j]
                        print(len(dataset), i)
                        break
            elif len(df2) != 0:
                for j in range(len(df2)):
                    if df2['stake3'][j] <= start and df2['stake4'][j] >= end:
                        dataset['temp'][i] = df2['temp'][j]
                        dataset['humidity'][i] = df2['humidity'][j]
                        dataset['visinility'][i] = df2['visinility'][j]
                        dataset['wd'][i] = df2['wd'][j]
                        dataset['ws'][i] = df2['ws'][j]
                        print(len(dataset), i)
                        break

    dataset.to_csv('outputdata/dataset_weather_20170815.csv')
    print('-------------------save weatherdata ok---------------------------')
    print('now :' + str(datetime.datetime.now()))

    # 增加一个字段，即该时刻之前，该道路是否发生了事故
    event_happened = []
    for i in range(len(dataset)):
        # 对于每一行记录，找出这个路段这个时刻之前的所有记录
        l = dataset['roadstake'][i].split('_')
        lxname = l[0]
        start = int(l[1])
        end = int(l[2])
        df = data[data['lxname'] == lxname]
        df = df[df['hour'] == str(dataset['hour'][i])]
        df = df[df['date'] < dataset['date'][i]]
        # 是否发生过事故
        length = 0
        if len(df) == 0:
            print(str(i) + "/" + str(len(dataset)))
            print('--------------no event-----------------')
            continue
        else:
            for j in range(len(df)):
                stake_end = df['stake'][j].index('+')
                if start <= int(df['stake'][j][1:stake_end]) and end > int(df['stake'][j][1:stake_end]):
                    length = length + 1
            print(str(i) + '//' + str(len(dataset)))
            print(length)
        event_happened.append(length)

    dataset['event_happened'] = event_happened

    dataset.to_csv(r'D:\陈超群\botgo项目数据\outputdata\dataset_20170815.csv')

if __name__ == '__main__':
    process_feature()
