# coding: utf-8

# 整合天气数据

import pandas as pd

# 读取数据集:每条路段的桥梁、隧道、是否管制、管制原因、管制等级和是否施工、时刻信息
dataset = pd.read_csv('inputdata/dataset_gaunzhi_shigong.csv', encoding='gbk')
print('---------- load dataset_guanzhi_shigong ok -----------------')

# 读取天气数据
weather = pd.read_csv('outputdata/weather.csv')
print('---------- load weather ok ---------------------------------')

# 读取安徽省站点对应的经纬度数据
station_data = pd.read_csv('inputdata/station.csv')
# print('---------- load station ok -------------------')

# 将安微省的天气提取出来
anhui_station = list(set(station_data['station']))
weather = weather[weather['station'].isin(anhui_station)]

# 去掉脏数据
weather = weather[weather['time'] != 20]

weather.index = range(len(weather))
weather['year'] = [str(weather['time'][i])[0:4] for i in range(len(weather))]
weather['month'] = [str(weather['time'][i])[4:6] for i in range(len(weather))]
weather['date'] = [str(weather['time'][i])[6:8] for i in range(len(weather))]
weather['hour'] = [str(weather['time'][i])[8:10] for i in range(len(weather))]

weather['ws'] = [weather['ws'][i][:-3] for i in range(len(weather))]
weather['visinility'] = [weather['visinility'][i][:-1] for i in range(len(weather))]

# 将站点对应的经纬度整合到天气表中
weather['lxname1'] = ['' for i in range(len(weather))]
weather['stake1'] = ['' for i in range(len(weather))]
weather['stake2'] = ['' for i in range(len(weather))]
weather['lxname2'] = ['' for i in range(len(weather))]
weather['stake3'] = ['' for i in range(len(weather))]
weather['stake4'] = ['' for i in range(len(weather))]
for i in range(len(weather)):
    df = station_data[station_data['station'] == weather['station'][i]]
    if len(df) > 0:
        # print(len(df),df['lxname1'][0])
        weather['lxname1'][i] = list(df['lxname1'])[0]
        weather['stake1'][i] = list(df['stake1'])[0]
        weather['stake2'][i] = list(df['stake2'])[0]
        weather['lxname2'][i] = list(df['lxname2'])[0]
        weather['stake3'][i] = list(df['stake3'])[0]
        weather['stake4'][i] = list(df['stake4'])[0]

weather.to_csv('outputdata/weather.csv')

# 只构造20170814至20170829的数据集
dataset = dataset[dataset['date'] >= '2017-08-14']
dataset = dataset[dataset['date'] < '2017-08-29']
dataset.index = range(len(dataset))
print(' ---------------------- dataset.index is ' + str(dataset.index) + '-----------------------')

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
    y = int(l[0])
    m = int(l[1])
    d = int(l[2])
    # print(' --------------------- y, m, d is -----------'+y,m,d)
    road = dataset['roadstake'][i].split('_')
    lxname = road[0]
    start = int(road[1])
    end = int(road[2])
    if d not in set(weather['date']):
        print(' ----------------- weaher set is ------------' + str(test))
        print('----------------- d is --------------------' + str(d))
        continue
    else:
        # 找出该天该时刻的所有道路的天气数据
        weather_data = weather[weather['year'] == y]
        weather_data = weather_data[weather_data['month'] == m]
        weather_data = weather_data[weather_data['date'] == d]
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

dataset.to_csv('outputdata/dataset.csv')




