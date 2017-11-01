# coding: utf-8

#输入特征数据，将格式进行处理后，保存发生事故的路段数据进行聚类

import pandas as pd
# LabelEncoder创建标签的整数编码，OneHotEncoder用于创建整数编码值的one hot编码
from sklearn.preprocessing import  LabelEncoder

def loadDataset():
    # 读取数据集:每条路段的桥梁、隧道、是否管制、管制原因、管制等级和是否施工、时刻、天气信息
    dataset = pd.read_csv(r'D:\陈超群\botgo项目数据\dataset_weather.csv', encoding='gbk')
    #删除天气数据为0的数据
    dataset = dataset[dataset['humidity'] != 0]
    return dataset


def processFeature(dataset):
    # 对数据集的特征进行处理
    # 将管制原因与管制等级的字符标签转化为整数编码
    label_encoder = LabelEncoder()
    # 保存label_encoder模型
    from sklearn.externals import joblib

    guanzhi_reason_integer = label_encoder.fit_transform(dataset['guanzhi_reason'])
    joblib.dump(label_encoder, 'label_encoder_guanzhi_reason.m')

    guanzhi_level_integer = label_encoder.fit_transform(dataset['guanzhi_level'])
    joblib.dump(label_encoder, 'label_encoder_guanzhi_level.m')
    dataset['guanzhi_reason'] = guanzhi_reason_integer
    dataset['guanzhi_level'] = guanzhi_level_integer

    #inverse_transform可以将转换好的编码变成原先的数值


    # 改变数据类型
    dataset['bridge'] = dataset['bridge'].astype(str).astype(object)
    dataset['tunnel'] = dataset['tunnel'].astype(str).astype(object)
    dataset['is_guanzhi'] = dataset['is_guanzhi'].astype(str).astype(object)
    dataset['guanzhi_reason'] = dataset['guanzhi_reason'].astype(str).astype(object)
    dataset['guanzhi_level'] = dataset['guanzhi_level'].astype(str).astype(object)
    dataset['is_shigong'] = dataset['is_shigong'].astype(str).astype(object)
    dataset['wd'] = dataset['wd'].astype(str).astype(object)
    # 将完整数据集保存
    dataset.to_csv('F:\dataset.csv')
    # 保存事故发生的数据,用于两步聚类
    dataset[dataset['event_number'] > 0].to_csv('F:\event_dataset.csv', ignore_index=False)
    return dataset
