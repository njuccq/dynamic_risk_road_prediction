import pandas as pd
from sklearn.externals import joblib
#LabelEncoder创建标签的整数编码，OneHotEncoder用于创建整数编码值的one hot编码
from sklearn.preprocessing import LabelEncoder


dataset=pd.read_csv(r'inputdata/dataset_detailed.csv',encoding='utf-8')


def processFeature(dataset):
    # 对数据集的特征进行处理
    # 将管制原因与管制等级的字符标签转化为整数编码
    label_encoder = LabelEncoder()
    #guanzhi_reason_integer = label_encoder.fit_transform(dataset['guanzhi_reason'])

    # 保存label_encoder模型
    #joblib.dump(label_encoder, r'outputmodel/label_encoder_guanzhi_reason.m')
    #guanzhi_level_integer = label_encoder.fit_transform(dataset['guanzhi_level'])
    #joblib.dump(label_encoder, r'outputmodel/label_encoder_guanzhi_level.m')
    roadstake_integer = label_encoder.fit_transform(dataset['roadstake'])
    joblib.dump(label_encoder, r'outputmodel/label_encoder_roadstake.m')
    #dataset['guanzhi_reason'] = guanzhi_reason_integer
    #dataset['guanzhi_level'] = guanzhi_level_integer
    dataset['roadstake_id'] = roadstake_integer

    # inverse_transform可以将转换好的编码变成原先的数值


    # 改变数据类型
    dataset['roadstake_id'] = dataset['roadstake_id'].astype(str).astype(object)
    dataset['bridge'] = dataset['bridge'].astype(str).astype(object)
    dataset['tunnel'] = dataset['tunnel'].astype(str).astype(object)
    dataset['is_guanzhi'] = dataset['is_guanzhi'].astype(str).astype(object)
    dataset['guanzhi_reason'] = dataset['guanzhi_reason'].astype(str).astype(object)
    dataset['guanzhi_level'] = dataset['guanzhi_level'].astype(str).astype(object)
    dataset['is_shigong'] = dataset['is_shigong'].astype(str).astype(object)
    dataset['wd'] = dataset['wd'].astype(str).astype(object)
    # 将完整数据集保存
    dataset.to_csv(r'outputdata/dataset_label_encoder.csv')

    # 对类别特征进行向量化处理
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.preprocessing import StandardScaler
    dic_vec = DictVectorizer(sparse=False)
    dates = dataset['date']
    roadstakes = dataset['roadstake']
    dataset = dataset.drop(['date', 'roadstake'], axis=1)
    dataset = dic_vec.fit_transform(dataset.to_dict(orient='record'))
    # 保存对特征进行向量化处理的模型
    joblib.dump(dic_vec, r"outputmodel/dic_vec.m")

    # 对于天气数据与时刻、该路段发生过的事故数等数值型特征需要进行标准化处理
    ss = StandardScaler()
    dataset = ss.fit_transform(dataset)
    # 保存对特征进行标准化处理的模型
    joblib.dump(ss, r"outputmodel/ss.m")
    dataset['roadstake'] = roadstakes
    dataset['date'] = dates
    return dataset

dataset = processFeature(dataset)

dataset=dataset.drop(['Unnamed: 0'],axis=1)

#初始化所有的样本为label=-1
dataset['label']=-1
#读取安全路段的名称与时刻
safe_road=pd.read_csv('inputdata/safe_road.csv')

#对于该时刻从未发生事故（截止到9月16日）的路段，将这些样本标记为0
for i in range(len(safe_road)):
    indexs=dataset[(dataset['roadstake']==safe_road['roadstake'][i]) & (dataset['hour']==safe_road['hour'][i])].index
    dataset.loc[indexs,'label']=0


#对于该时刻发生过2次以上事故的路段，将其发生过事故的路段以及对应时刻的样本标记为1
risk_road=pd.read_csv(r'inputdata/risk_road.csv')
for i in range(len(risk_road)):
    indexs=dataset[(dataset['roadstake']==risk_road['roadstake'][i]) & (dataset['hour']==risk_road['hour'][i]) &
                  (dataset['event_number']>0)].index
    dataset.loc[indexs,'label']=1


dataset.to_csv('outputdata/dataset_labe_20170930.csv',index=False)

