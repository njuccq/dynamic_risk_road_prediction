# coding: utf-8
# 暂时认为第三类的是因为高危路段造成的
import pandas as pd
# LabelEncoder创建标签的整数编码，OneHotEncoder用于创建整数编码值的one hot编码
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.externals import joblib

# 20170906：为了后续的分析，可以将roadstake作为索引


# 将事故数据在SPSS Modeler在进行两步聚类法
# 根据聚类后的结果给路段打上标签label,取值为0和1
def loadset():
    # 根据聚类后的结果给路段打上标签label,取值为0和1
    # 因为暂时没有聚类，因此先将发生事故的路段标记为1
    dataset = pd.read_csv('inputdata/dataset.csv')
    # 读取聚类结果集
    cluster_result = pd.read_csv('inputdata/cluster_result_3.csv', encoding='gbk')
    # 经过数据探索后，认为聚类3是高危路段
    rist_roadstake = cluster_result[cluster_result['$T-两步'] == '聚类-3']
    print('高危路段的路段数：' + str(len(rist_roadstake)))
    # 给所有的路段打上标签
    dataset['label'] = [0 for i in range(len(dataset))]

    print('--------------------dataset label 初始化---------------------' + str(len(dataset)))
    for i in range(len(dataset)):
        df = rist_roadstake[rist_roadstake['date'] == dataset['date'][i]]
        print('------------------------ rist_roadstake --------------------------------')
        print(str(df))
        print(df['date'] + '的高危路段数为' + str(len(df)))
        if len(df) != 0:
            df = df[df['hour'] == dataset['hour'][i]]
            print('------------------------- df[hour] == dataset[hour][i] -----------------')
            print(str(df))
            if len(df) != 0:
                for road in set(df['roadstake']):
                    if dataset['roadstake'][i] == road:
                        dataset.loc[i,'label'] = 1
                        print('-------------i/len(dataset)-----------------' + str(i) + '/' + str(len(dataset)))
                        break
            else:
                print('第' + str(i) + '条记录label为0')
                continue

        else:
            print('第' + str(i) + '条记录label为0')
            continue

    # 将不需要的字段去掉
    dataset = dataset.drop(['date', 'roadstake', 'event_number'], axis=1)
    dataset.to_csv('outputdata/dataset_label.csv',encoding='utf-8')
    return dataset

#neg_pos_scale是整数，表示负样本是正样本的多少倍,等于0时表示不对负样本进行采样
#train_test_scale是[0，1],表示训练集与测试集的比例
def split_train_test(dataset,neg_pos_scale,train_test_scale):
    # 处理正负样本不均衡
    # 正样本的数量
    pn = len(dataset[dataset['label'] == 1])
    # 负样本的数量
    nn = len(dataset[dataset['label'] == 0])
    # 在负样本中找出正样本数量的样本
    import random
    if neg_pos_scale!=0:
        neg_index = random.sample(list(dataset[dataset['label'] == 0].index), neg_pos_scale*pn)
        neg = dataset[dataset.index.isin(neg_index)]
    else:
        neg = dataset[dataset['label'] == 0]
    pos = dataset[dataset['label'] == 1]
    print('正样本与负样本的比例：'+str(len(neg))+":"+str(len(pos)))
    threshold=float(len(pos)/len(neg))
    print('标签的阈值：'+str(threshold))
    # 将正负样本划分为测试集与训练集
    neg.index = range(len(neg))
    pos.index = range(len(pos))
    #打乱neg数据集的索引序列
    lneg=list(neg.index)
    random.shuffle(lneg)
    neg_train_index=random.sample(lneg,int(train_test_scale*len(neg)))
    neg_train=neg[neg.index.isin(neg_train_index)]
    #neg_train = neg.sample(frac=train_test_scale)
    neg_test_index = neg.index.drop(neg_train.index)
    neg_test = neg[neg.index.isin(neg_test_index)]

    lpos=list(pos.index)
    random.shuffle(lpos)
    pos_train_index=random.sample(lpos,int(train_test_scale*len(pos)))
    pos_train=pos[pos.index.isin(pos_train_index)]
    pos_test_index = pos.index.drop(pos_train.index)
    pos_test = pos[pos.index.isin(pos_test_index)]


    # 特征向量
    #print(pos_train.columns)
    #feature_selected=list(pos_train.columns).remove('label')
    feature_selected=[col for col in pos_train.columns if col !='label']
    #print(feature_selected)
    train = pd.concat([neg_train, pos_train])
    test = pd.concat([neg_test, pos_test])
    x_train = train[feature_selected]
    y_train = train['label']
    x_test = test[feature_selected]
    y_test = test['label']
    print(x_train,x_test)

    # 对类别特征进行向量化处理
    from sklearn.feature_extraction import DictVectorizer
    dic_vec = DictVectorizer(sparse=False)
    x_train = dic_vec.fit_transform(x_train.to_dict(orient='record'))
    x_test = dic_vec.transform(x_test.to_dict(orient='record'))
    # 保存对特征进行向量化处理的模型
    joblib.dump(dic_vec, "dic_vec.m")

    # 对于天气数据与时刻、该路段发生过的事故数数值型特征需要进行标准化处理
    ss = StandardScaler()
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)
    joblib.dump(ss, "ss.m")

    from sklearn.model_selection import train_test_split
    # 使用GBDT和逻辑回归模型
    # 对训练集进行再次切分。一部分用于GBDT训练组合特征，另一部分用于逻辑回归进行分类器训练
    X_train, X_train_lr, Y_train, Y_train_lr = train_test_split(x_train, y_train, test_size=0.5)
    return  threshold,X_train, X_train_lr, Y_train, Y_train_lr,x_test,y_test


def train(x_train,y_train,x_train_lr,y_train_lr,solver,class_weigeht,n_estimator):
    enc = OneHotEncoder()
    grd = GradientBoostingClassifier(n_estimators=n_estimator)
    grd_enc = OneHotEncoder()
    # 初始化两个逻辑回归模型
    # class_weight参数用于标示分类模型中各种类型的权重，可以不输入，即不考虑权重，或者说所有类型的权重一样。
    # 如果选择输入的话，可以选择balanced让类库自己计算类型权重，或者我们自己输入各个类型的权重，比如对于0,1的二元模型，我们可以定义class_weight={0:0.9, 1:0.1}，这样类型0的权重为90%，而类型1的权重为10%。
    # 如果class_weight选择balanced，那么类库会根据训练样本量来计算权重。某种类型样本量越多，则权重越低，样本量越少，则权重越高。
    grd_lm = LogisticRegression(solver=solver, class_weight=class_weigeht)

    # 训练GBDT模型
    grd.fit(x_train, y_train)
    grd_enc.fit(grd.apply(x_train)[:, :, 0])
    # 训练逻辑回归分类器
    grd_lm.fit(grd_enc.transform(grd.apply(x_train_lr)[:, :, 0]), y_train_lr)
    #输出各个特征的权重值
    weights=grd_lm.coef_
    return weights, grd,grd_enc,grd_lm

def predict(grd_model,grd_enc_model,grd_lm_model,x_test,threshold):
    # 利用训练好的逻辑回归模型进行预测
    #threshold是判断正负的阈值
    # 预测为概率值
    y_pred_grd_lm_proba = grd_lm_model.predict_proba(grd_enc_model.transform(grd_model.apply(x_test)[:, :, 0]))[:, 1]
    y_pred_grd_lm=[]
    for i in range(len(y_pred_grd_lm_proba)):
        if y_pred_grd_lm_proba[i]>=threshold:
            y_pred_grd_lm.append(1)
        else:
            y_pred_grd_lm.append(0)
    # 直接输出预测标签
    #y_pred_grd_lm = grd_lm_model.predict(grd_enc_model.transform(grd_model.apply(x_test)[:, :, 0]))
    return  y_pred_grd_lm_proba,y_pred_grd_lm

# 计算召回率与准确率\F值
# a是度量准确率与召回率重要性的一个值,当a小于0.5时表示召回率更加重要
def calMeasures(y, predict_y, a):
    y.index=range(len(y))
    TP = 0
    TN = 0
    FP = 0
    FN = 0
    for i in range(len(y)):
        if y[i] == 1 and predict_y[i] == 1:
            TP = TP + 1
        elif y[i] == 0 and predict_y[i] == 0:
            TN = TN + 1
        elif y[i] == 1 and predict_y[i] == 0:
            FN = FN + 1
        elif y[i] == 0 and predict_y[i] == 1:
            FP = FP + 1
    recall = TP / (TP + FN)
    precision = TP / (TP + FP)
    F = 1 / (a * (1 / precision) + (1 - a) * (1 / recall))
    return recall, precision, F

if __name__ == '__main__':
    dataset = pd.read_csv('outputdata/dataset_label.csv')
    #在进行正负样本处理时，需要是否要对负样本进行下采样，如果是，那么split_train_test()这个函数的第二个值应该是个正整数，表示负样本是正样本的多个倍
    #如果不考虑对负样本进行下采样，而是用整个数据集进行判断，那么将split_train_test()这个函数的第二个值输入为0，在最后判断的时候给定标签0还是1的阈值由正样本与负样本的比值决定
    threshold,X_train, X_train_lr, Y_train, Y_train_lr, x_test, y_test = split_train_test(dataset, 1, 0.75)
    weights, grd,grd_enc,grd_lm = train(X_train,Y_train,X_train_lr,Y_train_lr,'lbfgs','balanced',1)
    # 保存模型
    print('-----------------------save model-----------------------------')
    joblib.dump(grd, "grd.m")
    joblib.dump(grd_enc, "grd_enc.m")
    joblib.dump(grd_lm, "grd_lm.m")
    y_pred_grd_lm_proba,y_pred_grd_lm = predict(grd,grd_enc,grd_lm,x_test,threshold)
    recall, precision, F = calMeasures(y_test, y_pred_grd_lm, 0.4)
    print('------------------threshold,recall, precision, F-----------------------')
    print(threshold, recall, precision, F)
