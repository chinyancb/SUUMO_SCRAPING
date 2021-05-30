import sys
import re
import pandas as pd
from pymongo import MongoClient
from pymongo import DESCENDING
from pymongo import ASCENDING



def main(collection, outputfilename):

    client =  MongoClient("mongodb://127.0.0.1:27017", username='pyuser', password='ellegarden', authSource='mydb')
    db = client.mydb
    collection = db[collection]
    print(collection)

    # documentのkeyを取得
    doc_keys = list(collection.find_one().keys())

    # ネストしてるkeyは別途処理するため除外
    doc_keys.remove('アクセス')
    doc_keys.remove('部屋の特徴・設備')
    doc_keys.remove('物件概要')

    # 部屋の特徴・設備のvalueを全て取り出し、uniqueにする
    b_feature_array = []
    for doc in collection.find():
        b_feature_array_tmp = doc["部屋の特徴・設備"].split('、')
        b_feature_array.extend(b_feature_array_tmp)
    b_feature_array = list(set(b_feature_array))

    # 最寄り駅数の最大値
    max_nearest_station = 0
    for doc in collection.find():
        max_nearest_station_tmp = len(doc['アクセス'])
        if max_nearest_station_tmp >= max_nearest_station:
            max_nearest_station = max_nearest_station_tmp


    # メイン処理
    loop_cnt = 0
    for doc in collection.find():
        document_data = {}
        document_data = data_treat(doc, document_data, doc_keys)
        document_data = access_data_treat(doc, document_data, max_nearest_station)
        document_data = room_feat_data_treat(doc, document_data, feature_list=b_feature_array)
        document_data = room_overview_data_treat(doc, document_data)

        # pandasとして読み込みしcsvで出力
        df = pd.DataFrame.from_dict(document_data, orient='index').T
        if loop_cnt == 0:
            df.to_csv(f'./var/{outputfilename}.csv', header=True)
        else:
            df.to_csv(f'./var/{outputfilename}.csv', mode='a', header=False)

        loop_cnt += 1
        print(loop_cnt)


# ネストしていないものはそのまま格納
def data_treat(doc, document_data, doc_keys):
    for key in doc_keys:
        value = doc[key]
        document_data[key] = value

    return document_data



# アクセス
def access_data_treat(doc, document_data, max_nearest_station):

    # 最寄り駅数
    document_data['最寄り駅数'] = len(doc['アクセス'])

    # 最寄り駅から徒歩何分か
    for i in range(0, max_nearest_station):
        try:
            result = re.match('.+駅.+分', doc['アクセス'][i])
            if result != None:
                key = '最寄り駅' + '_' + str(i)
                document_data[key] = result.group()
        except IndexError as e:
            key = '最寄り駅' + '_' + str(i)
            document_data[key] = None
            continue
            

    return document_data


# 「部屋の特徴・設備」の加工。0 or 1 でフラグ化する
def room_feat_data_treat(doc, document_data, feature_list):

    for feat in feature_list:
        result = re.match(f'.+{feat}.+', doc['部屋の特徴・設備'])
        if result != None:
            document_data[feat] = 1
        else:
            document_data[feat] = 0

    return document_data


# 物件概要
def room_overview_data_treat(doc, document_data):

    return document_data | doc['物件概要'][0]





if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('argument error. : <collection>, <outputfilname>')
        sys.exit(1)

    collection = sys.argv[1]
    outputfilename = sys.argv[2]
    main(collection, outputfilename)
