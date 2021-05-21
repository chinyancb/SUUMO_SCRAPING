import re
from pymongo import MongoClient
from pymongo import DESCENDING
from pymongo import ASCENDING



def main():

    client =  MongoClient("mongodb://127.0.0.1:27017", username='pyuser', password='ellegarden', authSource='mydb')
    db = client.mydb
    # 加工したデータを格納するコレクション
    collection_ana = db.suumo_ana

    # documentのkeyを取得
    doc_keys = list(db.suumo.find_one().keys())

    # ネストしてるkeyは別途処理するため除外
    doc_keys.remove('アクセス')
    doc_keys.remove('部屋の特徴・設備')
    doc_keys.remove('物件概要')

    # 部屋の特徴・設備のvalueを全て取り出し、uniqueにする
    b_feature_array = []
    for doc in db.suumo.find():
        b_feature_array_tmp = doc["部屋の特徴・設備"].split('、')
        b_feature_array.extend(b_feature_array_tmp)
    b_feature_array = list(set(b_feature_array))


    # メイン処理
    for doc in db.suumo.find().limit(1):
        document_data = {}
        document_data = data_treat(doc, document_data, doc_keys)
        document_data = access_data_treat(doc, document_data)
        document_data = room_feat_data_treat(doc, document_data, feature_list=b_feature_array)

    print(document_data)


# ネストしていないものはそのまま格納
def data_treat(doc, document_data, doc_keys):
    for key in doc_keys:
        value = doc[key]
        document_data[key] = value

    return document_data



# アクセス
def access_data_treat(doc, document_data):

    document_data['最寄り駅数'] = len(doc['アクセス'])
    for s in doc['アクセス']:
        result = re.match('.+桜新町駅.+分', s)
        if result != None:
            document_data['アクセス'] = result.group()

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




if __name__ == '__main__':
    main()
