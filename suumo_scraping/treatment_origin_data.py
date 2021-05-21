from pymongo import MongoClient
from pymongo import DESCENDING
from pymongo import ASCENDING







client =  MongoClient("mongodb://127.0.0.1:27017", username='pyuser', password='ellegarden', authSource='mydb')
db = client.mydb
collection = db.suumo
#print(db.suumo.find_one())


# 部屋の特徴・設備のvalueを全て取り出し、uniqueにする
b_feature_array = []
for doc in db.suumo.find().limit(5):
    b_feature_array_tmp = doc["部屋の特徴・設備"].split('、')
    b_feature_array.extend(b_feature_array_tmp)


b_feature_array = list(set(b_feature_array))
print(b_feature_array)
