from pymongo import MongoClient

def connect_mongodb():
    client =  MongoClient("mongodb://127.0.0.1:27017", username='pyuser', password='ellegarden', authSource='mydb')
    db = client.mydb
    collection = db.suumo
    return client, collection



def main():

    client, collection = connect_mongodb()

    try:
        # 重複しているかをカウント
        pipline = [{"$group":{"_id":"$building_name", "dups":{"$push":"$_id"}, "count": {"$sum": 1}}},{"$match":{"count": {"$gt": 1}}}]
        duplicates = collection.aggregate(pipline)
        print(len(list(duplicates)))

    except Exception as e:
        client.close()

    client.close()



if __name__ == "__main__":
    main()
