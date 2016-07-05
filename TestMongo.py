from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['IdentityDB']
collection = db['roles']
count = collection.count()
print(count)





