from pymongo import MongoClient

MONGO_URI = "mongodb+srv://vinaycool1512:Qi5ZoWnBnchMslSr@compareproduct.kqhvrxt.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)
db = client["productwise"]

def get_best_product(query):
    return db["product_best_prices"].find_one({"query": query})
