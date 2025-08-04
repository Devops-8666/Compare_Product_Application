from pymongo import MongoClient
import os
from datetime import datetime  # ✅ Fixes the NameError

MONGO_URI = "mongodb+srv://vinaycool1512:Qi5ZoWnBnchMslSr@compareproduct.kqhvrxt.mongodb.net/?retryWrites=true&w=majority"


def get_db():
    client = MongoClient(MONGO_URI)
    return client["productwise"]

def insert_products(products):
    db = get_db()
 #   collection = db["amazon_products"]
    collection = db["raw_product_price"]  # ✅ Changed collection name
    for p in products:
        p["date"] = datetime.utcnow()  # ✅ Add current UTC datetime
        collection.update_one(
           # {"url": p["url"]},  # avoid duplicates
            {"url": p["url"], "source": p["source"]},  # upsert by URL + source
            {"$set": p},
            upsert=True
        )

