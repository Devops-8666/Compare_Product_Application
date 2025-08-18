# compare_worker.py

from pymongo import MongoClient
import re
from collections import defaultdict

# ✅ Step 1: Connect to MongoDB Atlas
MONGO_URI = "mongodb+srv://vinaycool1512:Qi5ZoWnBnchMslSr@compareproduct.kqhvrxt.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)
db = client["productwise"]
raw = db["raw_product_price"]
best = db["product_best_prices"]

print("🚀 Connected to MongoDB Atlas successfully!")

# ✅ Step 2: Helper function to clean and normalize title
def normalize_title(title):
    title = title.lower()  # Make lowercase
    title = re.sub(r'[^a-z0-9 ]', '', title)  # Remove symbols
    title = re.sub(r'\s+', ' ', title).strip()  # Remove extra spaces
    return title

# ✅ Step 3: Fetch raw data
all_entries = list(raw.find())
print(f"📦 Total raw entries fetched: {len(all_entries)}")

# Group by normalized title
grouped = defaultdict(list)

for entry in all_entries:
    title = entry.get("title", "")
    price = entry.get("price", 0)

    if not title or not isinstance(price, (int, float, str)):
        print(f"⚠️ Skipping invalid entry: {entry}")
        continue

    try:
        price = float(price)
    except Exception as e:
        print(f"⚠️ Could not convert price for entry {entry}: {e}")
        continue

    product_key = normalize_title(title)
    entry["price"] = price
    grouped[product_key].append(entry)

print(f"🔑 Grouped into {len(grouped)} product keys")

# ✅ Step 4: Find best prices and update MongoDB
for key, entries in grouped.items():
    if not entries:
        continue

    entries.sort(key=lambda x: x["price"])
    best_price = entries[0]

    doc = {
        "product_key": key,
        "query": entries[0].get("query", ""),
        "best_price": {
            "price": best_price["price"],
            "title": best_price["title"],
            "source": best_price["source"],
            "url": best_price["url"],
        },
        "all_prices": [
            {
                "price": e["price"],
                "title": e["title"],
                "source": e["source"],
                "url": e["url"],
            } for e in entries
        ]
    }

    result = best.update_one({"product_key": key}, {"$set": doc}, upsert=True)
    if result.upserted_id:
        print(f"✅ Inserted new best price for '{key}'")
    else:
        print(f"♻️ Updated best price for '{key}'")

print("🎉 Product comparison complete. Best prices stored in 'product_best_prices'.")

