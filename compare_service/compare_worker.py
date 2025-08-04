# compare_worker.py

from pymongo import MongoClient
import re
from collections import defaultdict

# ✅ Step 1: Connect to MongoDB
MONGO_URI = "mongodb+srv://vinaycool1512:Qi5ZoWnBnchMslSr@compareproduct.kqhvrxt.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)
db = client["productwise"]
raw = db["raw_product_price"]
best = db["product_best_prices"]

# ✅ Step 2: Helper function to clean and normalize title
def normalize_title(title):
    title = title.lower()  # Make lowercase
    title = re.sub(r'[^a-z0-9 ]', '', title)  # Remove symbols
    title = re.sub(r'\s+', ' ', title).strip()  # Remove extra spaces
    return title

# ✅ Step 3: Group raw products by cleaned title (product_key)
grouped = defaultdict(list)
all_entries = list(raw.find())

for entry in all_entries:
    title = entry.get("title", "")
    price = entry.get("price", 0)

    # Skip entries with missing or invalid data
    if not title or not isinstance(price, (int, float, str)):
        continue

    try:
        price = float(price)
    except:
        continue

    product_key = normalize_title(title)
    entry["price"] = price  # Ensure price is float
    grouped[product_key].append(entry)

# ✅ Step 4: For each group, find best price & save all
for key, entries in grouped.items():
    if not entries:
        continue

    # Sort by price ascending
    entries.sort(key=lambda x: x["price"])
    best_price = entries[0]  # Cheapest one

    # Save in `product_best_prices` collection
    best.update_one(
        {"product_key": key},
        {
            "$set": {
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
        },
        upsert=True
    )

print("✅ Product comparison complete. Best prices stored in 'product_best_prices'.")

