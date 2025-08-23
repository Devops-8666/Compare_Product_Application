# compare_worker.py
from pymongo import MongoClient      # Import MongoDB client
import re                            # For regex-based title cleaning
from collections import defaultdict  # For grouping products by key
from bson.objectid import ObjectId   # To handle MongoDB ObjectIds
import os
#from dotenv import load_dotenv  #remove for openshift deplpy to take env from configmap

# load local .env (ignored in container if not present)
#load_dotenv()#remove for openshift deploy

# ✅ MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "productwise")
client = MongoClient(MONGO_URI)     # Connect to MongoDB Atlas cluster
db = client[DB_NAME]          # Select "productwise" database
raw = db["raw_product_price"]       # Collection with raw scraped prices
best = db["product_best_prices"]    # Collection with best price per product
meta = db["compare_metadata"]       # Metadata collection (used to track last processed record)

print("🚀 Connected to MongoDB Atlas successfully!")

# ✅ Normalize product title so that "Samsung Galaxy S21!!!" → "samsung galaxy s21"
def normalize_title(title):
    title = title.lower()                        # lowercase for consistency
    title = re.sub(r'[^a-z0-9 ]', '', title)     # remove special chars, keep only letters/numbers/spaces
    title = re.sub(r'\s+', ' ', title).strip()   # normalize spaces (collapse multiple → single)
    return title

# ✅ Resume from last processed ObjectId to avoid reprocessing 5000+ old docs
meta_doc = meta.find_one({"_id": "last_processed"})   # fetch checkpoint doc
last_id = ObjectId("000000000000000000000000")        # default: start from "zero" if no checkpoint exists
if meta_doc and "last_id" in meta_doc:
    last_id = meta_doc["last_id"]                     # restore last checkpoint if exists

print(f"⏳ Resuming from last ObjectId: {last_id}")

# ✅ Fetch only NEW raw records (incremental)
new_entries = list(raw.find({"_id": {"$gt": last_id}}).sort("_id", 1))
if not new_entries:
    print("✅ No new records found. Exiting.")
    exit(0)

print(f"📦 New raw entries to process: {len(new_entries)}")

# ✅ Group new entries by normalized product title
grouped = defaultdict(list)
for entry in new_entries:
    title = entry.get("title", "")
    price = entry.get("price", 0)

    # skip bad or missing values
    if not title or not isinstance(price, (int, float, str)):
        continue
    try:
        price = float(price)   # convert to float safely
    except:
        continue

    product_key = normalize_title(title)  # normalized title is key
    entry["price"] = price
    grouped[product_key].append(entry)    # group all entries of same product

print(f"🔑 Grouped into {len(grouped)} new product keys")

# ✅ For each grouped product → update best price DB
for key, entries in grouped.items():
    entries.sort(key=lambda x: x["price"])  # sort by price ascending
    best_price = entries[0]                 # pick lowest price as best

    # ✅ Check if product already exists in best collection
    existing = best.find_one({"product_key": key})
    all_prices = []

    if existing:
        # keep existing prices but prevent duplicate (source+url = unique key)
        seen = set()
        for e in existing.get("all_prices", []):
            unique_key = (e["source"], e["url"])
            seen.add(unique_key)
            all_prices.append(e)

        # add new prices only if not already present
        for e in entries:
            unique_key = (e["source"], e["url"])
            if unique_key not in seen:
                seen.add(unique_key)
                all_prices.append({
                    "price": e["price"],
                    "title": e["title"],
                    "source": e["source"],
                    "url": e["url"],
                })
    else:
        # new product: just take all new entries
        all_prices = [
            {"price": e["price"], "title": e["title"], "source": e["source"], "url": e["url"]}
            for e in entries
        ]

    # Final document to insert/update
    doc = {
        "product_key": key,
        "query": entries[0].get("query", ""),    # original search query
        "best_price": {                          # best (lowest) price entry
            "price": best_price["price"],
            "title": best_price["title"],
            "source": best_price["source"],
            "url": best_price["url"],
        },
        "all_prices": all_prices,                # all unique offers for this product
    }

    # ✅ Upsert: insert new or update existing
    result = best.update_one({"product_key": key}, {"$set": doc}, upsert=True)
    if result.upserted_id:
        print(f"✅ Inserted new best price for '{key}'")
    else:
        print(f"♻️ Updated best price for '{key}' with {len(entries)} new entries")

# ✅ Update last processed checkpoint
last_processed_id = new_entries[-1]["_id"]   # latest ObjectId processed
meta.update_one(
    {"_id": "last_processed"},
    {"$set": {"last_id": last_processed_id}},
    upsert=True
)

print(f"🎉 Done! Last processed ID updated to {last_processed_id}")

