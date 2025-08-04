from flask import Flask, render_template, request, redirect, url_for, jsonify
from pymongo import MongoClient
import re
import os
from bson import ObjectId

app = Flask(__name__, template_folder='../frontend/templates', static_folder='../frontend/static')

# MongoDB connection
MONGO_URI = "mongodb+srv://vinaycool1512:Qi5ZoWnBnchMslSr@compareproduct.kqhvrxt.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(MONGO_URI)
db = client["productwise"]
collection = db["product_best_prices"]

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        query = request.form.get("query")
        return redirect(url_for("search_product", product_name=query))
    return render_template("index.html")

#@app.route("/product/<product_name>")
#def search_product(product_name):
 #   search_regex = re.compile(re.escape(product_name), re.IGNORECASE)

  #  matched_docs = collection.find({
   #     "$or": [
    #        {"product_key": search_regex},
     #       {"best_price.title": search_regex},
      #      {"query": search_regex}
       # ]
    #})

    #results = []
    #for doc in matched_docs:
     #   best = doc.get("best_price", {})
      #  results.append({
       #     "title": best.get("title", ""),
        #    "price": best.get("price", ""),
         #   "source": best.get("source", ""),
         #  "url": best.get("url", "")
       # })

    #return render_template("result.html", product_name=product_name, results=results)
@app.route("/product/<product_name>")
def search_product(product_name):
    # Tokenize words like: ["headphone", "oneplus"]
    words = product_name.strip().lower().split()

    # Build MongoDB $and of $or regex conditions
    query_filters = []
    for word in words:
        regex = re.compile(re.escape(word), re.IGNORECASE)
        query_filters.append({
            "$or": [
                {"product_key": regex},
                {"best_price.title": regex},
                {"query": regex}
            ]
        })

    matched_docs = collection.find({"$and": query_filters})

    results = []
    for doc in matched_docs:
        best = doc.get("best_price", {})
        results.append({
            "title": best.get("title", ""),
            "price": best.get("price", ""),
            "source": best.get("source", ""),
            "url": best.get("url", "")
        })

    return render_template("result.html", product_name=product_name, results=results)


# Helper: convert ObjectId to string
def convert_objectid(o):
    if isinstance(o, ObjectId):
        return str(o)
    elif isinstance(o, list):
        return [convert_objectid(i) for i in o]
    elif isinstance(o, dict):
        return {k: convert_objectid(v) for k, v in o.items()}
    return o

@app.route("/debug/all-products")
def debug_all():
    all_docs = list(collection.find().limit(50))
    return jsonify(convert_objectid(all_docs))

if __name__ == "__main__":
     app.run(host='0.0.0.0', port=5000, debug=True)
    #app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

