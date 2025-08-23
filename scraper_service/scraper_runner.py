# scraper_runner.py

from amazon_scraper import scrape_amazon
from myntra_scraper import scrape_myntra
from db import get_db
import time

def get_tasks():
    db = get_db()
    return list(db.scrape_tasks.find({"status": {"$ne": "done"}}))

def mark_done(query):
    db = get_db()
    db.scrape_tasks.update_many({"query": query}, {"$set": {"status": "done"}})

def scrape_and_store(query):
    print(f"\n🔁 Scraping for: {query}")
    scrape_amazon(query)  # inserts internally
    scrape_myntra(query)  # inserts internally
    mark_done(query)

def run_once(interval_minutes=120):  # run once a day
  #  while True:
        print("\n🔍 Checking for new scrape tasks...")
        tasks = get_tasks()

        if tasks:
            for task in tasks:
                scrape_and_store(task["query"])
        else:
            print("✅ No pending scrape tasks found.")

#        print(f"⏳ Sleeping for {interval_minutes} minutes...\n")
 #       time.sleep(interval_minutes * 60)

if __name__ == "__main__":
    run_once()

