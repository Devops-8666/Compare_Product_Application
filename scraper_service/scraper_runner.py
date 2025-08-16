# scraper_runner.py

import os
import time
from amazon_scraper import scrape_amazon
from myntra_scraper import scrape_myntra
from db import get_db

# Read configs from environment (ConfigMap in OpenShift will inject these)
SCRAPER_INTERVAL = int(os.getenv("SCRAPER_INTERVAL", "120"))  # default 120 minutes
SCRAPER_SITES = os.getenv("SCRAPER_SITES", "amazon,myntra").split(",")

def get_tasks():
    db = get_db()
    return list(db.scrape_tasks.find({"status": {"$ne": "done"}}))

def mark_done(query):
    db = get_db()
    db.scrape_tasks.update_many({"query": query}, {"$set": {"status": "done"}})

def scrape_and_store(query):
    print(f"\n🔁 Scraping for: {query}")
    if "amazon" in SCRAPER_SITES:
        scrape_amazon(query)  # inserts internally
    if "myntra" in SCRAPER_SITES:
        scrape_myntra(query)  # inserts internally
    mark_done(query)

def run_periodically(interval_minutes=SCRAPER_INTERVAL):
    while True:
        print("\n🔍 Checking for new scrape tasks...")
        tasks = get_tasks()

        if tasks:
            for task in tasks:
                scrape_and_store(task["query"])
        else:
            print("✅ No pending scrape tasks found.")

        print(f"⏳ Sleeping for {interval_minutes} minutes...\n")
        time.sleep(interval_minutes * 60)

if __name__ == "__main__":
    run_periodically()

