from scraper.amazon_all_pages import scrape_amazon
from db import insert_products  # ✅ Import the DB insertion function

def run_scraper():
    query = input("Enter a product to scrape: ").strip()
    print(f"\n🔍 Scraping '{query}' from Amazon...\n")
    amazon_data = scrape_amazon(query)

    # ✅ Insert into MongoDB if products were found
    if amazon_data:
        insert_products(amazon_data)
        print(f"✅ Inserted {len(amazon_data)} products into MongoDB.")
    else:
        print("⚠️ No data to insert.")

if __name__ == "__main__":
    run_scraper()
