# myntra_scraper_worker.py

from scraper.myntra import scrape_myntra

def main():
    query = input("Enter a product to scrape from Myntra: ").strip()
    if query:
        print(f"[Myntra] 🔍 Scraping search results for: {query}")
        scrape_myntra(query)
    else:
        print("❌ Please enter a valid search query.")

if __name__ == "__main__":
    main()

