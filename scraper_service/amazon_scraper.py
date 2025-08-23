# scraper/amazon.py

from playwright.sync_api import sync_playwright, TimeoutError
from db import insert_products
import time

def scrape_amazon(query):
    print(f"\n[Amazon] Searching for: {query}")
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        context = browser.new_context()
        page = context.new_page()

        print("[Amazon] Opening homepage...")
        page.goto("https://www.amazon.in", timeout=60000)

        print("[Amazon] Waiting for search box...")
        try:
            page.wait_for_selector("input#twotabsearchtextbox", timeout=15000)
        except TimeoutError:
            print("❌ Search box not found.")
            return []

        page.fill("input#twotabsearchtextbox", query)
        page.keyboard.press("Enter")

        print("[Amazon] Waiting for product results...")
        try:
            page.wait_for_selector("div.s-main-slot", timeout=15000)
        except TimeoutError:
            print("❌ Timeout waiting for results container.")
            return []

        page_num = 1
        while True:
            print(f"\n📄 Page {page_num}:")
            time.sleep(3)
            blocks = page.query_selector_all("div.s-main-slot > div[data-component-type='s-search-result']")
            print(f"🔍 Found {len(blocks)} product blocks. Extracting titles, prices, and URLs...")

            for i, block in enumerate(blocks):
                try:
                    asin = block.get_attribute("data-asin")
                    if not asin:
                        print(f"[Block {i}] ❌ Missing ASIN, skipped.")
                        continue

                    url = f"https://www.amazon.in/dp/{asin}"

                    title_elem = block.query_selector("h2 span")
                    title = title_elem.inner_text().strip() if title_elem else None

                    price_elem = block.query_selector("span.a-price > span.a-offscreen")
                    price_text = price_elem.inner_text().strip().replace("₹", "").replace(",", "") if price_elem else None

                    if not title or not price_text:
                        missing = []
                        if not title: missing.append("title")
                        if not price_text: missing.append("price")
                        print(f"[Block {i}] ❌ Missing {', '.join(missing)}, skipped.")
                        continue

                    print(f"[Block {i}] ✅ {title} — ₹{price_text} — {url}")

                    results.append({
                        "title": title,
                        "price": float(price_text),
                        "url": url,
                        "source": "Amazon",
                        "query": query
                    })

                except Exception as e:
                    print(f"[Block {i}] ⚠️ Error: {e}")
                    continue

            # Check for 'Next' button
            try:
                next_button = page.query_selector("a.s-pagination-next")
                disabled = next_button.get_attribute("aria-disabled") if next_button else "true"
                if next_button and disabled != "true":
                    print("➡️ Going to next page...")
                    next_button.click()
                    page.wait_for_timeout(3000)
                    page.wait_for_selector("div.s-main-slot", timeout=15000)
                    page_num += 1
                else:
                    print("⏹️ No more pages.")
                    break
            except Exception as e:
                print(f"⚠️ Pagination error: {e}")
                break

        browser.close()

    if results:
        insert_products(results)
        print(f"✅ Extracted {len(results)} products inserted into DB.")
    else:
        print("❌ No products extracted.")
    return results

