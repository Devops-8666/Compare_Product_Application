# scraper/myntra.py

from playwright.sync_api import sync_playwright
from urllib.parse import quote, urljoin
from db import insert_products  # assumes insert_products(list_of_dicts) exists

def clean_price(text):
    return text.replace("Rs.", "").replace("₹", "").strip() if text else None

def scrape_myntra(search_query):
    all_products = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        encoded_query = quote(search_query)
        page_number = 1
        max_pages = 10  # limit pages to prevent infinite scraping

        while page_number <= max_pages:
            print(f"[Myntra] 🔍 Page {page_number} for '{search_query}'...")
            url = f"https://www.myntra.com/{encoded_query}?p={page_number}"
            page.goto(url, timeout=60000)

            try:
                page.wait_for_selector('li.product-base', timeout=10000)
            except:
                print("❌ No more products or failed to load.")
                break

            product_elements = page.query_selector_all('li.product-base')
            if not product_elements:
                print("❌ No product elements found on page.")
                break

            for product in product_elements:
                try:
                    title = product.query_selector('h3.product-brand').inner_text().strip()
                    subtitle = product.query_selector('h4.product-product').inner_text().strip()

                    # Price handling: fallback for missing discounts
                    discounted_elem = product.query_selector('span.product-discountedPrice')
                    original_elem = product.query_selector('span.product-strike')
                    price_elem = product.query_selector('span.product-price')

                    discounted_price = clean_price(discounted_elem.inner_text()) if discounted_elem else None
                    original_price = clean_price(original_elem.inner_text()) if original_elem else None

                    if not discounted_price and price_elem:
                        discounted_price = clean_price(price_elem.inner_text())

                    if not original_price:
                        original_price = clean_price(price_elem.inner_text()) if price_elem else discounted_price

                    # Skip product if no valid price found
                    if not discounted_price:
                        continue

                    relative_url = product.query_selector('a').get_attribute('href')
                    full_url = urljoin("https://www.myntra.com", relative_url)

                    product_data = {
                        "source": "myntra",
                        "query": search_query,
                        "title": f"{title} {subtitle}",
                        "price": discounted_price,
                        "original_price": original_price,
                        "url": full_url
                    }
                    all_products.append(product_data)

                    print(f"✅ {product_data['title']} | Rs. {discounted_price}Rs. {original_price} | {full_url}")
                except Exception as e:
                    print(f"⚠️ Error parsing product: {e}")
                    continue

            # Pagination: check for "Next" page
            next_button = page.query_selector('li.pagination-next')
            if next_button and "disabled" not in (next_button.get_attribute("class") or ""):
                page_number += 1
            else:
                break

        browser.close()

    if all_products:
        insert_products(all_products)
        print(f"\n✅ Inserted {len(all_products)} Myntra products into MongoDB.")
    else:
        print("❌ No products found to insert.")

