# Use official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy all scraper scripts
COPY . .

# If your MongoDB logic is shared in ../db.py, copy it
COPY db.py /app/db.py

# Copy Playwright dependency config
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser dependencies
RUN apt-get update && \
    apt-get install -y wget gnupg ca-certificates fonts-liberation libnss3 libxss1 libasound2 libatk1.0-0 libatk-bridge2.0-0 libgbm1 libgtk-3-0 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 xdg-utils && \
    pip install playwright && \
    playwright install --with-deps

# Default command to run Amazon & Myntra scrapers (can be customized later)
CMD ["python3", "scraper_runner.py"]


