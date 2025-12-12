import csv
import random
from datetime import datetime, timedelta

# Standard crypto list to make it look real
symbols = ["BTC", "ETH", "SOL", "ADA", "XRP", "DOT", "DOGE", "AVAX", "LTC", "LINK"]
sources = ["Legacy_System_A", "Legacy_System_B"]

header = ["Ticker", "Price", "MarketCap", "Date", "Source"]
rows = []

# Generate 50 rows of data
for i in range(50):
    symbol = random.choice(symbols)
    # Random price between $10 and $50,000
    price = round(random.uniform(10, 50000), 2)
    # Random huge number for market cap
    mcap = random.randint(1_000_000, 1_000_000_000)
    # Random date within the last 30 days
    days_ago = random.randint(0, 30)
    date_str = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    rows.append([symbol, price, mcap, date_str, random.choice(sources)])

# Write to the file
with open("data/coins.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print("Success! Generated data/coins.csv with 50 rows.")