import pandas as pd
import requests
import os
from typing import List, Dict
from src.core.config import settings

def fetch_csv_data(file_path: str = "/app/data/coins.csv") -> List[Dict]:
    """Reads CSV and normalizes headers."""
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns={
            "Ticker": "symbol", 
            "Price": "price_usd",
            "MarketCap": "market_cap",
            "Date": "timestamp"
        })
        df['source'] = 'csv_file'
        # Ensure timestamp is string for consistency before Pydantic parsing
        df['timestamp'] = df['timestamp'].astype(str)
        return df.to_dict(orient='records')
    except Exception as e:
        print(f"CSV Error: {e}")
        return []

def fetch_api_data() -> List[Dict]:
    """Fetches live data from CoinPaprika API."""
    url = "https://api.coinpaprika.com/v1/tickers"
    
    # 1. Secure Authentication [cite: 16, 22]
    # We use the key from settings (loaded from .env)
    headers = {
        "Authorization": settings.COINPAPRIKA_API_KEY
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 2. Normalize to Unified Schema [cite: 20]
        results = []
        for item in data[:10]: # Limit to 10 for testing
            results.append({
                "symbol": item['symbol'],
                "price_usd": float(item['quotes']['USD']['price']),
                "market_cap": int(item['quotes']['USD']['market_cap']),
                "timestamp": item['last_updated'], # ISO format string
                "source": "coinpaprika_api"
            })
        return results

    except Exception as e:
        print(f"API Error: {e}")
        return []