import pandas as pd
import requests
import time
from typing import List, Dict
from src.core.config import settings

# --- Source 1: Legacy CSV (P0) ---
def fetch_csv_data(file_path: str = "/app/data/coins.csv") -> List[Dict]:
    """
    Reads legacy data from a local CSV file.
    """
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns={
            "Ticker": "symbol", 
            "Price": "price_usd",
            "MarketCap": "market_cap",
            "Date": "timestamp"
        })
        df['source'] = 'csv_file'
        df['timestamp'] = df['timestamp'].astype(str)
        return df.to_dict(orient='records')
    except Exception as e:
        print(f"CSV Error: {e}")
        return []

# --- Source 2: CoinPaprika API (P0) ---
def fetch_api_data() -> List[Dict]:
    """
    Fetches data from CoinPaprika API.
    """
    url = "https://api.coinpaprika.com/v1/tickers"
    
    headers = {}
    if hasattr(settings, 'COINPAPRIKA_API_KEY') and settings.COINPAPRIKA_API_KEY:
        headers = {"Authorization": settings.COINPAPRIKA_API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"CoinPaprika Failed: {response.status_code}")
            return []
            
        data = response.json()
        results = []
        for item in data[:50]:
            results.append({
                "symbol": item['symbol'],
                "price_usd": float(item['quotes']['USD']['price']),
                "market_cap": int(item['quotes']['USD']['market_cap']),
                "timestamp": item['last_updated'],
                "source": "coinpaprika_api"
            })
        return results
    except Exception as e:
        print(f"CoinPaprika Error: {e}")
        return []

# --- Source 3: CoinCap API (Reliable Docker Replacement for CoinGecko) ---
def fetch_coingecko_data() -> List[Dict]:
    """
    Fetches data from CoinCap API as a stable replacement for CoinGecko.
    Note: CoinCap is used here because CoinGecko free tier consistently fails in Docker environments.
    """
    url = "https://api.coincap.io/v2/assets"
    params = {"limit": 50}
    
    try:
        print(f"--- Contacting CoinCap API (CoinGecko Replacement) ---", flush=True)
        response = requests.get(url, params=params, timeout=20)
        
        if response.status_code == 200:
            data = response.json().get('data', [])
            results = []
            for item in data:
                # CoinCap returns strings for numbers, must cast them
                results.append({
                    "symbol": item['symbol'],
                    "price_usd": float(item.get('priceUsd') or 0),
                    "market_cap": int(float(item.get('marketCapUsd') or 0)),
                    # CoinCap often uses current time for list updates; using current time for timestamp
                    "timestamp": pd.Timestamp.now(tz='UTC').isoformat(), 
                    "source": "coincap_api"  # Change source to reflect CoinCap
                })
            print(f"SUCCESS: Fetched {len(results)} records from CoinCap.", flush=True)
            return results
        else:
            print(f"CoinCap Failed: Status {response.status_code}", flush=True)
            return []
            
    except Exception as e:
        print(f"CoinCap Error: {e}", flush=True)
        return []