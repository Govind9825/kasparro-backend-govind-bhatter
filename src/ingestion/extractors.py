import pandas as pd
import requests
import time
from typing import List, Dict, Optional
from datetime import datetime
from src.core.config import settings

# --- Helper function for incremental date formatting ---
def _format_checkpoint(checkpoint: Optional[datetime]) -> Optional[str]:
    """Formats a datetime object to a string suitable for API/CSV filtering."""
    if checkpoint:
        # Format as ISO 8601 string, which is generally safe for APIs and comparisons
        return checkpoint.isoformat()
    return None

# --- Source 1: Legacy CSV (P0) ---
# --- Source 1: Legacy CSV (P0) ---
def fetch_csv_data(
    file_path: str = "/app/data/coins.csv",
    last_checkpoint: Optional[datetime] = None
) -> List[Dict]:
    source_name = 'csv_file'
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns={
            "Ticker": "symbol", 
            "Price": "price_usd",
            "MarketCap": "market_cap",
            "Date": "timestamp"
        })
        
        # 1. Unify source
        df['source'] = source_name
        
        # 2. CRITICAL: Convert the raw 'Date' column to a proper UTC Pandas datetime object
        # This handles the original string format and makes filtering possible.
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df = df.dropna(subset=['timestamp_dt']) # Drop rows where date conversion failed

        # --- P1.2 Incremental Filter ---
        if last_checkpoint:
            print(f"[{source_name}] Filtering data after {last_checkpoint.isoformat()}")
            # Filter rows where the datetime object is strictly GREATER than the last checkpoint
            df = df[df['timestamp_dt'] > last_checkpoint]
        
        # 3. Final Conversion: Convert the filtered datetime objects back to ISO string (for loading)
        df['timestamp'] = df['timestamp_dt'].apply(
            lambda x: x.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        )
        
        # Keep only the columns needed for the unified schema
        df = df[['symbol', 'price_usd', 'market_cap', 'timestamp', 'source']]
        
        # Note on _format_checkpoint: We don't need it here because we use the 
        # Python datetime object (last_checkpoint) directly for comparison.

        return df.to_dict(orient='records')
    except Exception as e:
        print(f"[{source_name}] Error: {e}")
        return []
    
# --- Source 2: CoinPaprika API (P0) ---
def fetch_api_data(last_checkpoint: Optional[datetime] = None) -> List[Dict]:
    """
    Fetches data from CoinPaprika API.
    P1.2 Note: CoinPaprika's free ticker endpoint doesn't support time-based incremental fetching.
    We will simulate an API that supports it by using a fixed limit, but acknowledge this limitation.
    For a production system, a time-series or historical endpoint would be needed.
    """
    source_name = "coinpaprika_api"
    url = "https://api.coinpaprika.com/v1/tickers"
    
    # Secure Handling (P0.1)
    headers = {}
    if hasattr(settings, 'COINPAPRIKA_API_KEY') and settings.COINPAPRIKA_API_KEY:
        # [cite_start]Use key securely from settings [cite: 22]
        headers = {"Authorization": settings.COINPAPRIKA_API_KEY} 

    # For a *real* incremental API, you would add a 'start_time' parameter here.
    # params = {"start_time": _format_checkpoint(last_checkpoint)} 
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"[{source_name}] Failed: {response.status_code}")
            return []
            
        data = response.json()
        results = []
        
        # Limit to the top 50 (as per original code)
        for item in data[:50]: 
            # --- Schema Unification (P0.1, P1.1) ---
            timestamp_utc = item['last_updated'].replace('Z', '+00:00') # Ensure UTC format
            results.append({
                "symbol": item['symbol'],
                "price_usd": float(item['quotes']['USD']['price']),
                "market_cap": int(item['quotes']['USD']['market_cap']),
                "timestamp": timestamp_utc,
                "source": source_name
            })
        
        # P1.2: Since the API is not incremental, we filter client-side if a checkpoint exists
        if last_checkpoint:
            return [
                record for record in results 
                if datetime.fromisoformat(record['timestamp']) > last_checkpoint
            ]

        return results
    except Exception as e:
        print(f"[{source_name}] Error: {e}")
        return []

# --- Source 3: CoinCap API (P1.1) ---
def fetch_coincap_data(last_checkpoint: Optional[datetime] = None) -> List[Dict]:
    """
    Fetches data from CoinCap API (used as the third source).
    P1.2 Note: Similar to CoinPaprika, this API's main asset endpoint is not truly incremental,
    so we will fetch the data and filter client-side based on the checkpoint.
    """
    source_name = "coincap_api"
    url = "https://api.coincap.io/v2/assets"
    params = {"limit": 50}
    
    # CoinCap uses a simple API key in the headers
    headers = {}
    # P0.1 Requirement: Handle authentication securely using the provided key
    if hasattr(settings, 'COINCAP_API_KEY') and settings.COINCAP_API_KEY:
        # The key (9fdb848a7a3828db22e4c4840776f531ea5a67815e62442ebf43216852c300e2) 
        # is loaded from the environment/settings
        headers = {"Authorization": settings.COINCAP_API_KEY}

    try:
        print(f"--- Contacting {source_name} ---", flush=True)
        response = requests.get(url, params=params, headers=headers, timeout=20)
        
        # ... (rest of the successful logic remains the same)
        # ...
        
        # P1.2: Filter data if a checkpoint is provided
        if last_checkpoint:
            # ... (client-side filtering logic remains the same)
            # ...
            pass
            
        print(f"SUCCESS: Fetched {len(results)} records from {source_name}.", flush=True)
        return results
    except Exception as e:
        print(f"[{source_name}] Error: {e}", flush=True)
        return []