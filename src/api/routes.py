from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import Optional
import time
import uuid

from src.core.database import get_db, get_db_status
from src.core.models import CryptoData
# Import the new extractor
from src.ingestion.extractors import fetch_csv_data, fetch_api_data, fetch_coingecko_data
from src.schemas.crypto import UnifiedCryptoData
from src.ingestion.loader import load_data

router = APIRouter()

@router.get("/data")
def get_crypto_data(
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = None
):
    query = db.query(CryptoData)
    if symbol:
        query = query.filter(CryptoData.symbol == symbol.upper())
    
    total_records = query.count()
    data = query.offset(offset).limit(limit).all()
    
    return {
        "metadata": {"total": total_records, "limit": limit, "offset": offset},
        "data": data
    }

# --- P1.3 Requirement: Stats Endpoint ---
@router.get("/stats")
def get_etl_statistics(db: Session = Depends(get_db)):
    """Returns aggregated stats (Records processed, Last success, etc.)"""
    
    # 1. Aggregation: Count records per source
    source_counts = db.query(
        CryptoData.source, 
        func.count(CryptoData.id)
    ).group_by(CryptoData.source).all()
    
    # 2. Aggregation: Get latest timestamp
    last_update = db.execute(text("SELECT MAX(timestamp) FROM unified_crypto_data")).scalar()

    return {
        "system_status": "operational",
        "last_successful_run": last_update,
        "records_by_source": {source: count for source, count in source_counts},
        "total_records": sum(count for _, count in source_counts)
    }

@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    is_connected = get_db_status()
    return {"status": "healthy", "database": "connected" if is_connected else "disconnected"}

@router.post("/run-etl")
def run_etl_job(db: Session = Depends(get_db)):
    # 1. Extract from ALL 3 Sources (P1.1)
    csv_data = fetch_csv_data()          # Source 1
    api_data = fetch_api_data()          # Source 2
    quirky_data = fetch_coingecko_data() # Source 3 (New!)
    
    raw_data = csv_data + api_data + quirky_data
    
    # 2. Transform & Load
    valid_data = []
    for record in raw_data:
        try:
            valid_data.append(UnifiedCryptoData(**record).model_dump())
        except Exception:
            pass

    inserted_count = load_data(db, valid_data)
    
    return {
        "message": "ETL Job Completed",
        "sources_processed": ["csv_standard", "coinpaprika_api", "csv_quirky"],
        "total_extracted": len(raw_data),
        "new_records_inserted": inserted_count
    }