from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
import time
import uuid

from src.core.database import get_db, get_db_status
from src.core.models import CryptoData
from src.ingestion.extractors import fetch_csv_data, fetch_api_data
from src.schemas.crypto import UnifiedCryptoData
from src.ingestion.loader import load_data

router = APIRouter()

# P0.2: GET /data with Pagination & Filtering
@router.get("/data")
def get_crypto_data(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100), # Pagination: Default 10, max 100
    offset: int = Query(0, ge=0),         # Pagination: Skip N records
    symbol: Optional[str] = None          # Filtering: Optional symbol search
):
    start_time = time.time()
    
    # 1. Build Query
    query = db.query(CryptoData)
    
    # 2. Apply Filter (if symbol is provided)
    if symbol:
        query = query.filter(CryptoData.symbol == symbol.upper())
    
    # 3. Apply Pagination
    total_records = query.count()
    data = query.offset(offset).limit(limit).all()
    
    # 4. Calculate Latency
    process_time = time.time() - start_time
    latency_ms = int(process_time * 1000)
    
    # 5. Return Response with Metadata
    return {
        "metadata": {
            "request_id": str(uuid.uuid4()),
            "api_latency_ms": latency_ms,
            "total_records_matched": total_records,
            "page_limit": limit,
            "page_offset": offset
        },
        "data": data
    }

# P0.2: GET /health (Updated to show real ETL status)
@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    # Check DB Connection
    is_connected = get_db_status()
    
    # Check Last ETL Run (Query the most recent timestamp in the DB)
    last_run = None
    if is_connected:
        try:
            # Get the newest timestamp from the table
            result = db.execute(text("SELECT MAX(timestamp) FROM unified_crypto_data")).scalar()
            last_run = result
        except Exception:
            pass

    return {
        "status": "healthy",
        "database": "connected" if is_connected else "disconnected",
        "etl_last_run": last_run 
    }

# POST /run-etl (Moved here to keep main.py clean)
@router.post("/run-etl")
def run_etl_job(db: Session = Depends(get_db)):
    # Extract
    csv_data = fetch_csv_data()
    api_data = fetch_api_data()
    raw_data = csv_data + api_data
    
    # Transform
    valid_data = []
    for record in raw_data:
        try:
            valid_data.append(UnifiedCryptoData(**record).model_dump())
        except Exception:
            pass

    # Load
    inserted_count = load_data(db, valid_data)
    
    return {
        "message": "ETL Job Completed",
        "processed": len(raw_data),
        "inserted": inserted_count
    }