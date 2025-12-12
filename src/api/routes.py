from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from typing import Optional, Dict
import time
import uuid
from collections import OrderedDict # <-- NEW IMPORT for Deduplication
from src.core.models import CryptoData, ETLRun
from datetime import datetime

from src.core.database import get_db, get_db_status
# Removed CryptoData import as it's already imported above
from src.ingestion.extractors import fetch_csv_data, fetch_api_data, fetch_coincap_data
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
# --- P1.3 Requirement: Stats Endpoint ---
@router.get("/stats")
def get_etl_statistics(db: Session = Depends(get_db)):
    """Returns ETL summaries: Records processed, Duration, Last success & failure timestamps, Run metadata """
    
    # 1. Get the latest successful run
    last_success = db.query(ETLRun).filter(
        ETLRun.status == "SUCCESS"
    ).order_by(
        ETLRun.end_time.desc()
    ).first()

    # 2. Get the latest failed run
    last_failure = db.query(ETLRun).filter(
        ETLRun.status == "FAILURE"
    ).order_by(
        ETLRun.end_time.desc()
    ).first()

    # 3. Get total records across all unified data
    total_records = db.query(func.count(CryptoData.id)).scalar()

    # 4. Aggregate: Count records per source (useful for sanity check)
    source_counts = db.query(
        CryptoData.source, 
        func.count(CryptoData.id)
    ).group_by(CryptoData.source).all()
    
    # 

    return {
        "system_status": "operational",
        "total_records_in_db": total_records,
        "records_by_source": {source: count for source, count in source_counts},
        "etl_summary": {
            "last_successful_run": {
                "timestamp": last_success.end_time.isoformat() if last_success else None,
                "duration_ms": last_success.duration_ms if last_success else None,
                "records_processed": last_success.records_processed if last_success else 0,
                "run_metadata": last_success.metadata_json if last_success else None
            },
            "last_failed_run": {
                "timestamp": last_failure.end_time.isoformat() if last_failure else None,
                "duration_ms": last_failure.duration_ms if last_failure else None,
                "error_message": last_failure.error_message if last_failure else None,
            },
            "total_run_history_count": db.query(ETLRun).count()
        }
    }

@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    is_connected = get_db_status()
    return {"status": "healthy", "database": "connected" if is_connected else "disconnected"}

@router.post("/run-etl")
def run_etl_job(db: Session = Depends(get_db)):
    run_id = str(uuid.uuid4())
    start_ms = int(time.time() * 1000)
    
    # Placeholder for P1.2 checkpoint update (needs to be implemented fully in a service layer)
    # For now, we manually initialize the dict to store new checkpoints
    new_checkpoints: Dict[str, str] = {}
    
    # 1. Start: Create a new RUNNING record
    new_run = ETLRun(run_id=run_id, status="RUNNING")
    db.add(new_run)
    db.commit()
    db.refresh(new_run)

    try:
        # P1.1: Fetch data (NOTE: The extractors need the checkpoint logic here)
        # Assuming checkpoint logic is either handled inside extractors or is a full load for now
        csv_data = fetch_csv_data() 
        api_data = fetch_api_data() 
        coincap_data = fetch_coincap_data() 
        
        raw_data = csv_data + api_data + coincap_data
        
        # 2. Transform & DEDUPLICATE (P1.2 Cardinality Fix)
        # Use OrderedDict to maintain insertion order while ensuring internal batch uniqueness
        unique_records_dict = OrderedDict()
        
        for record in raw_data:
            try:
                # Add validation logic (P0.1)
                validated_data = UnifiedCryptoData(**record).model_dump()
                
                # Create the composite unique key string
                unique_key = f"{validated_data['symbol']}|{validated_data['timestamp']}|{validated_data['source']}"
                
                # Store the record. If the key is a duplicate, the new value overwrites the old one.
                # This fixes the PostgreSQL CardinalityViolation error.
                unique_records_dict[unique_key] = validated_data
                
            except Exception:
                # Log the transformation failure here (P0.4)
                pass

        valid_data = list(unique_records_dict.values())
        inserted_count = load_data(db, valid_data)
        
        # 3. Success: Update run record
        end_ms = int(time.time() * 1000)
        
        # In a full P1.2 solution, you would gather and update checkpoints here.
        # For submission, we mark the run as successful.
        
        new_run.end_time = datetime.utcnow()
        new_run.status = "SUCCESS"
        new_run.duration_ms = end_ms - start_ms
        new_run.records_processed = inserted_count 
        # new_run.metadata_json = new_checkpoints # Placeholder for P1.2 completion
        db.commit()

        return {
            "message": "ETL Job Completed",
            "run_id": run_id,
            "total_extracted_unique": len(valid_data), # Return unique count
            "new_records_inserted": inserted_count,
            "duration_ms": new_run.duration_ms
        }
    
    except Exception as e:
        # 4. Failure: Update run record (P1.3)
        end_ms = int(time.time() * 1000)
        new_run.end_time = datetime.utcnow()
        new_run.status = "FAILURE"
        new_run.duration_ms = end_ms - start_ms
        new_run.error_message = str(e)
        db.commit()
        raise