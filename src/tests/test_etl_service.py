import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
from sqlalchemy.orm import Session
import requests # Used for mocking API errors

# --- Import your system components ---
from src.services.etl_service import run_full_etl_cycle
from src.core.models import CryptoData, ETLRun

# --- Fixtures for Mock Data ---
# Define a set of consistent data points for easy testing
SOURCE_NAME_CSV = "csv_file"
SOURCE_NAME_API = "coinpaprika_api"

# Helper to create data dictionaries
def create_data(source, symbol, price, minutes_ago):
    timestamp_dt = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return {
        "source": source,
        "symbol": symbol,
        "price_usd": price,
        "market_cap": 1000000,
        "timestamp": timestamp_dt.isoformat(),
    }

# Mock data sets
MOCK_BATCH_1 = [
    create_data(SOURCE_NAME_CSV, "BTC", 60000.0, 50), # 50 mins old
    create_data(SOURCE_NAME_API, "ETH", 4000.0, 45), # 45 mins old
]
MOCK_BATCH_2 = [
    create_data(SOURCE_NAME_CSV, "XLM", 0.5, 30),  # 30 mins old (NEW)
    create_data(SOURCE_NAME_API, "DOT", 15.0, 1), # 1 min old (NEW)
]

# --- Mocking Extractor Functions ---

# Use a side effect function to control the data returned, 
# ensuring we test incremental logic
def mock_csv_extractor_side_effect(last_checkpoint):
    # Simulates the extractor returning all data on the first run (last_checkpoint is None)
    # and only new data on subsequent runs.
    if last_checkpoint is None:
        return MOCK_BATCH_1 
    # In a real scenario, this mock would use the checkpoint to filter data.
    # For testing, we just return the new batch directly if the checkpoint exists.
    return MOCK_BATCH_2


# =========================================================================
# 🧪 P1.2: Incremental Ingestion & Idempotency Tests
# =========================================================================

# 1. Test Initial Load and Idempotency (P1.2, P1.4)
@patch('src.services.etl_service.fetch_coincap_data', return_value=[])
@patch('src.services.etl_service.fetch_api_data', return_value=MOCK_BATCH_1)
@patch('src.services.etl_service.fetch_csv_data', return_value=MOCK_BATCH_1)
def test_initial_load_and_idempotency(mock_csv, mock_api, mock_coincap, db: Session):
    """
    Test 1: Initial load works. 
    Test 2: Re-running with same data (Idempotency) adds no new rows.
    """
    
    # --- Run 1: Initial Load ---
    run_full_etl_cycle(db)
    assert db.query(CryptoData).count() == len(MOCK_BATCH_1) * 2
    
    # --- Run 2: Idempotent Run (P1.2) ---
    run_full_etl_cycle(db)
    # Total count must remain the same because the UPSERT updated the existing rows
    assert db.query(CryptoData).count() == len(MOCK_BATCH_1) * 2 
    
    # Check ETLRun tracking (P1.3)
    assert db.query(ETLRun).filter(ETLRun.status == "SUCCESS").count() == 2


# 2. Test Incremental Data Ingestion (P1.2, P1.4)
@patch('src.services.etl_service.fetch_coincap_data', return_value=[])
# Patch the CSV extractor to use the side effect for incremental data
@patch('src.services.etl_service.fetch_csv_data', side_effect=mock_csv_extractor_side_effect)
@patch('src.services.etl_service.fetch_api_data', return_value=[]) 
def test_incremental_ingestion_with_checkpoint(mock_api, mock_csv, mock_coincap, db: Session):
    """
    Test that the second run picks up the checkpoint and only ingests new data.
    """
    
    # Run 1: Initial Load
    run_full_etl_cycle(db)
    initial_count = db.query(CryptoData).count()
    
    # Run 2: Incremental Load (should only get MOCK_BATCH_2 data)
    run_full_etl_cycle(db)
    
    # The new count should be initial_count + len(MOCK_BATCH_2)
    assert db.query(CryptoData).count() == initial_count + len(MOCK_BATCH_2)
    
    # Verify new symbol (XLM) exists
    assert db.query(CryptoData).filter(CryptoData.symbol == "XLM").count() == 1
    
    # Verify checkpoint was successfully created (P1.2)
    last_success = db.query(ETLRun).filter(ETLRun.status == "SUCCESS").order_by(ETLRun.end_time.desc()).first()
    assert last_success.metadata_json is not None
    assert SOURCE_NAME_CSV + "_checkpoint" in last_success.metadata_json


# =========================================================================
# ❌ P1.2/P1.4: Failure and Recovery Tests
# =========================================================================

# 3. Test Failure Scenario and Checkpoint Non-Update (P1.2, P1.4)
def mock_failing_extractor(last_checkpoint=None):
    """Simulates a critical error."""
    raise requests.exceptions.HTTPError("403 Rate Limit Hit")

@patch('src.services.etl_service.fetch_coincap_data', side_effect=mock_failing_extractor)
@patch('src.services.etl_service.fetch_api_data', return_value=MOCK_BATCH_1)
@patch('src.services.etl_service.fetch_csv_data', return_value=MOCK_BATCH_1)
def test_failure_recovery_logic(mock_csv, mock_api, mock_coincap, db: Session):
    """
    Tests that a failure is recorded, and the system attempts to resume 
    from the original checkpoint (since checkpoint is not updated on failure).
    """
    
    # --- Run 1: Failure Run ---
    with pytest.raises(requests.exceptions.HTTPError):
        run_full_etl_cycle(db)
        
    # Verify Failure Tracking (P1.3): ETLRun must be recorded as FAILURE
    failed_run = db.query(ETLRun).order_by(ETLRun.end_time.desc()).first()
    assert failed_run.status == "FAILURE"
    assert "Rate Limit Hit" in failed_run.error_message # P0.4/P1.4 Failure Scenario
    
    # Verify Checkpoint Integrity (P1.2): Checkpoint should NOT have been updated on failure
    # (The previous successful checkpoint is still the valid starting point)
    assert failed_run.metadata_json is None or failed_run.metadata_json == {}
    
    # --- Run 2: Recovery Run ---
    # Restore the failing extractor to a successful state
    mock_coincap.side_effect = None
    mock_coincap.return_value = MOCK_BATCH_2 # New successful data
    
    run_full_etl_cycle(db)
    
    # Verify the successful run completed and updated the checkpoint
    success_run = db.query(ETLRun).filter(ETLRun.status == "SUCCESS").order_by(ETLRun.end_time.desc()).first()
    assert success_run.status == "SUCCESS"
    assert db.query(CryptoData).count() == 4 # Initial 2 + New 2
    
# =========================================================================
# 📊 P1.3: Stats Endpoint Data Generation Test
# =========================================================================

# 4. Test ETLRun Metadata Recording (P1.3)
@patch('src.services.etl_service.fetch_coincap_data', return_value=[])
@patch('src.services.etl_service.fetch_api_data', return_value=MOCK_BATCH_1)
@patch('src.services.etl_service.fetch_csv_data', return_value=MOCK_BATCH_1)
def test_etl_run_metadata_recording(mock_csv, mock_api, mock_coincap, db: Session):
    """
    Tests that the ETLRun table correctly records metrics needed for the /stats endpoint.
    """
    # Use the run ID to track the specific run
    result = run_full_etl_cycle(db)
    
    # Query the ETLRun record using the returned run_id
    etl_run = db.query(ETLRun).filter(ETLRun.run_id == result['run_id']).first()
    
    # Verify P1.3 required data points
    assert etl_run.status == "SUCCESS"
    assert etl_run.records_processed == result['new_records_inserted']
    assert etl_run.duration_ms is not None
    assert etl_run.end_time is not None
    assert etl_run.metadata_json is not None