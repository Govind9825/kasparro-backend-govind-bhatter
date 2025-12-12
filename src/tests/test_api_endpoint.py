import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

# 1. Import your main application and core models
from main import app # Assuming your main FastAPI app is in 'main.py'
from src.core.models import CryptoData, ETLRun

# 2. Define a fixture to yield the TestClient
# This fixture assumes you have a fixture for the test database session (db) 
# and a mechanism (e.g., dependency override) to inject it into the app.
@pytest.fixture(scope="module")
def client(db):
    """Overrides the dependency and yields the TestClient."""
    from src.core.database import get_db
    
    # Dependency override function
    def override_get_db():
        try:
            yield db
        finally:
            db.close()

    # Apply the override
    app.dependency_overrides[get_db] = override_get_db
    
    # Yield the TestClient
    with TestClient(app) as c:
        yield c
    
    # Clean up the override
    app.dependency_overrides.clear()

# 3. Fixture to seed the database with consistent data
@pytest.fixture
def seeded_db(db: Session):
    """Seeds the database with data and ETL Run metadata for testing."""
    
    # Data for /data endpoint tests (P0.2)
    test_data = [
        CryptoData(symbol="BTC", price_usd=60000.0, market_cap=1.2e12, source="coinpaprika_api", timestamp=datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)),
        CryptoData(symbol="ETH", price_usd=4000.0, market_cap=4.8e11, source="coinpaprika_api", timestamp=datetime(2025, 1, 1, 10, 5, 0, tzinfo=timezone.utc)),
        CryptoData(symbol="BTC", price_usd=59500.0, market_cap=1.1e12, source="csv_file", timestamp=datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)),
        CryptoData(symbol="DOT", price_usd=15.0, market_cap=1.5e10, source="coincap_api", timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)),
    ]
    db.add_all(test_data)
    
    # Metadata for /health and /stats endpoint tests (P0.2, P1.3)
    db.add_all([
        ETLRun(status="SUCCESS", end_time=datetime.now(timezone.utc) - timedelta(hours=1), duration_ms=5000, records_processed=200, run_id="success-1"),
        ETLRun(status="FAILURE", end_time=datetime.now(timezone.utc) - timedelta(minutes=10), duration_ms=1000, error_message="Rate limit", run_id="failure-1"),
    ])

    db.commit()
    return db

# --- P1.4: API Endpoint Tests ---
def test_health_check_db_connected(client: TestClient, db: Session):
    """Tests that the /health endpoint correctly reports DB connectivity."""
    
    # The database connection is active via the fixture injection
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert response.json()["database"] == "connected"

@patch('src.core.database.get_db_status', return_value=False)
def test_health_check_db_disconnected(mock_db_status, client: TestClient, db: Session):
    """Tests the /health endpoint when DB connectivity fails."""
    
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy" # Overall service can still be "healthy"
    assert response.json()["database"] == "disconnected"
    
    # Note: You should also check for ETL last-run status in the real implementation!

def test_get_data_pagination(client: TestClient, seeded_db: Session):
    """Tests P0.2 pagination (limit=2, offset=2)."""
    
    # Total records are 4. Fetching the last 2.
    response = client.get("/data?limit=2&offset=2")
    data = response.json()
    
    assert response.status_code == 200
    assert data["metadata"]["total"] == 4
    assert len(data["data"]) == 2 # Should return 2 records
    
    # Verify the offset is applied (should return the 3rd and 4th records)
    assert data["data"][0]["symbol"] == "BTC" # The second BTC record (59500)
    assert data["data"][1]["symbol"] == "DOT"


def test_get_data_filtering_by_symbol(client: TestClient, seeded_db: Session):
    """Tests P0.2 filtering by symbol."""
    
    response = client.get("/data?symbol=BTC")
    data = response.json()
    
    assert response.status_code == 200
    assert data["metadata"]["total"] == 2 # Only two BTC records exist
    assert len(data["data"]) == 2
    
    # Verify all returned records are BTC
    assert all(d["symbol"] == "BTC" for d in data["data"])

def test_get_stats_endpoint(client: TestClient, seeded_db: Session):
    """Tests P1.3 requirement: Expose ETL summaries."""
    
    response = client.get("/stats")
    stats = response.json()
    
    assert response.status_code == 200
    
    # 1. Total Records (P1.3)
    assert stats["total_records_in_db"] == 4
    
    # 2. Source Aggregation (P1.3)
    assert stats["records_by_source"]["coinpaprika_api"] == 2
    assert stats["records_by_source"]["csv_file"] == 1
    assert stats["records_by_source"]["coincap_api"] == 1
    
    # 3. Last Successful Run (P1.3)
    success_summary = stats["etl_summary"]["last_successful_run"]
    assert success_summary["timestamp"] is not None
    assert success_summary["duration_ms"] == 5000
    assert success_summary["records_processed"] == 200
    
    # 4. Last Failed Run (P1.3)
    failure_summary = stats["etl_summary"]["last_failed_run"]
    assert failure_summary["timestamp"] is not None
    assert failure_summary["error_message"] == "Rate limit"
    
    # 5. Total Run History (P1.3)
    assert stats["etl_summary"]["total_run_history_count"] == 2