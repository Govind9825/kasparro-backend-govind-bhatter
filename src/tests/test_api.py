from fastapi.testclient import TestClient
from src.main import app
from src.schemas.crypto import UnifiedCryptoData
import pytest

client = TestClient(app)

# --- REQUIREMENT: Test at least one API endpoint ---
def test_health_endpoint():
    """Verifies the health check returns 200 and correct structure."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "database" in data

def test_data_endpoint_pagination():
    """Verifies that pagination limits work."""
    response = client.get("/data?limit=2&offset=0")
    assert response.status_code == 200
    data = response.json()
    # We expect exactly 2 records in the 'data' list
    assert len(data["data"]) == 2
    assert data["metadata"]["page_limit"] == 2

# --- REQUIREMENT: Test ETL transformation logic ---
def test_pydantic_transformation():
    """Tests that our schema correctly capitalizes symbols."""
    raw_data = {
        "symbol": "btc",  # Lowercase input
        "price_usd": 50000.0,
        "market_cap": 1000000,
        "timestamp": "2023-10-01T12:00:00",
        "source": "test_source"
    }
    # Create the model
    model = UnifiedCryptoData(**raw_data)
    
    # Assert it auto-capitalized to 'BTC'
    assert model.symbol == "BTC" 

# --- REQUIREMENT: Test at least one failure scenario ---
def test_negative_price_validation():
    """Tests that negative prices are rejected."""
    bad_data = {
        "symbol": "BTC",
        "price_usd": -100.0, # Invalid negative price
        "market_cap": 1000,
        "timestamp": "2023-10-01T12:00:00",
        "source": "test"
    }
    
    # Expect a Validation Error
    with pytest.raises(ValueError):
        UnifiedCryptoData(**bad_data)