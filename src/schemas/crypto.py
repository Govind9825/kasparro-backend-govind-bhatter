from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class UnifiedCryptoData(BaseModel):
    symbol: str
    price_usd: float = Field(ge=0) # Price must be positive
    market_cap: int = Field(ge=0)
    timestamp: datetime # Pydantic will auto-parse ISO strings vs Date strings
    source: str

    @field_validator('symbol')
    def uppercase_symbol(cls, v):
        return v.upper() # Normalize "btc" to "BTC"