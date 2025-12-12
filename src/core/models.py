from sqlalchemy import Column, Integer, String, Float, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class CryptoData(Base):
    __tablename__ = "unified_crypto_data"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    price_usd = Column(Float)
    market_cap = Column(String) # Storing as string to handle massive numbers safely, or use BigInteger
    timestamp = Column(DateTime)
    source = Column(String)

    # Constraint: Don't allow duplicate entries for the same coin+time+source
    __table_args__ = (
        UniqueConstraint('symbol', 'timestamp', 'source', name='uix_symbol_time_source'),
    )