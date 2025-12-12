from sqlalchemy import Column, Integer, String, Float, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSON

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

class ETLRun(Base):
    __tablename__ = "etl_runs"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, unique=True, index=True, default=lambda: str(uuid.uuid4()))
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(String, default="RUNNING") # SUCCESS, FAILURE
    duration_ms = Column(Integer, nullable=True) # Duration in milliseconds
    records_processed = Column(Integer, default=0)
    error_message = Column(String, nullable=True)
    # P1.2/P2.2 requirement - for storing checkpoint data if needed
    metadata_json = Column(JSON, nullable=True)