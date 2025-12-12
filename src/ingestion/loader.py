from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict
from src.core.models import CryptoData # Import your SQLAlchemy model

def load_data(db: Session, data: List[Dict]) -> int:
    """
    Performs an Idempotent Bulk UPSERT (Update or Insert) operation (P1.2).

    Data is considered unique based on the composite key: (symbol, timestamp, source).
    If a record with the same key exists, its price/market_cap are updated (UPSERT).
    
    Args:
        db: The SQLAlchemy database session.
        data: A list of dictionaries, where each dict is a row conforming to the UnifiedCryptoData schema.

    Returns:
        The total number of records successfully processed (inserted or updated).
    """
    if not data:
        return 0

    try:
        # 1. Define the unique composite key for conflict resolution.
        # This MUST match the UniqueConstraint defined on your CryptoData model:
        conflict_target = [CryptoData.symbol, CryptoData.source, CryptoData.timestamp]
        
        # 2. Prepare the bulk INSERT statement
        insert_stmt = insert(CryptoData).values(data)
        
        # 3. Define what to do ON CONFLICT (the Idempotency core)
        # If a conflict on the key is detected, UPDATE the price and market_cap.
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_target,
            set_={
                # Use insert_stmt.excluded to refer to the data trying to be inserted
                'price_usd': insert_stmt.excluded.price_usd,
                'market_cap': insert_stmt.excluded.market_cap,
                # Add any other columns you want to update on conflict
            }
        )

        # 4. Execute and Commit
        # We execute the bulk UPSERT in a single, fast database call.
        db.execute(upsert_stmt)
        db.commit()
        
        # The total number of records processed is the size of the input list.
        print(f"LOAD: Successfully processed {len(data)} records via Idempotent UPSERT.")
        return len(data)

    except Exception as e:
        db.rollback()
        print(f"LOAD CRITICAL ERROR: Idempotent bulk UPSERT failed: {e}")
        # Reraise the exception to be caught by the ETLRun failure logic in etl_service.py
        raise