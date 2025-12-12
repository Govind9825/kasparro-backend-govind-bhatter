from sqlalchemy.orm import Session
from src.core.models import CryptoData

def load_data(db: Session, data: list):
    """
    Inserts data into Postgres.
    Prevents duplicates within the batch AND against the database.
    """
    if not data:
        return 0
        
    count = 0
    # 1. Memory Cache to track what we have seen in this specific run
    seen_in_batch = set()

    for record in data:
        # Create a unique signature for this record (Symbol + Date + Source)
        record_key = (record['symbol'], record['timestamp'], record['source'])

        # CHECK 1: Have we already seen this in the CURRENT list?
        if record_key in seen_in_batch:
            continue
        
        # CHECK 2: Does it exist in the DATABASE?
        existing = db.query(CryptoData).filter(
            CryptoData.symbol == record['symbol'],
            CryptoData.timestamp == record['timestamp'],
            CryptoData.source == record['source']
        ).first()

        if not existing:
            db_obj = CryptoData(**record)
            db.add(db_obj)
            seen_in_batch.add(record_key) # Mark as seen
            count += 1
            
    try:
        db.commit()
        return count
    except Exception as e:
        db.rollback()
        print(f"Database Insert Error: {e}")
        return 0