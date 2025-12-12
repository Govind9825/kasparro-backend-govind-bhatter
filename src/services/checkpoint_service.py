from sqlalchemy.orm import Session
from src.core.models import ETLRun # Assuming you import ETLRun here
from typing import Optional
from datetime import datetime, timezone

def get_last_successful_checkpoint(db: Session, source_name: str) -> Optional[datetime]:
    """Retrieves the latest checkpoint timestamp from the last successful run."""
    
    # 1. Find the most recent SUCCESSFUL run record
    last_successful_run = db.query(ETLRun).filter(
        ETLRun.status == "SUCCESS"
    ).order_by(
        ETLRun.end_time.desc()
    ).first()

    if last_successful_run and last_successful_run.metadata_json:
        # 2. Check the metadata_json for the specific source's checkpoint
        checkpoint_str = last_successful_run.metadata_json.get(source_name + "_checkpoint")
        if checkpoint_str:
            # Convert ISO string back to datetime object
            return datetime.fromisoformat(checkpoint_str.replace('Z', '+00:00'))

    # If no success record or no checkpoint for this source, return a default starting time
    # (e.g., a very old date, or None to signify a full load)
    return None