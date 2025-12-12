from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.core.config import settings
from src.core.models import Base # Import the model we just made

engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Creates tables if they don't exist."""
    Base.metadata.create_all(bind=engine)

def get_db():
    """Dependency for API endpoints to get a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_status():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"DB Error: {e}")
        return False