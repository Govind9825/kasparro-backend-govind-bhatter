from fastapi import FastAPI
from contextlib import asynccontextmanager
from src.core.database import init_db
from src.api.routes import router

# NEW WAY: Define startup logic in a lifespan function
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load logic (Startup)
    init_db()
    yield
    # Clean up logic (Shutdown) - none needed for now

# Pass lifespan to the app
app = FastAPI(title="Kasparro ETL System", lifespan=lifespan)

# Include routes
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "System is running. Go to /docs to test endpoints."}