#!/bin/bash
set -e

# Wait for DB (Ensures tables can be created)
echo "Waiting for PostgreSQL to become fully available..."
while ! pg_isready -h db -p 5432 -U ${POSTGRES_USER}; do
  sleep 1
done
echo "PostgreSQL is ready."

# 1. Initialize the database schema (Creates CryptoData and ETLRun tables)
echo "Initializing database schema..."
python -c "from src.core.database import init_db; init_db()"

# 2. Start the ETL job in the background (P0.3 Auto-Start Requirement)
# This executes one initial ETL run to populate data immediately.
echo "Starting initial ETL job..."
python -c "from src.api.routes import run_etl_job; from src.core.database import get_db; db_session = next(get_db()); run_etl_job(db_session)" &

# 3. Start the API server in the foreground (Docker's main process)
echo "Starting Uvicorn API server..."
exec uvicorn src.main:app --host 0.0.0.0 --port 8000