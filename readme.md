# Kasparro Backend & ETL System

## Overview
A production-grade ETL pipeline and REST API that ingests cryptocurrency data from CoinPaprika (API) and legacy CSV files, normalizes it into a unified schema, and stores it in PostgreSQL.

## Architecture
- **Language:** Python 3.10
- **Framework:** FastAPI
- **Database:** PostgreSQL 15
- **Containerization:** Docker & Docker Compose
- **Validation:** Pydantic
- **Deployment:** Azure VM (Ubuntu) + Cron Job

## Features
- **Unified Schema:** Normalizes data from different sources (API vs CSV) into a single `unified_crypto_data` table.
- **Idempotency:** Prevents duplicate records using composite keys.
- **Health Check:** `/health` endpoint monitors DB connectivity.
- **Automated Testing:** Pytest suite covering ETL logic and API endpoints.

## Cloud Deployment
The system is live on Azure.
- **Public URL:** http://98.70.24.63:8000/health
- **Scheduled Job:** Hourly Cron Job.

## How to Run (Local)

### 1. Clone the repository
```bash
git clone [https://github.com/Govind9825/kasparro-backend-govind-bhatter.git](https://github.com/Govind9825/kasparro-backend-govind-bhatter.git)
cd kasparro-backend-govind-bhatter
```

### 2. Configure Environment
Create a `.env` file in the root directory:
```env
COINPAPRIKA_API_KEY=dummy-key-for-assignment
POSTGRES_USER=kasparro_user
POSTGRES_PASSWORD=kasparro_password
POSTGRES_DB=crypto_db
POSTGRES_HOST=db
POSTGRES_PORT=5432
```

### 3. Data Setup (CSV)
Ensure the legacy data file exists at `data/coins.csv`. If missing, generate it:
```bash
python generate_csv.py
```

### 4. Run the System
Build and start the services:
```bash
make up
```

### 5. Test the System
Run the automated test suite:
```bash
make test
```

## API Endpoints
- **Health Check:** `http://localhost:8000/health`
- **Documentation:** `http://localhost:8000/docs`
- **Trigger ETL:** `POST http://localhost:8000/run-etl`