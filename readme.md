\# 🚀 Kasparro Backend \& ETL System  
\#\#\# P0 + P1 Implementation

This project implements a production-grade \*\*ETL pipeline\*\* and \*\*backend API\*\* capable of ingesting, transforming, normalizing, and serving cryptocurrency data from multiple heterogeneous sources.

It fulfills all required tasks for \*\*P0 (Foundation Layer)\*\* and \*\*P1 (Growth Layer)\*\*.

---

\#\#\# 📘 Table of Contents  
\- Overview  
\- Architecture  
\- Features  
\- Local Setup  
\- API Endpoints  
\- Cloud Deployment  
\- Project Structure  

---

\# 🌟 Overview

The Kasparro system processes crypto data from \*\*three ingestion sources\*\*:

1. CoinPaprika API  
2. CoinCap API  
3. Legacy CSV file  

All incoming data is normalized into a \*\*single unified schema\*\* and stored in PostgreSQL using idempotent UPSERT logic.

---

\# 🧱 Architecture

\| Component \| Technology \| Purpose \|  
\|----------\|------------\|---------\|  
\| Backend API \| FastAPI \| High-performance Python API \|  
\| ETL Pipeline \| Python \| Ingestion, transformation \|  
\| Database \| PostgreSQL 15 \| UPSERT + storage \|  
\| Containers \| Docker, Docker Compose \| Reproducible runtime \|  
\| Validation \| Pydantic \| Schema enforcement \|  
\| Testing \| Pytest \| ETL + API testing \|  

---

\# 🚀 Features (P0 + P1)

\#\# ✔️ P0 Features  
\- Unified schema  
\- ETL ingestion from at least one source  
\- Basic FastAPI service  
\- /health and /data endpoints  
\- Dockerized environment  

\#\# ✔️ P1 Features  
\- Ingests data from 3 sources (CSV + 2 APIs)  
\- Incremental ingestion via etl\_runs checkpointing  
\- Idempotent UPSERT logic  
\- /stats endpoint for ETL metadata  
\- /run-etl endpoint for on-demand execution  
\- Cloud deployment with Azure VM  
\- Hourly ETL scheduling via Azure Cron  
\- Centralized logs via Azure Monitor  

---

\# 💻 Local Setup

\#\# 1. Prerequisites  
Docker  
Docker Compose  
make (optional)

\#\# 2. Clone Repository  
git clone \[YOUR\_REPO\_URL\]  
cd kasparro-backend-govind-bhatter

\#\# 3. Create .env file  
POSTGRES\_USER=kasparro\_user  
POSTGRES\_PASSWORD=kasparro\_password  
POSTGRES\_DB=crypto\_db  
COINPAPRIKA\_API\_KEY=dummy  
COINCAP\_API\_KEY=dummy  

\#\# 4. Run CSV generator  
python generate\_csv.py

\#\# 5. Start services  
make up

Stop:  
make down

---

\# 🌐 API Endpoints

Base URL: http:\/\/localhost:8000

\| Endpoint \| Method \| Description \|  
\|----------\|--------\|-------------\|  
\| /health \| GET \| Returns DB \& ETL status \|  
\| /data \| GET \| Paginated crypto data \|  
\| /stats \| GET \| ETL summary \|  
\| /run-etl \| POST \| Trigger ETL pipeline \|  
\| /docs \| GET \| Swagger UI \|  

---

\# ☁️ Cloud Deployment

Public Endpoint:  
http:\/\/98.70.24.63:8000/health

Scheduled ETL:  
Azure Cloud Scheduler → POST /run-etl hourly

Logs:  
Azure Monitor / Log Analytics

---

\# 📁 Project Structure

kasparro-backend/  
\|\_\_ app/  
\|   \|\_\_ api/  
\|   \|   routes.py  
\|   \|   validators.py  
\|   \|\_\_ etl/  
\|   \|   extract.py  
\|   \|   transform.py  
\|   \|   load.py  
\|   \|   utils.py  
\|   \|\_\_ models/  
\|   \|   schemas.py  
\|   \|\_\_ db/  
\|   \|   connection.py  
\|   \|   migrations.sql  
\|   main.py  
\|\_\_ data/  
\|   coins.csv  
docker-compose.yml  
Dockerfile  
entrypoint.sh  
requirements.txt  
README.md

---

\# ✅ Summary

This repository provides a full ETL + API backend that:  
\- Ingests multi-source crypto data  
\- Validates it via Pydantic  
\- Stores it with PostgreSQL UPSERT idempotency  
\- Exposes APIs for querying  
\- Deploys to Azure VM  
\- Runs scheduled ETL via Azure Cron  

