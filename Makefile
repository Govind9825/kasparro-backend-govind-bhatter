# Makefile for Kasparro ETL

# P0.3 Requirement: Run the system
up:
	docker-compose up --build -d

# P0.3 Requirement: Stop the system
down:
	docker-compose down

# Helper to view logs
logs:
	docker-compose logs -f backend

# Helper to run the ETL manually (via docker exec)
run-etl:
	docker-compose exec backend python -c "from src.api.routes import run_etl_job; from src.core.database import SessionLocal; print(run_etl_job(SessionLocal()))"

# Helper to run tests (P0.4 Requirement)
test:
	docker-compose exec backend pytest