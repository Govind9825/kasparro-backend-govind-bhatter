# ------------------------------------------------------------------------
# Stage 1: Builder - Installs Dependencies and compiles packages
# ------------------------------------------------------------------------
FROM python:3.10-slim as builder

# Set working directory inside the container
WORKDIR /app

# Install dependencies needed for Pandas, PostgreSQL client (via psycopg2)
# gcc and libc-dev are necessary for compiling many Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc-dev python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------------------------------
# Stage 2: Final Image - Minimal runtime environment
# ------------------------------------------------------------------------
FROM python:3.10-slim

WORKDIR /app

# Install PostgreSQL client tools (CRUCIAL FIX for 'pg_isready' in entrypoint.sh)
# The client tools are small and essential for the P0.3 health check
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy runtime packages from the builder stage
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy application code and necessary files
COPY src ./src
COPY data ./data
COPY entrypoint.sh .

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# P0.3 Requirement: Set the entrypoint to the custom script
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (runs uvicorn, which is called by the entrypoint)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]