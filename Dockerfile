# ==============================================================================
# Dockerfile: Final ETL Delivery (Airflow, Streaming & Analytical Runtime)
# ==============================================================================
FROM python:3.11-slim

WORKDIR /opt/project

# Install essential system build dependencies and PostgreSQL client libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    git \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and directories
COPY . .

# Environment configuration
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/project:/opt/airflow
ENV PYTHONUNBUFFERED=1

EXPOSE 8080 8501

CMD ["python", "src/run_pipeline.py"]
