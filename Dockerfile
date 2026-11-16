# ==============================================================================
# Runtime image: Apache Airflow 3.1.8 + ETL / GX / Kafka / Streamlit dependencies
# Shared by the `airflow`, `stream_consumer` and `realtime_dashboard` services.
# ==============================================================================
FROM apache/airflow:3.1.8-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow itself comes from the base image; it is pinned again here so pip never
# upgrades/downgrades it while resolving the project dependencies.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/project:/opt/airflow \
    PYTHONUNBUFFERED=1

WORKDIR /opt/project
EXPOSE 8080 8501
