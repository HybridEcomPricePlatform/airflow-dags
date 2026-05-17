FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

# On installe UNIQUEMENT les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# SUPPRIMEZ les lignes COPY dags/ et COPY config/
# Elles seront gérées par le volume dans docker-compose
