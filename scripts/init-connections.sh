#!/bin/bash
# init-connections.sh
# Initialise les connexions Airflow automatiquement

echo "Initialisation des connexions Airflow..."

# Connexion Kafka
airflow connections add 'kafka_default' \
    --conn-type 'kafka' \
    --conn-extra '{"bootstrap.servers": "kafka:9092", "group.id": "airflow-consumer-group", "auto.offset.reset": "earliest", "enable.auto.commit": false}'

# Connexion MongoDB
airflow connections add 'mongo_default' \
    --conn-type 'generic' \
    --conn-host 'mongo' \
    --conn-port '27017' \
    --conn-schema 'price_db'

# Importer les variables
airflow variables import /opt/airflow/config/airflow_variables.json

echo "✅ Connexions et variables initialisées"