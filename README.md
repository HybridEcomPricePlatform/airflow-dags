# ✈️ Airflow DAGs — Price Intelligence Platform

DAGs Airflow pour l'orchestration batch de la plateforme.

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `scrape_and_export` | `0 */6 * *` | Scraping Jumia + Electroplanet → GCS |
| `mongo_to_bigtable` | Manuel | Migration MongoDB → Bigtable |
| `dbt_transform` | `0 */6 * *` | Transformations dbt → BigQuery |

## Installation

```bash
# Builder l'image Airflow
docker compose build airflow-webserver
docker compose up -d airflow-webserver airflow-scheduler airflow-worker
```

## Connexions requises

| Connection ID | Type | Description |
|---------------|------|-------------|
| `mongo_default` | Generic | MongoDB |
| `google_cloud_default` | Google Cloud | GCP credentials |
| `nifi_api` | HTTP | NiFi REST API |

## Variables Airflow

```json
{
  "KAFKA_BROKER": "kafka:9092",
  "KAFKA_TOPIC": "price-updates",
  "MONGO_URI": "mongodb://price_app:AppPass2026!@price-mongodb:27017/price_db",
  "GOOGLE_CLOUD_PROJECT": "price-intel-prod",
  "GCS_BUCKET": "price-raw-data-price-intel-prod"
}
```

## CI/CD

GitHub Actions — `.github/workflows/airflow-ci.yml`

Jobs :
- `validate-dags` — vérifie les imports DAGs
- `lint-python` — flake8