import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor

PRICE_INTEL_HOME = os.getenv('PRICE_INTEL_HOME', '/home/sara/price-intelligence')

# Variables d'env héritées du container — passées explicitement parce que
# env= dans BashOperator remplace os.environ au lieu de le compléter.
SHARED_ENV = {
    'PRICE_INTEL_HOME': PRICE_INTEL_HOME,
    'PYTHONPATH': PRICE_INTEL_HOME,
    'MONGO_HOST': os.getenv('MONGO_HOST', 'price-mongodb'),
    'MONGO_URI': os.getenv('MONGO_URI', 'mongodb://price_app:AppPass2026!@price-mongodb:27017/price_db'),
    'MONGO_DATABASE': os.getenv('MONGO_DATABASE', 'price_db'),
    'MONGO_COLLECTION': os.getenv('MONGO_COLLECTION', 'price_events'),
    'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'price-kafka:29092'),
    'KAFKA_TOPIC': os.getenv('KAFKA_TOPIC', 'price-updates'),
    'KAFKA_DLQ_TOPIC': os.getenv('KAFKA_DLQ_TOPIC', 'price-updates-dlq'),
    'SCRAPY_FEED_DIR': os.getenv('SCRAPY_FEED_DIR', '/opt/airflow/storage'),
    'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS', ''),
    'GOOGLE_CLOUD_PROJECT': os.getenv('GOOGLE_CLOUD_PROJECT', 'price-intel-prod'),
    'GCS_BUCKET': os.getenv('GCS_BUCKET', 'price-raw-data-price-intel-prod'),
}

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='scrape_and_export',
    default_args=default_args,
    description='Scrapes Jumia and Electroplanet then exports to GCS',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['scraping', 'ingestion'],
) as dag:

    scrape_jumia = BashOperator(
        task_id='scrape_jumia',
        bash_command=(
            'cd $PRICE_INTEL_HOME && '
            '~/.local/bin/scrapy crawl jumia '
            '-s CLOSESPIDER_ITEMCOUNT=500 '
            '--set=SCRAPY_SETTINGS_MODULE=scrapers.settings'
        ),
        env=SHARED_ENV,
    )

    scrape_ep = BashOperator(
        task_id='scrape_ep',
        bash_command=(
            'cd $PRICE_INTEL_HOME && '
            '~/.local/bin/scrapy crawl electroplanet '
            '-s CLOSESPIDER_ITEMCOUNT=200 '
            '--set=SCRAPY_SETTINGS_MODULE=scrapers.settings'
        ),
        env=SHARED_ENV,
    )

    export_to_gcs = BashOperator(
        task_id='export_to_gcs',
        bash_command='cd $PRICE_INTEL_HOME && python3 scrapers/utils/export_to_gcs.py',
        env=SHARED_ENV,
    )

    wait_nifi_completion = HttpSensor(
        task_id='wait_nifi_completion',
        http_conn_id='nifi_api',
        endpoint='nifi-api/flow/process-groups/root/status',
        response_check=lambda response: response.json().get('processGroupStatus', {}).get('aggregateSnapshot', {}).get('queuedCount') == 0,
        poke_interval=60,
        timeout=3600,
    )

    [scrape_jumia, scrape_ep] >> export_to_gcs >> wait_nifi_completion
