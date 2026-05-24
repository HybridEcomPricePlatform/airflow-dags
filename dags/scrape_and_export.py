import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor

PRICE_INTEL_HOME = os.getenv('PRICE_INTEL_HOME', '/home/sara/price-intelligence')

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
        bash_command='cd $PRICE_INTEL_HOME/scrapers && ~/.local/bin/scrapy crawl jumia -s CLOSESPIDER_ITEMCOUNT=500',
        env={'PRICE_INTEL_HOME': PRICE_INTEL_HOME},
    )

    scrape_ep = BashOperator(
        task_id='scrape_ep',
        bash_command='cd $PRICE_INTEL_HOME/scrapers && ~/.local/bin/scrapy crawl electroplanet -s CLOSESPIDER_ITEMCOUNT=200',
        env={'PRICE_INTEL_HOME': PRICE_INTEL_HOME},
    )

    export_to_gcs = BashOperator(
        task_id='export_to_gcs',
        bash_command='cd $PRICE_INTEL_HOME && python3 scrapers/utils/export_to_gcs.py',
        env={'PRICE_INTEL_HOME': PRICE_INTEL_HOME},
    )

    wait_nifi_completion = HttpSensor(
        task_id='wait_nifi_completion',
        http_conn_id='nifi_api',
        endpoint='nifi-api/flow/process-groups/root/status',
        # Assuming the response has a queuedCount representing Kafka messages to Bigtable
        response_check=lambda response: response.json().get('processGroupStatus', {}).get('aggregateSnapshot', {}).get('queuedCount') == 0,
        poke_interval=60,
        timeout=3600,
    )

    [scrape_jumia, scrape_ep] >> export_to_gcs >> wait_nifi_completion
