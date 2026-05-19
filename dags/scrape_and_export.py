import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

# Add the project directory to sys.path to be able to import the utility
sys.path.append('/home/sara/price-intelligence')
from scrapers.utils.export_to_gcs import export_recent_files

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
        bash_command='cd /home/sara/price-intelligence && scrapy crawl jumia -s CLOSESPIDER_ITEMCOUNT=500',
    )

    scrape_ep = BashOperator(
        task_id='scrape_ep',
        bash_command='cd /home/sara/price-intelligence && scrapy crawl electroplanet -s CLOSESPIDER_ITEMCOUNT=200',
    )

    export_to_gcs = PythonOperator(
        task_id='export_to_gcs',
        python_callable=export_recent_files,
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
