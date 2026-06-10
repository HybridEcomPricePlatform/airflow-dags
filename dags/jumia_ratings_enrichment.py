import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

WSL_HOST = '172.30.148.175'
WSL_USER = 'sara'
SSH_KEY = '/home/airflow/.ssh/airflow_rsa'

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='jumia_ratings_enrichment',
    default_args=default_args,
    description='Weekly enrichment of Jumia product ratings and review counts',
    schedule_interval='0 2 * * 1',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    tags=['enrichment', 'ratings', 'jumia'],
) as dag:

    enrich_ratings = BashOperator(
        task_id='enrich_ratings',
        bash_command=(
            f'ssh -i {SSH_KEY} '
            f'-o StrictHostKeyChecking=no -T '
            f'{WSL_USER}@{WSL_HOST} '
            f'"cd ~/price-intelligence && '
            f'MONGO_URI=mongodb://price_app:AppPass2026!@localhost:27017/price_db '
            f'MONGO_DATABASE=price_db '
            f'MONGO_COLLECTION=price_events '
            f'KAFKA_BOOTSTRAP_SERVERS=localhost:9092 '
            f'venv/bin/scrapy crawl jumia_ratings '
            f'--set=SCRAPY_SETTINGS_MODULE=scrapers.settings 2>&1"'
        ),
    )
