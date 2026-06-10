from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

WSL_HOST = os.getenv('WSL_HOST', '172.30.148.175')
WSL_USER = 'sara'
SSH_KEY = '/home/airflow/.ssh/airflow_rsa'

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_transform',
    default_args=default_args,
    description='Runs dbt transformations after scraping is done',
    schedule_interval='0 */12 * * *',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['transformation', 'dbt', 'bigquery'],
) as dag:

    wait_for_scraping = ExternalTaskSensor(
        task_id='wait_for_scraping',
        external_dag_id='scrape_and_export',
        external_task_id='export_to_gcs',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        poke_interval=60,
        timeout=3600,
    )

    # dbt runs via SSH on WSL host where credentials and venv are available
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=(
            f'ssh -i {SSH_KEY} '
            f'-o StrictHostKeyChecking=no -T '
            f'{WSL_USER}@{WSL_HOST} '
            f'"cd ~/price-intelligence/dbt_price && '
            f'source ~/price-intelligence/venv/bin/activate && '
            f'dbt run --profiles-dir . 2>&1"'
        ),
    )

    wait_for_scraping >> run_dbt
