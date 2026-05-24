from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    schedule_interval='0 */6 * * *',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['transformation', 'dbt', 'bigquery'],
) as dag:

    # Wait for the scrape_and_export DAG to finish its export_to_gcs task
    wait_for_scraping = ExternalTaskSensor(
        task_id='wait_for_scraping',
        external_dag_id='scrape_and_export',
        external_task_id='export_to_gcs',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=3600,
    )

    # We install dbt-bigquery quickly on the fly and run dbt
    # using the profiles.yml located in the dbt_price directory
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='pip install --user dbt-bigquery && cd /home/sara/price-intelligence/dbt_price && ~/.local/bin/dbt run --profiles-dir .',
    )

    wait_for_scraping >> run_dbt
