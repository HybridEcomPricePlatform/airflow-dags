import json
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.hooks.bigtable import BigtableHook

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 1,
}

def read_mongo(**context):
    """
    Extracts all documents from the price_events collection in MongoDB.
    """
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.get_database(os.getenv('MONGO_DATABASE', 'price_db'))
    collection = db.get_collection(os.getenv('MONGO_COLLECTION', 'price_events'))
    
    documents = list(collection.find({}, {'_id': 0}))
    
    # Save documents locally to avoid XCom size limits
    temp_file = '/tmp/mongo_export.json'
    with open(temp_file, 'w') as f:
        json.dump(documents, f, default=str)
        
    context['ti'].xcom_push(key='mongo_count', value=len(documents))
    return temp_file

def write_bigtable(**context):
    """
    Writes the extracted MongoDB documents to Bigtable.
    """
    ti = context['ti']
    temp_file = ti.xcom_pull(task_ids='read_mongo')
    
    with open(temp_file, 'r') as f:
        documents = json.load(f)
        
    hook = BigtableHook(
        gcp_conn_id='google_cloud_default',
        project_id='price-intel-prod',
        instance_id='price-intelligence'
    )
    client = hook.get_client()
    instance = client.instance('price-intelligence')
    table = instance.table('product_prices')
    
    rows = []
    for e in documents:
        product_id = e.get('product_id', '')
        scraped_at = e.get('scraped_at', '')
        row_key = f"{product_id}#{scraped_at}".encode('utf-8')
        
        row = table.direct_row(row_key)
        
        # price_cf
        row.set_cell('price_cf', b'price', str(e.get('price', '')).encode('utf-8'))
        row.set_cell('price_cf', b'currency', str(e.get('currency', '')).encode('utf-8'))
        row.set_cell('price_cf', b'availability', str(e.get('availability', '')).encode('utf-8'))
        
        # metadata_cf
        product_name = e.get('product_name', e.get('name', ''))
        source_url = e.get('source_url', e.get('url', ''))
        
        row.set_cell('metadata_cf', b'product_name', str(product_name).encode('utf-8'))
        row.set_cell('metadata_cf', b'site_name', str(e.get('site_name', '')).encode('utf-8'))
        row.set_cell('metadata_cf', b'category', str(e.get('category', '')).encode('utf-8'))
        row.set_cell('metadata_cf', b'source_url', str(source_url).encode('utf-8'))
        row.set_cell('metadata_cf', b'image_url', str(e.get('image_url', '')).encode('utf-8'))
        row.set_cell('metadata_cf', b'schema_version', str(e.get('schema_version', '1.0')).encode('utf-8'))
        
        rows.append(row)
        
    # Write events batch
    status = table.mutate_rows(rows)
    success_count = sum(1 for s in status if s.code == 0)
    
    ti.xcom_push(key='bigtable_count', value=success_count)

def validate_count(**context):
    """
    Validates that all documents were migrated.
    """
    ti = context['ti']
    mongo_count = ti.xcom_pull(task_ids='read_mongo', key='mongo_count')
    bigtable_count = ti.xcom_pull(task_ids='write_bigtable', key='bigtable_count')
    
    print(f"MongoDB documents: {mongo_count}")
    print(f"Bigtable rows written: {bigtable_count}")
    
    if mongo_count != bigtable_count:
        raise ValueError(f"Mismatch! Mongo: {mongo_count}, Bigtable: {bigtable_count}")
    else:
        print("Migration validation successful!")

with DAG(
    dag_id='mongo_to_bigtable',
    default_args=default_args,
    description='One-shot migration from MongoDB to Bigtable',
    schedule_interval=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['migration', 'mongodb', 'bigtable'],
) as dag:

    read_mongo_task = PythonOperator(
        task_id='read_mongo',
        python_callable=read_mongo,
    )

    write_bigtable_task = PythonOperator(
        task_id='write_bigtable',
        python_callable=write_bigtable,
    )

    validate_count_task = PythonOperator(
        task_id='validate_count',
        python_callable=validate_count,
    )

    read_mongo_task >> write_bigtable_task >> validate_count_task
