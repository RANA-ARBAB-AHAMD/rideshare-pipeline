from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import step functions from scripts
from scripts.extract_data import generate_fake_data
from scripts.transform_data import transform_data
from scripts.aggregate_data import aggregate_data
from scripts.load_data import load_to_postgres

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='rideshare_etl_dag',
    default_args=default_args,
    description='Medallion-style ETL pipeline for rideshare data',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Bronze: Extract raw data
    bronze_task = PythonOperator(
        task_id='bronze_extract_raw',
        python_callable=generate_fake_data
    )

    # Silver: Clean / transform data
    silver_task = PythonOperator(
        task_id='silver_clean_data',
        python_callable=transform_data
    )

    # Gold: Aggregate analytics
    gold_task = PythonOperator(
        task_id='gold_aggregate_data',
        python_callable=aggregate_data
    )

    # Load: Load into PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    # Pipeline flow:
    bronze_task >> silver_task >> gold_task >> load_task
