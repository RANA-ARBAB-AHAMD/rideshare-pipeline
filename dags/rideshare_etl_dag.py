from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import from local scripts package inside dags folder
from scripts.extract_data import generate_fake_data
from scripts.transform_data import transform_data
from scripts.load_data import load_to_postgres

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='rideshare_etl_dag',
    default_args=default_args,
    description='ETL pipeline for rideshare data',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=generate_fake_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    extract_task >> transform_task >> load_task
