from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator




def print_hello():
    print("✅ Rideshare pipeline is running!")


default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rideshare_test_dag',
    default_args=default_args,
    description='Test DAG for rideshare pipeline',
    schedule='@daily',  
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )
