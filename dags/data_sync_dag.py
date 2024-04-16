# data_sync_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_sync_script import main

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 13),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG(
    'data_sync_dag',
    default_args=default_args,
    description='A DAG to synchronize data from web sources to a database',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the task using PythonOperator
synchronize_data = PythonOperator(
    task_id='synchronize_data',
    python_callable=main,
    dag=dag,
)

# Task sequence
synchronize_data
