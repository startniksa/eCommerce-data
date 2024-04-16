import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Set up logging
logger = logging.getLogger("airflow.task")

def my_simple_function():
    logger.info("This is a log message from my simple function")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_python_function',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example']
)

task = PythonOperator(
    task_id='log_message_task',
    python_callable=my_simple_function,
    dag=dag,
)
