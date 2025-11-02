from datetime import datetime, timedelta

from airflow import DAG
import pendulum

from airflow.operators.bash import BashOperator

DEFAULT_START = pendulum.datetime(2024, 1, 1, tz="UTC")

default_args = {
    'owner': 'loonycord',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='simple_hello_world',  # Explicitly setting dag_id
    description='A simple hello world DAG',  # Explicitly setting description
    default_args=default_args,  # Explicitly setting default_args
    start_date=DEFAULT_START,  # Explicitly setting start_date
    schedule='@daily',  # Updated to use 'schedule' instead of 'schedule_interval'
    tags=['example', 'hello_world']
) as dag:
    
    task = BashOperator(
        task_id='print_hello_task',
        bash_command='echo "Hello World once again from Airflow!"',
        dag=dag,
)

task