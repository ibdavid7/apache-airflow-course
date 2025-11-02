from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
DEFAULT_START = pendulum.datetime(2024, 1, 1, tz="UTC")


default_args = {
    'owner': 'loonycord',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_func():
    print("The Simples possible Python operator")
    
with DAG(
    dag_id = 'execute_simple_python_operators',
    description ='Python Operator Example in DAGs',
    default_args = default_args,
    start_date = DEFAULT_START,
    schedule = '@daily',
    catchup = False,
    tags = ['example', 'python_operator','simple']
) as dag:

    task = PythonOperator(
        task_id='python_task',
        python_callable=print_func
    )

    task