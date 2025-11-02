from datetime import datetime, timedelta
import pandas as pd
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.standard.sensors.filesystem import FileSensor

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

default_args = {
    'owner': 'loonycorn',
    'start_date': START_DATE,
    'schedule_interval': '@once',
}

with DAG(
    dag_id='simple_file_sensor_01', 
    description='A simple file sensor DAG',
    default_args=default_args, 
    tags=['sensor', 'file sensor']
    ) as dag:

    # wait_for_file = FileSensor(
    #     task_id='wait_for_file',
    #     filepath=Variable.get("sensor_file_path", default_var="/tmp/data.csv"),
    #     poke_interval=30,
    #     timeout=600
    # )

    checking_for_file = FileSensor(
        task_id='checking_for_file',
        filepath=Variable.get("sensor_file_path", default_var="/workspaces/apache-airflow-course/tmp/laptops.csv"),
        poke_interval=10,
        timeout=60*10
    )

    checking_for_file

