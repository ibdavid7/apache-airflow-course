import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
   'owner': 'loonycorn'
}

DEFAULT_START = datetime(2024, 1, 1)

def read_csv_file():
    df = pd.read_csv('/workspaces/apache-airflow-course/datasets/insurance.csv')

    print(df)

    return df.to_json()


with DAG(
    dag_id = 'python_pipeline_1',
    description='Running a Python pipeline',
    default_args=default_args,
    start_date=DEFAULT_START,
    schedule='@once',
    tags = ['python', 'transform', 'pipeline']
) as dag:
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

read_csv_file