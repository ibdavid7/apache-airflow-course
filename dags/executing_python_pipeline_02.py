import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
   'owner': 'loonycorn'
}

DEFAULT_START = datetime(2024, 1, 1)

def read_csv_file():
    df = pd.read_csv('/workspaces/apache-airflow-course/datasets/insurance.csv')

    print(df)

    return df.to_json()

def remove_null_values(ti, **kwargs):

    print("kwargs: ", kwargs)

    df_json = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(df_json)

    df_cleaned = df.dropna()

    print(df_cleaned)

    return df_cleaned.to_json()

def group_by_smoker(ti, **kwargs):
    df_json = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(df_json)
    
    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'charges':'mean'
    }).reset_index()
    
    smoker_df.to_csv(
        '/workspaces/apache-airflow-course/output/grouped_by_smoker.csv', index=False
    )


def group_by_region(ti, **kwargs):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean', 
        'charges': 'mean'
    }).reset_index()
    

    region_df.to_csv(
        '/workspaces/apache-airflow-course/output/grouped_by_region.csv', index=False)



with DAG(
    dag_id = 'python_pipeline_2',
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

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    group_by_smoker = PythonOperator(
        task_id='group_by_smoker',
        python_callable=group_by_smoker
    )

    group_by_region = PythonOperator(
        task_id='group_by_region',
        python_callable=group_by_region
    )

read_csv_file >> remove_null_values >> [group_by_smoker, group_by_region]