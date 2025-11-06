import pandas as pd

from random import choice

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}

DEFAULT_START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

ORIGINAL_DATA_PATH = '/workspaces/apache-airflow-course/datasets/cars_details.csv'
CLEANED_DATA_PATH = '/workspaces/apache-airflow-course/datasets/cleaned_car_details.csv'
OUTPUT_PATH = '/workspaces/apache-airflow-course/output/{0}.csv'

def read_csv_file():
    df = pd.read_csv(ORIGINAL_DATA_PATH)

    print(df)

    return df.to_json()


def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)

    df = df.dropna()

    dtypes = {
        'car_model_year': int,
        'mileage': int
    }

    df = df.astype(dtypes)

    print(df)

    df.to_csv(CLEANED_DATA_PATH, index=False)

    return df.to_json()


def determine_branch():
    choose = choice([True, False])
    
    if choose:
        return 'save_output_as_csv'
    else:
        return 'save_output_as_table'

def filter_by_new_car(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    filtered_df = df[df['condition'] == 'New']
    
    filtered_df.to_csv(OUTPUT_PATH.format('new_cars'), index=False)


def filter_by_used_car(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    filtered_df = df[df['condition'] == 'Used']
    
    filtered_df.to_csv(OUTPUT_PATH.format('used_cars'), index=False)

def filter_by_certified_car(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')

    df = pd.read_json(json_data)

    filtered_df = df[df['condition'] == 'Certified Pre-Owned']
    
    filtered_df.to_csv(OUTPUT_PATH.format('certified_cars'), index=False)



with DAG(
    dag_id = 'car_data_processing_pipeline_01',
    description = 'Simple data processing pipeline',
    default_args = default_args,
    start_date = DEFAULT_START_DATE,
    schedule = '@once',
    tags = ['python', 'scaling', 'pipeline', 'celery'],
    template_searchpath = '/workspaces/apache-airflow-course/sql_statements'
) as dag:

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    save_output_as_csv = EmptyOperator(
        task_id='save_output_as_csv'
    )

    save_output_as_table = EmptyOperator(
        task_id='save_output_as_table'
    )

    filter_by_new_car = PythonOperator(
        task_id='filter_by_new_car_csv',
        python_callable=filter_by_new_car
    )
    
    filter_by_used_car = PythonOperator(
        task_id='filter_by_used_car_csv',
        python_callable=filter_by_used_car
    )
    
    filter_by_certified_car = PythonOperator(
        task_id='filter_by_certified_car_csv',
        python_callable=filter_by_certified_car
    )

    create_table_new_car = SQLExecuteQueryOperator(
        task_id='create_table_new_car',
        conn_id='postgres_connection',
        sql='create_table_car.sql',
        params={'table_name': 'new_car'}
    )

    create_table_used_car = SQLExecuteQueryOperator(
        task_id='create_table_used_car',
        conn_id='postgres_connection',
        sql='create_table_car.sql',
        params={'table_name': 'used_car'}
    )

    create_table_certified_car = SQLExecuteQueryOperator(
        task_id='create_table_certified_car',
        conn_id='postgres_connection',
        sql='create_table_car.sql',
        params={'table_name': 'certified_car'}
    )

    insert_data_new_car = SQLExecuteQueryOperator(
        task_id='insert_data_new_car',
        conn_id='postgres_connection',
        sql='insert_car_data.sql',
        params={
            'table_name': 'new_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'New'"
        }
    )

    insert_data_used_car = SQLExecuteQueryOperator(
        task_id='insert_data_used_car',
        conn_id='postgres_connection',
        sql='insert_car_data.sql',
        params={
            'table_name': 'used_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'Used'"
        }
    )

    insert_data_certified_car = SQLExecuteQueryOperator(
        task_id='insert_data_certified_car',
        conn_id='postgres_connection',
        sql='insert_car_data.sql',
        params={
            'table_name': 'certified_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'Certified Pre-Owned'"
        }
    )

read_csv_file >> remove_null_values >> \
    determine_branch >> [save_output_as_csv, save_output_as_table]

save_output_as_csv >> \
    [filter_by_new_car, filter_by_used_car, filter_by_certified_car]

save_output_as_table >> \
    [create_table_new_car, create_table_used_car, create_table_certified_car]

create_table_new_car >> insert_data_new_car
create_table_used_car >> insert_data_used_car
create_table_certified_car >> insert_data_certified_car


