import pandas as pd

from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup, Label
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator

from airflow.models import Variable

import pendulum


default_args = {
   'owner': 'loonycorn'
}

DATASETS_PATH = '/workspaces/apache-airflow-course/datasets/insurance.csv'

OUTPUT_PATH = '/workspaces/apache-airflow-course/output/{0}.csv'

DEFAULT_START = pendulum.datetime(2024, 1, 1, tz="UTC")

def read_csv_file(ti):
    df = pd.read_csv(DATASETS_PATH)

    print(df)

    ti.xcom_push(key='my_csv', value=df.to_json())

def remove_null_values(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_preprocessing.read_csv_file',
        key='my_csv',
    )
    
    print('json_data:', json_data)

    df = pd.read_json(json_data)
    
    df = df.dropna()

    print(df)
    
    ti.xcom_push(key='cleaned_csv', value=df.to_json())

def determine_branch():
    transform_action = Variable.get("transform_action", default_var=None)
    
    if transform_action.startswith('filter'):
        return 'filtering.{0}'.format(transform_action)
    elif transform_action == 'groupby_region_smoker':
        return 'grouping.{0}'.format(transform_action)


def filter_by_southwest(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_preprocessing.remove_null_values',
        key='cleaned_csv',
    )
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    
    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_preprocessing.remove_null_values',
        key='cleaned_csv',
    )
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    
    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_preprocessing.remove_null_values',
        key='cleaned_csv',
    )
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    
    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)


def filter_by_northeast(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_preprocessing.remove_null_values',
        key='cleaned_csv',
    )
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    
    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)


def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(
        task_ids='reading_and_preprocessing.remove_null_values',
        key='cleaned_csv',
    )
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean', 
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)


    
with DAG(
    dag_id = 'executing_branching_03',
    description = 'Running a branching pipeline',
    default_args = default_args,
    start_date = DEFAULT_START,
    schedule = '@once',
    tags = ['python', 'transform', 'pipeline', 'branching']
) as dag:

    with TaskGroup('reading_and_preprocessing', tooltip='Reading and Preprocessing Tasks') as reading_and_preprocessing_group:
    
        read_csv_file = PythonOperator(
            task_id='read_csv_file',
            python_callable=read_csv_file
        )

        remove_null_values = PythonOperator(
            task_id='remove_null_values',
            python_callable=remove_null_values
        )

    read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )
    
    with TaskGroup('filtering', tooltip='Filtering Tasks') as filtering_group:
    
        filter_by_southwest = PythonOperator(
            task_id='filter_by_southwest',
            python_callable=filter_by_southwest
        )
        
        filter_by_southeast = PythonOperator(
            task_id='filter_by_southeast',
            python_callable=filter_by_southeast
        )

        filter_by_northwest = PythonOperator(
            task_id='filter_by_northwest',
            python_callable=filter_by_northwest
        )

        filter_by_northeast = PythonOperator(
            task_id='filter_by_northeast',
            python_callable=filter_by_northeast
        )

    with TaskGroup('grouping', tooltip='Grouping Tasks') as grouping_group:

        groupby_region_smoker = PythonOperator(
            task_id='groupby_region_smoker',
            python_callable=groupby_region_smoker
        )

    reading_and_preprocessing_group >> Label('Preprocssed Data') >> determine_branch >> Label('Branch on Condition') >> [filtering_group, grouping_group]

   





















