from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd

import json
import csv

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.decorators import dag, task

default_args = {
    'owner': 'loonycorn'
}

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

@dag(dag_id='data_transformation_storage_pipeline_01',
    description = 'Data transformation and storage pipeline',
    default_args = default_args,
    start_date = START_DATE,
    schedule = '@once',
    tags = ['sqlite', 'python', 'taskflow_api', 'xcoms'])
def data_transformation_storage_pipeline():

    @task
    def read_dataset():
        df = pd.read_csv('/workspaces/apache-airflow-course/datasets/car_data.csv')

        return df.to_json()
    
    @task
    def create_table():
        sqlite_operator = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="my_sqlite_db_conn",
            sql=r"""CREATE TABLE IF NOT EXISTS car_data (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        model TEXT NOT NULL,
                        body_style TEXT NOT NULL,
                        seat INTEGER NOT NULL,
                        price INTEGER NOT NULL);""",
        )

        sqlite_operator.execute(context=None)

    @task
    def insert_selected_data(ti, **kwargs):
        # ti = kwargs['ti']

        json_data = ti.xcom_pull(task_ids='read_dataset')
        
        df = pd.read_json(json_data)
        
        df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]
        
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        insert_query = r"""
            INSERT INTO car_data (brand, model, body_style, seat, price)
            VALUES (?, ?, ?, ?, ?)
        """

        parameters = df.to_dict(orient='records')

        for record in parameters:
            sqlite_operator = SQLExecuteQueryOperator(
                task_id="insert_data",
                conn_id="my_sqlite_db_conn",
                sql=insert_query,
                parameters=tuple(record.values()),
            )

            sqlite_operator.execute(context=None)

    read_dataset() >> create_table() >> insert_selected_data()


data_transformation_storage_pipeline()

