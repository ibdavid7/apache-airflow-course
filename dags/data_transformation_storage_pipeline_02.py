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

@dag(dag_id='data_transformation_storage_pipeline_02',
    description = 'Data transformation and storage pipeline',
    default_args = default_args,
    start_date = START_DATE,
    schedule = '@once',
    tags = ['sqlite', 'python', 'taskflow_api', 'xcoms'])
def data_transformation_storage_pipeline():

    @task
    def read_car_data():
        df = pd.read_csv('/workspaces/apache-airflow-course/datasets/car_data.csv')

        return df.to_json()
    
    @task
    def read_car_categories():
        df = pd.read_csv('/workspaces/apache-airflow-course/datasets/car_categories.csv')

        return df.to_json()
    
    @task
    def create_table_car_data():
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
    def create_table_car_categories():
        sqlite_operator = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="my_sqlite_db_conn",
            sql=r"""CREATE TABLE IF NOT EXISTS car_categories (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        category TEXT NOT NULL
                    );""",
        )
        sqlite_operator.execute(context=None)

    @task
    def insert_car_data(ti, **kwargs):

        json_data = ti.xcom_pull(task_ids='read_car_data')
        
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
            
    @task
    def insert_car_categories(ti, **kwargs):

        json_data = ti.xcom_pull(task_ids='read_car_categories')
        
        df = pd.read_json(json_data)
        
        df = df[['Brand', 'Category']]
        
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        insert_query = r"""
            INSERT INTO car_categories (brand, category)
            VALUES (?, ?)
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
            
    @task
    def join():
        sqlite_operator = SQLExecuteQueryOperator(
            task_id="join_tables",
            conn_id="my_sqlite_db_conn",
            sql=r"""
                CREATE TABLE IF NOT EXISTS car_details AS
                SELECT cd.brand, cd.model, cd.price, cc.category
                FROM car_data cd
                JOIN car_categories cc ON cd.brand = cc.brand;
            """,
        )
        results = sqlite_operator.execute(context=None)
        # for row in results:
            # print(row)        

    join_task = join()

    read_car_data() >> create_table_car_data() >> insert_car_data() >> join_task
    read_car_categories() >> create_table_car_categories() >> insert_car_categories() >> join_task


data_transformation_storage_pipeline()

