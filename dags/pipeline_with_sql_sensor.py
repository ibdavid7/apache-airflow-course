from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

default_args = {
   'owner': 'loonycorn',
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}



with DAG(
    dag_id = 'pipeline_with_sql_sensor',
    description = 'Executing a pipeline with a SQL sensor',
    default_args = default_args,
    catchup = False,
    tags = ['postgres', 'sensor', 'sql sensor']
) as dag:

    create_laptops_table = SQLExecuteQueryOperator(
        task_id = 'create_laptops_table',
        conn_id = 'postgres_connection',
        sql = """
            CREATE TABLE IF NOT EXISTS laptops (
                id SERIAL PRIMARY KEY,
                company VARCHAR(255),
                product VARCHAR(255),
                type_name VARCHAR(255),
                price_euros NUMERIC(10, 2)
            );
        """
    )

    create_premium_laptops_table = SQLExecuteQueryOperator(
        task_id = 'create_premium_laptops_table',
        conn_id = 'postgres_connection',
        sql = """
            CREATE TABLE IF NOT EXISTS premium_laptops (
                id SERIAL PRIMARY KEY,
                company VARCHAR(255),
                product VARCHAR(255),
                type_name VARCHAR(255),
                price_euros NUMERIC(10, 2)
            );
        """
    )

    wait_for_premium_laptops = SqlSensor(
        task_id='wait_for_premium_laptops',
        conn_id='postgres_connection',
        sql="SELECT EXISTS(SELECT 1 FROM laptops WHERE price_euros > 500)",
        poke_interval=10,
        timeout=10 * 60
    )

    insert_data_into_premium_laptops_table = SQLExecuteQueryOperator(
        task_id = 'insert_data_into_premium_laptops_table',
        conn_id='postgres_connection',
        sql="""INSERT INTO premium_laptops 
               SELECT * FROM laptops WHERE price_euros > 500"""
    )

    delete_laptop_data = SQLExecuteQueryOperator(
        task_id='delete_laptop_data',
        conn_id='postgres_connection',
        sql="DELETE FROM laptops"
    )


    [create_laptops_table, create_premium_laptops_table] >> \
    wait_for_premium_laptops >> \
    insert_data_into_premium_laptops_table >> delete_laptop_data

