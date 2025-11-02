from datetime import datetime, timedelta

from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



default_args = {
   'owner': 'loonycorn'
}

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

with DAG(
    dag_id = 'postgres_pipeline_01',
    description = 'Running a pipeline using the Postgres operator',
    default_args = default_args,
    start_date = START_DATE,
    schedule = '@once',
    tags = ['operator', 'postgres'],
    template_searchpath = '/workspaces/apache-airflow-course/sql_statements'
) as dag:

    create_table_customers = SQLExecuteQueryOperator(
        task_id = 'create_table_customers',
        conn_id = 'postgres_connection',
        sql = 'create_table_customers.sql'
    )

    create_table_customer_purchases = SQLExecuteQueryOperator(
        task_id = 'create_table_customer_purchases',
        conn_id = 'postgres_connection',
        sql = 'create_table_customer_purchases.sql'
    )

    create_table_customers >> create_table_customer_purchases
