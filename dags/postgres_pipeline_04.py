from datetime import datetime, timedelta

import csv
from airflow.providers.standard.operators.python import PythonOperator


from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



default_args = {
   'owner': 'loonycorn'
}

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)
def saving_to_csv(ti):
    filter_data = ti.xcom_pull(task_ids='filtering_customers_by_name')

    with open('/workspaces/apache-airflow-course/output/filtered_customers_by_name.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Name', 'Product', 'Price'])
        for row in filter_data:
            writer.writerow(row)
            
with DAG(
    dag_id = 'postgres_pipeline_04',
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

    insert_customers = SQLExecuteQueryOperator(
        task_id = 'insert_customers',
        conn_id = 'postgres_connection',
        sql = 'insert_customers.sql'
    )
    
    insert_customer_purchases = SQLExecuteQueryOperator(
        task_id = 'insert_customer_purchases',
        conn_id = 'postgres_connection',
        sql = 'insert_customer_purchases.sql'
    )
    
    joining_tables = SQLExecuteQueryOperator(
        task_id = 'joining_tables',
        conn_id = 'postgres_connection',
        sql = 'joining_tables.sql'
    )

    # filtering_customers = SQLExecuteQueryOperator(
    #     task_id = 'filtering_customers',
    #     conn_id = 'postgres_connection',
    #     sql = 'filtering_customers.sql',
    #     parameters={'lower_bound': 5, 'upper_bound': 9}
    # )
    
    filtering_customers = SQLExecuteQueryOperator(
        task_id = 'filtering_customers_by_name',
        conn_id = 'postgres_connection',
        sql = 'filtering_customers_by_name.sql',
        parameters={'names': ['Kiele West', 'Koma Day-Lewis']}
    )
    
    saving_to_csv = PythonOperator(
        task_id='saving_to_csv',
        python_callable=saving_to_csv
    )

    create_table_customers >> create_table_customer_purchases >> insert_customers >> insert_customer_purchases >> joining_tables >> filtering_customers >> saving_to_csv
