from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import date, datetime, timedelta

default_args = {
    'owner' : 'loonycorn'
}

START = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

with DAG(
    dag_id = 'simple_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = START,
    schedule = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users_temp (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    city VARCHAR(50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'postgres_connection',
        dag = dag,
    )


    insert_values_1 = SQLExecuteQueryOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users_temp (name, age, is_active) VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'postgres_connection',
        dag = dag,
    )

    insert_values_2 = SQLExecuteQueryOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users_temp (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        sqlite_conn_id = 'postgres_connection',
        dag = dag,
    )

    display_result = SQLExecuteQueryOperator(
        task_id = 'display_result',
        sql = r"""SELECT * FROM users_temp""",
        sqlite_conn_id = 'postgres_connection',
        dag = dag,
        do_xcom_push = True
    )


    drop_table = SQLExecuteQueryOperator(
        task_id = 'drop_table',
        sql = r"""
            DROP TABLE users_temp;
        """,
        sqlite_conn_id = 'postgres_connection',
        dag = dag,
    )

create_table >> [insert_values_1, insert_values_2] >> display_result >> drop_table


