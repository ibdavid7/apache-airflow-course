import time
from datetime import datetime, timedelta

import json

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import dag, task, TaskGroup, Label


from airflow.operators.python import PythonOperator

default_args = {"owner": "loonycorn"}

DEFAULT_START = datetime.now().replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(days=5)


@dag(
    dag_id="passing_data_with_operators_02",
    description="Passing data between tasks with TaskFlow API",
    default_args=default_args,
    start_date=DEFAULT_START,
    schedule="@once",
    tags=["xcom", "python", "taskflow"],
)
def passing_data_with_taskflow_api():

    @task
    def get_order_prices():
        return {'o1': 234.45, 'o2': 10.00, 'o3': 34.77, 'o4': 45.66, 'o5': 399}

    @task
    def compute_sum(order_price_data: dict):
        return sum(order_price_data.values())

    @task
    def compute_average(order_price_data: dict):
        total = sum(order_price_data.values())
        return total / len(order_price_data)

    @task
    def display_result(total, average):
        print(f"Total price of goods {total}")
        print(f"Average price of goods {average}")

    order_price_data = get_order_prices()

    with TaskGroup(group_id='compute_metrics') as compute_metrics:
        total = compute_sum(order_price_data)
        average = compute_average(order_price_data)

        order_price_data >> Label('values for sum') >> total
        order_price_data >> Label('values for average') >> average

    compute_metrics >> display_result(total, average)

dag = passing_data_with_taskflow_api()
