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
    dag_id="multiple_outputs_01",
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

    @task(multiple_outputs=True)
    def compute_total_and_average_(order_price_data: dict):
        total = sum(order_price_data.values())
        average = total / len(order_price_data)
        return {"total": total, "average": average}
        # return total, average

    @task
    def display_result(total, average):
        print(f"Total price of goods {total}")
        print(f"Average price of goods {average}")

    order_price_data = get_order_prices()
    
    

    with TaskGroup(group_id='compute_metrics') as compute_metrics:

        price_metrics = compute_total_and_average_(order_price_data)
        order_price_data >> Label('values for compute metrics') >> price_metrics["total"]

    compute_metrics >> display_result(total=price_metrics["total"], average=price_metrics["average"])
dag = passing_data_with_taskflow_api()
