import time

from datetime import datetime, timedelta
import pendulum
from airflow import DAG

from airflow.operators.python import PythonOperator

DEFAULT_START = pendulum.datetime(2024, 1, 1, tz="UTC")

default_args = {
    'owner' : 'loonycorn'
}

def increment_by_1(counter):
    print("Count {counter}!".format(counter=counter))

    return counter + 1


def multiply_by_100(counter):
    print("Count {counter}!".format(counter=counter))

    return counter * 100


# def subtract_9(counter):
#     print("Count {counter}!".format(counter=counter))

#     return counter - 9

# def print(counter):
#     print("Count {counter}!".format(counter=counter))

#     return counter - 9

with DAG(
    dag_id = 'cross_task_communication',
    description = 'Cross-task communication with XCom',
    default_args = default_args,
    start_date = DEFAULT_START,
    schedule = '@daily',
    tags = ['xcom', 'python']
) as dag:
    taskA = PythonOperator(
        task_id = 'increment_by_1',
        python_callable = increment_by_1,
        op_kwargs={'counter': 100}
    )

    taskB = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable = multiply_by_100,
        op_kwargs={'counter': 9}
    )


taskA >> taskB



