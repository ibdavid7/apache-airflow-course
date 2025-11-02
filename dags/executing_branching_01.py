from datetime import datetime, timedelta
import pendulum

from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator


from random import choice

default_args = {
    'owner' : 'loonycorn',
}

DEFAULT_START= pendulum.datetime(2024, 1, 1, tz="UTC")

def has_driving_license():
    return choice([True, False])
 
def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'eligible_to_drive'
    else:
        return 'not_eligible_to_drive'                      

def eligible_to_drive():
    print("You can drive, you have a license!")

def not_eligible_to_drive():
    print("I'm afraid you are out of luck, you need a license to drive")


with DAG(
    dag_id = 'executing_branching_01',
    description = 'Running branching pipelines',
    default_args = default_args,
    start_date = DEFAULT_START,
    schedule = '@once',
    tags = ['branching', 'conditions']
) as dag:

    taskA = PythonOperator(
        task_id = 'has_driving_license',
        python_callable = has_driving_license              
    )

    taskB = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'eligible_to_drive',
        python_callable = eligible_to_drive              
    )

    taskD = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive              
    )

taskA >> taskB >> [taskC, taskD]




