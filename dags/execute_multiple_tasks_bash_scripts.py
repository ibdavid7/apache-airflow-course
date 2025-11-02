from datetime import datetime, timedelta

from airflow import DAG
import pendulum

from airflow.operators.bash import BashOperator

DEFAULT_START = pendulum.datetime(2024, 1, 1, tz="UTC")

default_args = {
    'owner': 'loonycord',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='executing_multiple_tasks_bash_script',  # Explicitly setting dag_id
    description='A DAG to execute multiple tasks',  # Explicitly setting description
    default_args=default_args,  # Explicitly setting default_args
    start_date=DEFAULT_START,  # Explicitly setting start_date
    schedule='@once',  # Updated to use 'schedule' instead of 'schedule_interval'
    template_searchpath='dags/bash_scripts',
    tags=['example', 'hello_world']
) as dag:
    
    taskA = BashOperator(
        task_id='taskA',
        bash_command='taskA.sh'
    )
    taskB = BashOperator(
        task_id='taskB',
        bash_command='taskB.sh'
    )
    taskC = BashOperator(
        task_id='taskC',
        bash_command='taskC.sh'
    )
    taskD = BashOperator(
        task_id='taskD',
        bash_command='taskD.sh'
    )
    taskE = BashOperator(
        task_id='taskE',
        bash_command='taskE.sh'
    )
    taskF = BashOperator(
        task_id='taskF',
        bash_command='taskF.sh'
    )
    taskG = BashOperator(
        task_id='taskG',
        bash_command='taskG.sh'
    )

taskA >> [taskB, taskC, taskD]
taskB >> taskE
taskC >> taskF
taskD >> taskG


# taskA >> [taskB, taskC] >> taskD
# taskA.set_downstream(taskB)
# taskA >> taskB
# taskA.set_upstream(taskB)
# taskA << taskB

